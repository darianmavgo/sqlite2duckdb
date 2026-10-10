// sqlite2duckdb walks a directory tree and converts every SQLite database
// file it finds into a sibling DuckDB file. The SQLite files are never
// modified or deleted.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	_ "modernc.org/sqlite"
)

var sqliteExts = map[string]bool{
	".db": true, ".sqlite": true, ".sqlite3": true, ".db3": true, ".s3db": true,
}

var sqliteMagic = []byte("SQLite format 3\x00")

type result struct {
	src, dst string
	tables   int
	rows     int64
	skipped  string
	err      error
}

func main() {
	workers := flag.Int("workers", 32, "number of concurrent workers")
	force := flag.Bool("force", false, "overwrite existing .duckdb files")
	dryRun := flag.Bool("dry-run", false, "list files that would be migrated and exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] [root ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	roots := flag.Args()
	if len(roots) == 0 {
		roots = []string{"."}
	}
	if *workers < 1 {
		*workers = 1
	}

	jobs := make(chan string)
	results := make(chan result)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Go(func() {
			for src := range jobs {
				results <- migrate(src, *force)
			}
		})
	}

	var found atomic.Int64
	go func() {
		for _, root := range roots {
			filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					fmt.Fprintf(os.Stderr, "walk %s: %v\n", p, err)
					return nil
				}
				if d.IsDir() || !d.Type().IsRegular() || !sqliteExts[strings.ToLower(filepath.Ext(p))] {
					return nil
				}
				if !isSQLite(p) {
					return nil
				}
				found.Add(1)
				if *dryRun {
					fmt.Println(p, "->", duckPath(p))
					return nil
				}
				jobs <- p
				return nil
			})
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	start := time.Now()
	var ok, skipped, failed int
	var totalRows int64
	for r := range results {
		switch {
		case r.err != nil:
			failed++
			fmt.Printf("FAIL  %s: %v\n", r.src, r.err)
		case r.skipped != "":
			skipped++
			fmt.Printf("SKIP  %s (%s)\n", r.src, r.skipped)
		default:
			ok++
			totalRows += r.rows
			fmt.Printf("OK    %s -> %s (%d tables, %d rows)\n", r.src, r.dst, r.tables, r.rows)
		}
	}
	if *dryRun {
		fmt.Printf("%d sqlite files found\n", found.Load())
		return
	}
	fmt.Printf("done in %s: %d migrated, %d skipped, %d failed, %d rows\n",
		time.Since(start).Round(time.Millisecond), ok, skipped, failed, totalRows)
	if failed > 0 {
		os.Exit(1)
	}
}

func isSQLite(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	buf := make([]byte, len(sqliteMagic))
	if _, err := io.ReadFull(f, buf); err != nil {
		return false
	}
	return bytes.Equal(buf, sqliteMagic)
}

func duckPath(src string) string {
	return strings.TrimSuffix(src, filepath.Ext(src)) + ".duckdb"
}

func quote(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

func migrate(src string, force bool) (res result) {
	res.src = src
	res.dst = duckPath(src)
	if _, err := os.Stat(res.dst); err == nil && !force {
		res.skipped = "destination exists; use -force"
		return
	}
	tmp := res.dst + ".tmp"
	os.Remove(tmp)
	os.Remove(tmp + ".wal")
	defer func() {
		if res.err != nil {
			os.Remove(tmp)
			os.Remove(tmp + ".wal")
			return
		}
	}()

	if err := convert(src, tmp, &res); err != nil {
		res.err = err
		return
	}
	os.Remove(res.dst + ".wal")
	if err := os.Rename(tmp, res.dst); err != nil {
		res.err = err
	}
	return
}

type column struct {
	name string
	typ  string // DuckDB type
}

func convert(src, tmp string, res *result) error {
	ctx := context.Background()

	absSrc, err := filepath.Abs(src)
	if err != nil {
		return err
	}
	u := url.URL{Scheme: "file", Path: absSrc, RawQuery: "mode=ro"}
	sdb, err := sql.Open("sqlite", u.String())
	if err != nil {
		return err
	}
	defer sdb.Close()

	connector, err := duckdb.NewConnector(tmp+"?threads=1&memory_limit=1GB", nil)
	if err != nil {
		return err
	}
	ddb := sql.OpenDB(connector)
	ddb.SetMaxOpenConns(1)
	defer func() {
		ddb.Close()
		connector.Close()
	}()

	rows, err := sdb.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		tables = append(tables, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, t := range tables {
		n, err := copyTable(ctx, sdb, ddb, t)
		if err != nil {
			return fmt.Errorf("table %s: %w", t, err)
		}
		res.tables++
		res.rows += n
	}
	// Flush to the main file before the rename.
	if _, err := ddb.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return err
	}
	return nil
}

// columnType picks a DuckDB type from the values actually stored, since
// SQLite columns are dynamically typed.
func columnType(ctx context.Context, sdb *sql.DB, table, col string) (string, error) {
	rows, err := sdb.QueryContext(ctx,
		fmt.Sprintf(`SELECT DISTINCT typeof(%s) FROM %s`, quote(col), quote(table)))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	set := map[string]bool{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return "", err
		}
		if t != "null" {
			set[t] = true
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	switch {
	case len(set) == 0:
		return "VARCHAR", nil
	case len(set) == 1 && set["integer"]:
		return "BIGINT", nil
	case len(set) == 1 && set["real"]:
		return "DOUBLE", nil
	case len(set) == 2 && set["integer"] && set["real"]:
		return "DOUBLE", nil
	case len(set) == 1 && set["blob"]:
		return "BLOB", nil
	default:
		return "VARCHAR", nil
	}
}

func copyTable(ctx context.Context, sdb, ddb *sql.DB, table string) (int64, error) {
	ci, err := sdb.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quote(table)))
	if err != nil {
		return 0, err
	}
	var cols []column
	for ci.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := ci.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			ci.Close()
			return 0, err
		}
		cols = append(cols, column{name: name})
	}
	ci.Close()
	if err := ci.Err(); err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns")
	}

	defs := make([]string, len(cols))
	sel := make([]string, len(cols))
	for i := range cols {
		t, err := columnType(ctx, sdb, table, cols[i].name)
		if err != nil {
			return 0, err
		}
		cols[i].typ = t
		defs[i] = quote(cols[i].name) + " " + t
		sel[i] = quote(cols[i].name)
	}
	if _, err := ddb.ExecContext(ctx,
		fmt.Sprintf(`CREATE OR REPLACE TABLE %s (%s)`, quote(table), strings.Join(defs, ", "))); err != nil {
		return 0, err
	}

	data, err := sdb.QueryContext(ctx,
		fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(sel, ", "), quote(table)))
	if err != nil {
		return 0, err
	}
	defer data.Close()

	dconn, err := ddb.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer dconn.Close()

	var n int64
	err = dconn.Raw(func(dc any) error {
		app, err := duckdb.NewAppenderFromConn(dc.(driver.Conn), "", table)
		if err != nil {
			return err
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		args := make([]driver.Value, len(cols))
		for data.Next() {
			if err := data.Scan(ptrs...); err != nil {
				app.Close()
				return err
			}
			for i, v := range vals {
				args[i] = convertValue(v, cols[i].typ)
			}
			if err := app.AppendRow(args...); err != nil {
				app.Close()
				return err
			}
			n++
		}
		if err := data.Err(); err != nil {
			app.Close()
			return err
		}
		return app.Close()
	})
	return n, err
}

func convertValue(v any, typ string) driver.Value {
	if v == nil {
		return nil
	}
	switch typ {
	case "BIGINT":
		switch x := v.(type) {
		case int64:
			return x
		}
	case "DOUBLE":
		switch x := v.(type) {
		case int64:
			return float64(x)
		case float64:
			return x
		}
	case "BLOB":
		if b, ok := v.([]byte); ok {
			return b
		}
	}
	// VARCHAR (mixed or text) and any fallback.
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(x)
	}
}
