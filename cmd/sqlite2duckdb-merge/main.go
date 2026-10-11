// sqlite2duckdb-merge copies rows from an existing SQLite database into an
// existing DuckDB database. By default only rows that are not already present
// are inserted, and only into tables that exist in both databases under the
// same name. The SQLite file is opened read-only.
//
// A row is "already present" when its key matches a row in the DuckDB table.
// The key is, in order of preference: the -key flag, the DuckDB table's
// primary key, the SQLite table's primary key, or all shared columns.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	_ "modernc.org/sqlite"
)

type column struct {
	name string
	typ  string // type: DuckDB type for dst columns, staging type for src
	pk   bool
}

func main() {
	tablesFlag := flag.String("tables", "", "comma-separated tables to migrate (default: all tables in both databases)")
	keyFlag := flag.String("key", "", "comma-separated key columns used to detect existing rows (applies to every table)")
	all := flag.Bool("all", false, "insert every source row, without checking for existing rows")
	dryRun := flag.Bool("dry-run", false, "report how many rows would be inserted without writing")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] source.sqlite dest.duckdb\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(flag.Arg(0), flag.Arg(1), splitList(*tablesFlag), splitList(*keyFlag), *all, *dryRun); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func quote(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

func run(src, dst string, only, key []string, all, dryRun bool) error {
	ctx := context.Background()
	for _, p := range []string{src, dst} {
		if _, err := os.Stat(p); err != nil {
			return err
		}
	}
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

	connector, err := duckdb.NewConnector(dst, nil)
	if err != nil {
		return err
	}
	ddb := sql.OpenDB(connector)
	ddb.SetMaxOpenConns(1)
	defer func() {
		ddb.Close()
		connector.Close()
	}()

	srcTables, err := listStrings(ctx, sdb,
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return fmt.Errorf("list sqlite tables: %w", err)
	}
	dstTables, err := listStrings(ctx, ddb,
		`SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='main'`)
	if err != nil {
		return fmt.Errorf("list duckdb tables: %w", err)
	}
	inDst := map[string]bool{}
	for _, t := range dstTables {
		inDst[t] = true
	}
	wanted := map[string]bool{}
	for _, t := range only {
		wanted[t] = true
	}

	var totalIns int64
	var failed int
	start := time.Now()
	for _, t := range srcTables {
		if len(wanted) > 0 && !wanted[t] {
			continue
		}
		if !inDst[t] {
			fmt.Printf("SKIP  %s (not in destination)\n", t)
			continue
		}
		scanned, ins, err := mergeTable(ctx, sdb, ddb, t, key, all, dryRun)
		if err != nil {
			failed++
			fmt.Printf("FAIL  %s: %v\n", t, err)
			continue
		}
		verb := "inserted"
		if dryRun {
			verb = "would insert"
		}
		fmt.Printf("OK    %s (%d source rows, %s %d)\n", t, scanned, verb, ins)
		totalIns += ins
	}
	for t := range wanted {
		found := false
		for _, s := range srcTables {
			found = found || s == t
		}
		if !found {
			fmt.Printf("SKIP  %s (not in source)\n", t)
		}
	}
	if !dryRun {
		if _, err := ddb.ExecContext(ctx, "CHECKPOINT"); err != nil {
			return err
		}
	}
	fmt.Printf("done in %s: %d rows %s, %d tables failed\n",
		time.Since(start).Round(time.Millisecond), totalIns,
		map[bool]string{false: "inserted", true: "would be inserted"}[dryRun], failed)
	if failed > 0 {
		return fmt.Errorf("%d tables failed", failed)
	}
	return nil
}

func listStrings(ctx context.Context, db *sql.DB, q string) ([]string, error) {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func sqliteColumns(ctx context.Context, db *sql.DB, table string) ([]column, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quote(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []column
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, column{name: name, pk: pk > 0})
	}
	return cols, rows.Err()
}

func duckColumns(ctx context.Context, db *sql.DB, table string) ([]column, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quote(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []column
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk bool
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, column{name: name, typ: ctype, pk: pk})
	}
	return cols, rows.Err()
}

// stagingType picks a type for a source column from the values actually
// stored, since SQLite columns are dynamically typed.
func stagingType(ctx context.Context, sdb *sql.DB, table, col string) (string, error) {
	vals, err := listStrings(ctx, sdb,
		fmt.Sprintf(`SELECT DISTINCT typeof(%s) FROM %s WHERE %s IS NOT NULL`, quote(col), quote(table), quote(col)))
	if err != nil {
		return "", err
	}
	set := map[string]bool{}
	for _, v := range vals {
		set[v] = true
	}
	switch {
	case len(set) == 1 && set["integer"]:
		return "BIGINT", nil
	case len(set) == 1 && set["real"], len(set) == 2 && set["integer"] && set["real"]:
		return "DOUBLE", nil
	case len(set) == 1 && set["blob"]:
		return "BLOB", nil
	}
	return "VARCHAR", nil
}

func mergeTable(ctx context.Context, sdb, ddb *sql.DB, table string, keyOverride []string, all, dryRun bool) (scanned, inserted int64, err error) {
	scols, err := sqliteColumns(ctx, sdb, table)
	if err != nil {
		return 0, 0, err
	}
	dcols, err := duckColumns(ctx, ddb, table)
	if err != nil {
		return 0, 0, err
	}
	dByName := map[string]column{}
	for _, c := range dcols {
		dByName[c.name] = c
	}
	// Shared columns, in source order.
	var cols []column // typ = staging type
	for _, c := range scols {
		if _, ok := dByName[c.name]; !ok {
			continue
		}
		t, err := stagingType(ctx, sdb, table, c.name)
		if err != nil {
			return 0, 0, err
		}
		cols = append(cols, column{name: c.name, typ: t, pk: c.pk})
	}
	if len(cols) == 0 {
		return 0, 0, fmt.Errorf("no columns in common")
	}
	shared := map[string]bool{}
	for _, c := range cols {
		shared[c.name] = true
	}

	// Choose the key used to detect existing rows.
	var keyCols []string
	switch {
	case len(keyOverride) > 0:
		keyCols = keyOverride
	default:
		for _, c := range dcols {
			if c.pk {
				keyCols = append(keyCols, c.name)
			}
		}
		if len(keyCols) == 0 {
			for _, c := range cols {
				if c.pk {
					keyCols = append(keyCols, c.name)
				}
			}
		}
	}
	for _, k := range keyCols {
		if !shared[k] {
			return 0, 0, fmt.Errorf("key column %q is not in both tables", k)
		}
	}
	wholeRow := len(keyCols) == 0
	if wholeRow {
		for _, c := range cols {
			keyCols = append(keyCols, c.name)
		}
	}

	conn, err := ddb.Conn(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	stage := "_merge_stage"
	defs := make([]string, len(cols))
	sel := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = quote(c.name) + " " + c.typ
		sel[i] = quote(c.name)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`CREATE OR REPLACE TEMP TABLE %s (%s)`, stage, strings.Join(defs, ", "))); err != nil {
		return 0, 0, err
	}
	defer conn.ExecContext(ctx, `DROP TABLE IF EXISTS `+stage)

	data, err := sdb.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(sel, ", "), quote(table)))
	if err != nil {
		return 0, 0, err
	}
	defer data.Close()

	err = conn.Raw(func(dc any) error {
		app, err := duckdb.NewAppenderFromConn(dc.(driver.Conn), "", stage)
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
			scanned++
		}
		if err := data.Err(); err != nil {
			app.Close()
			return err
		}
		return app.Close()
	})
	if err != nil {
		return 0, 0, err
	}

	// Cast staged values to the destination column types.
	casts := make([]string, len(cols))
	names := make([]string, len(cols))
	for i, c := range cols {
		casts[i] = fmt.Sprintf(`CAST(%s AS %s) AS %s`, quote(c.name), dByName[c.name].typ, quote(c.name))
		names[i] = quote(c.name)
	}
	query := fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(casts, ", "), stage)
	if wholeRow {
		query = strings.Replace(query, "SELECT ", "SELECT DISTINCT ", 1)
	}
	if !all {
		conds := make([]string, len(keyCols))
		for i, k := range keyCols {
			op := "="
			if wholeRow {
				op = "IS NOT DISTINCT FROM"
			}
			conds[i] = fmt.Sprintf(`d.%s %s s.%s`, quote(k), op, quote(k))
		}
		query = fmt.Sprintf(`SELECT * FROM (%s) s WHERE NOT EXISTS (SELECT 1 FROM %s d WHERE %s)`,
			query, quote(table), strings.Join(conds, " AND "))
		if !wholeRow {
			// Rows with duplicate keys within the source: keep the first.
			query = fmt.Sprintf(`SELECT * FROM (%s) WHERE true QUALIFY row_number() OVER (PARTITION BY %s) = 1`,
				query, strings.Join(quoteAll(keyCols), ", "))
		}
	}

	if dryRun {
		err = conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM (%s)`, query)).Scan(&inserted)
		return scanned, inserted, err
	}
	res, err := conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (%s) %s`, quote(table), strings.Join(names, ", "), query))
	if err != nil {
		return scanned, 0, err
	}
	inserted, err = res.RowsAffected()
	return scanned, inserted, err
}

func quoteAll(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = quote(id)
	}
	return out
}

func convertValue(v any, typ string) driver.Value {
	if v == nil {
		return nil
	}
	switch typ {
	case "BIGINT":
		if x, ok := v.(int64); ok {
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
