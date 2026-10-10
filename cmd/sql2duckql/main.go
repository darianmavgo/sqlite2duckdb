// sql2duckql walks a directory tree and translates every SQLite-dialect .sql
// file into a DuckDB-compatible .duckql file next to it. The .sql files are
// never modified or deleted.
package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darianmavgo/sqlite2duckdb/translate"
)

type result struct {
	src, dst string
	stats    translate.Stats
	skipped  string
	err      error
}

func main() {
	workers := flag.Int("workers", 32, "number of concurrent workers")
	force := flag.Bool("force", false, "overwrite existing .duckql files")
	dryRun := flag.Bool("dry-run", false, "list files that would be translated and exit")
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
				results <- convert(src, *force)
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
				if d.IsDir() || !d.Type().IsRegular() || !strings.EqualFold(filepath.Ext(p), ".sql") {
					return nil
				}
				found.Add(1)
				if *dryRun {
					fmt.Println(p, "->", duckqlPath(p))
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
	var ok, skipped, failed, stmts, unsupported int
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
			stmts += r.stats.Statements
			unsupported += r.stats.Skipped
			fmt.Printf("OK    %s -> %s (%d statements, %d commented out)\n",
				r.src, r.dst, r.stats.Statements, r.stats.Skipped)
		}
	}
	if *dryRun {
		fmt.Printf("%d .sql files found\n", found.Load())
		return
	}
	fmt.Printf("done in %s: %d translated, %d skipped, %d failed, %d statements (%d commented out)\n",
		time.Since(start).Round(time.Millisecond), ok, skipped, failed, stmts, unsupported)
	if failed > 0 {
		os.Exit(1)
	}
}

func duckqlPath(src string) string {
	return strings.TrimSuffix(src, filepath.Ext(src)) + ".duckql"
}

func convert(src string, force bool) (res result) {
	res.src = src
	res.dst = duckqlPath(src)
	if _, err := os.Stat(res.dst); err == nil && !force {
		res.skipped = "destination exists; use -force"
		return
	}

	in, err := os.Open(src)
	if err != nil {
		res.err = err
		return
	}
	defer in.Close()

	tmp := res.dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		res.err = err
		return
	}
	res.stats, err = translate.Translate(in, out)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, res.dst)
	}
	if err != nil {
		os.Remove(tmp)
		res.err = err
	}
	return
}
