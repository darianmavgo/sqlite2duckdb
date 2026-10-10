package translate

import (
	"database/sql"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func run(t *testing.T, in string) string {
	t.Helper()
	var sb strings.Builder
	if _, err := Translate(strings.NewReader(in), &sb); err != nil {
		t.Fatal(err)
	}
	return sb.String()
}

func TestRewrites(t *testing.T) {
	out := run(t, "CREATE TABLE `t`(id INTEGER PRIMARY KEY AUTOINCREMENT, [n] REAL, d DATETIME, b BLOB) WITHOUT ROWID;\n"+
		"INSERT INTO t VALUES(1,2.5,'a;b''c','x'||char(10),X'00ff');\n")
	for _, want := range []string{
		`CREATE TABLE "t"(id INTEGER PRIMARY KEY, "n" DOUBLE, d TIMESTAMP, b BLOB);`,
		`'x'||chr(10)`,
		`'\x00\xFF'::BLOB`,
		`'a;b''c'`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestSkips(t *testing.T) {
	in := "PRAGMA foreign_keys=OFF;\nBEGIN IMMEDIATE;\n" +
		"CREATE TABLE a(x);\n" +
		"CREATE TRIGGER tr AFTER INSERT ON a BEGIN INSERT INTO a VALUES(1); END;\n" +
		"CREATE INDEX i ON a(x) WHERE x > 1;\n" +
		"DELETE FROM sqlite_sequence;\nCOMMIT;\n"
	out := run(t, in)
	if n := strings.Count(out, "-- [skipped:"); n != 4 {
		t.Errorf("want 4 skipped, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "BEGIN TRANSACTION;") || !strings.Contains(out, "COMMIT;") {
		t.Errorf("transaction statements wrong:\n%s", out)
	}
}

// TestRealDump translates a real `sqlite3 .dump` and executes it in DuckDB.
func TestRealDump(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not installed")
	}
	setup := `
CREATE TABLE people(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, score REAL, born DATETIME, pic BLOB);
INSERT INTO people(name,score,born,pic) VALUES('Ann;1',1.5,'2020-01-02 03:04:05',x'cafe'),('O''Neil',NULL,NULL,NULL),('line'||char(10)||'two',3,'2021-01-01 00:00:00',x'');
CREATE INDEX people_name ON people(name);
CREATE INDEX people_partial ON people(score) WHERE score > 1;
CREATE VIEW v AS SELECT name FROM people;
CREATE TRIGGER trg AFTER INSERT ON people BEGIN UPDATE people SET score = 0 WHERE id = new.id; END;`
	dbFile := filepath.Join(t.TempDir(), "src.db")
	if b, err := exec.Command("sqlite3", dbFile, setup).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	dump, err := exec.Command("sqlite3", dbFile, ".dump").Output()
	if err != nil {
		t.Fatal(err)
	}
	out := run(t, string(dump))

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(out); err != nil {
		t.Fatalf("duckdb rejected translated dump: %v\n%s", err, out)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM people WHERE name LIKE '%' || chr(10) || '%' OR name = 'Ann;1'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("want 2 rows, got %d", n)
	}
	var sz int
	if err := db.QueryRow(`SELECT octet_length(pic) FROM people WHERE name='Ann;1'`).Scan(&sz); err != nil || sz != 2 {
		t.Errorf("blob roundtrip: size=%d err=%v", sz, err)
	}
}
