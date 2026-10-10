// Package translate converts SQLite-dialect SQL (for example the output of
// `sqlite3 db .dump`) into SQL that DuckDB can execute.
//
// Statements DuckDB cannot represent (PRAGMAs, triggers, virtual tables,
// sqlite_sequence bookkeeping, partial indexes) are kept in the output as
// commented-out text with a "-- [skipped: reason]" header, so nothing is
// silently lost.
package translate

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

// Stats counts what happened during a Translate run.
type Stats struct {
	Statements int
	Skipped    int
}

// Translate reads SQLite SQL from r and writes DuckDB SQL to w one statement
// at a time, so arbitrarily large dumps are fine.
func Translate(r io.Reader, w io.Writer) (Stats, error) {
	var st Stats
	br := bufio.NewReaderSize(r, 1<<20)
	bw := bufio.NewWriterSize(w, 1<<20)
	for {
		stmt, err := nextStatement(br)
		if strings.TrimSpace(stmt) != "" {
			out, skipped := Statement(stmt)
			if hasCode(stmt) {
				st.Statements++
				if skipped {
					st.Skipped++
				}
			}
			if _, werr := bw.WriteString(out); werr != nil {
				return st, werr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return st, bw.Flush()
			}
			return st, err
		}
	}
}

// hasCode reports whether the text holds anything besides whitespace and comments.
func hasCode(s string) bool {
	for _, t := range tokenize(s) {
		if t.kind != tWS && t.kind != tComment {
			return true
		}
	}
	return false
}

// nextStatement returns the next ';'-terminated statement (terminator
// included), honoring quotes, comments and CREATE TRIGGER ... END bodies.
// At end of input it returns the remaining text and io.EOF.
func nextStatement(br *bufio.Reader) (string, error) {
	var sb strings.Builder
	var word []byte
	var words []string // lowercased words seen so far in this statement
	flushWord := func() {
		if len(word) > 0 {
			words = append(words, strings.ToLower(string(word)))
			word = word[:0]
		}
	}
	isTrigger := func() bool {
		for i, w := range words {
			if i > 3 {
				break
			}
			if w == "trigger" {
				return true
			}
		}
		return false
	}
	readUntil := func(close byte) error {
		for {
			c, err := br.ReadByte()
			if err != nil {
				return err
			}
			sb.WriteByte(c)
			if c == close {
				// doubled quote is an escape
				n, perr := br.Peek(1)
				if perr == nil && n[0] == close && close != ']' {
					br.ReadByte()
					sb.WriteByte(close)
					continue
				}
				return nil
			}
		}
	}
	for {
		c, err := br.ReadByte()
		if err != nil {
			return sb.String(), err
		}
		sb.WriteByte(c)
		switch {
		case c == '\'' || c == '"' || c == '`':
			flushWord()
			if err := readUntil(c); err != nil {
				return sb.String(), err
			}
		case c == '[':
			flushWord()
			if err := readUntil(']'); err != nil {
				return sb.String(), err
			}
		case c == '-':
			if n, _ := br.Peek(1); len(n) == 1 && n[0] == '-' {
				flushWord()
				for {
					c, err := br.ReadByte()
					if err != nil {
						return sb.String(), err
					}
					sb.WriteByte(c)
					if c == '\n' {
						break
					}
				}
			}
		case c == '/':
			if n, _ := br.Peek(1); len(n) == 1 && n[0] == '*' {
				flushWord()
				br.ReadByte()
				sb.WriteByte('*')
				prev := byte(0)
				for {
					c, err := br.ReadByte()
					if err != nil {
						return sb.String(), err
					}
					sb.WriteByte(c)
					if prev == '*' && c == '/' {
						break
					}
					prev = c
				}
			}
		case c == ';':
			flushWord()
			if isTrigger() && (len(words) == 0 || words[len(words)-1] != "end") {
				continue
			}
			return sb.String(), nil
		case c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			word = append(word, c)
		default:
			flushWord()
		}
	}
}

type tokKind int

const (
	tWS tokKind = iota
	tComment
	tString
	tIdent // quoted identifier, already normalized to "double quotes"
	tBlob  // X'..' literal, already converted
	tWord
	tOther
)

type token struct {
	kind tokKind
	text string
}

func isWordStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isWordChar(c byte) bool {
	return isWordStart(c) || c >= '0' && c <= '9' || c == '$'
}

func tokenize(s string) []token {
	var toks []token
	i := 0
	// scanQuoted returns the index just past the closing quote.
	scanQuoted := func(start int, q byte) int {
		j := start + 1
		for j < len(s) {
			if s[j] == q {
				if q != ']' && j+1 < len(s) && s[j+1] == q {
					j += 2
					continue
				}
				return j + 1
			}
			j++
		}
		return len(s)
	}
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			j := i
			for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
				j++
			}
			toks = append(toks, token{tWS, s[i:j]})
			i = j
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			j := strings.IndexByte(s[i:], '\n')
			if j < 0 {
				j = len(s)
			} else {
				j += i + 1
			}
			toks = append(toks, token{tComment, s[i:j]})
			i = j
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			j := strings.Index(s[i+2:], "*/")
			if j < 0 {
				j = len(s)
			} else {
				j += i + 4
			}
			toks = append(toks, token{tComment, s[i:j]})
			i = j
		case c == '\'':
			j := scanQuoted(i, '\'')
			toks = append(toks, token{tString, s[i:j]})
			i = j
		case c == '"':
			j := scanQuoted(i, '"')
			toks = append(toks, token{tIdent, s[i:j]})
			i = j
		case c == '`':
			j := scanQuoted(i, '`')
			inner := strings.ReplaceAll(s[i+1:max(j-1, i+1)], "``", "`")
			toks = append(toks, token{tIdent, `"` + strings.ReplaceAll(inner, `"`, `""`) + `"`})
			i = j
		case c == '[':
			j := scanQuoted(i, ']')
			inner := s[i+1 : max(j-1, i+1)]
			toks = append(toks, token{tIdent, `"` + strings.ReplaceAll(inner, `"`, `""`) + `"`})
			i = j
		case (c == 'x' || c == 'X') && i+1 < len(s) && s[i+1] == '\'':
			j := scanQuoted(i+1, '\'')
			hex := s[i+2 : max(j-1, i+2)]
			var b strings.Builder
			b.WriteByte('\'')
			for k := 0; k+1 < len(hex); k += 2 {
				b.WriteString(`\x`)
				b.WriteString(strings.ToUpper(hex[k : k+2]))
			}
			b.WriteString(`'::BLOB`)
			toks = append(toks, token{tBlob, b.String()})
			i = j
		case isWordStart(c):
			j := i
			for j < len(s) && isWordChar(s[j]) {
				j++
			}
			toks = append(toks, token{tWord, s[i:j]})
			i = j
		case c >= '0' && c <= '9':
			j := i
			for j < len(s) && (isWordChar(s[j]) || s[j] == '.') {
				j++
			}
			toks = append(toks, token{tOther, s[i:j]})
			i = j
		default:
			toks = append(toks, token{tOther, string(c)})
			i++
		}
	}
	return toks
}

// significant returns the indexes of tokens that are not whitespace/comments.
func significant(toks []token) []int {
	var idx []int
	for i, t := range toks {
		if t.kind != tWS && t.kind != tComment {
			idx = append(idx, i)
		}
	}
	return idx
}

var typeMap = map[string]string{
	"DATETIME": "TIMESTAMP",
	"REAL":     "DOUBLE",
	"CLOB":     "VARCHAR",
}

// Statement translates one statement. The second result reports whether it
// was commented out as unsupported.
func Statement(stmt string) (string, bool) {
	toks := tokenize(stmt)
	sig := significant(toks)
	if len(sig) == 0 {
		return stmt, false
	}
	word := func(n int) string {
		if n < len(sig) && toks[sig[n]].kind == tWord {
			return strings.ToUpper(toks[sig[n]].text)
		}
		return ""
	}
	first := word(0)

	// create-kind helpers: skip TEMP/TEMPORARY/UNIQUE/IF NOT EXISTS when classifying
	kind := ""
	if first == "CREATE" {
		for n := 1; n < len(sig); n++ {
			w := word(n)
			if w == "TEMP" || w == "TEMPORARY" || w == "UNIQUE" {
				continue
			}
			kind = w
			break
		}
	}

	if reason := unsupported(toks, sig, first, kind); reason != "" {
		return commentOut(stmt, reason), true
	}

	isCreateTable := first == "CREATE" && kind == "TABLE"
	depth := 0
	closedBody := false
	var out strings.Builder
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		switch t.kind {
		case tWord:
			up := strings.ToUpper(t.text)
			switch {
			case up == "UNISTR" && nextSigIs(toks, i, "("):
				j := i + 1
				for toks[j].kind == tWS || toks[j].kind == tComment {
					j++
				}
				k := j + 1
				for k < len(toks) && (toks[k].kind == tWS || toks[k].kind == tComment) {
					k++
				}
				if k < len(toks) && toks[k].kind == tString {
					toks[k].text = unistr(toks[k].text)
					toks[k].kind = tOther
				}
				continue
			case up == "AUTOINCREMENT":
				trimTrailingWS(&out)
				continue
			case isCreateTable && depth == 0 && closedBody && (up == "WITHOUT" || up == "ROWID" || up == "STRICT"):
				trimTrailingWS(&out)
				continue
			case isCreateTable && depth >= 1:
				if m, ok := typeMap[up]; ok {
					out.WriteString(m)
					continue
				}
			case !isCreateTable && up == "CHAR" && nextSigIs(toks, i, "("):
				out.WriteString("chr")
				continue
			case first == "BEGIN" && i == sig[0]:
				// BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE] [TRANSACTION] -> BEGIN TRANSACTION
				out.WriteString("BEGIN TRANSACTION")
				for i+1 < len(toks) {
					n := toks[i+1]
					if n.kind == tWS {
						i++
						continue
					}
					u := strings.ToUpper(n.text)
					if n.kind == tWord && (u == "DEFERRED" || u == "IMMEDIATE" || u == "EXCLUSIVE" || u == "TRANSACTION") {
						i++
						continue
					}
					break
				}
				continue
			}
			out.WriteString(t.text)
		case tOther:
			if isCreateTable {
				if t.text == "(" {
					depth++
				} else if t.text == ")" {
					depth--
					if depth == 0 {
						closedBody = true
					}
				}
			}
			out.WriteString(t.text)
		default:
			out.WriteString(t.text)
		}
	}
	return out.String(), false
}

func trimTrailingWS(sb *strings.Builder) {
	s := strings.TrimRight(sb.String(), " \t")
	sb.Reset()
	sb.WriteString(s)
}

func nextSigIs(toks []token, i int, text string) bool {
	for j := i + 1; j < len(toks); j++ {
		if toks[j].kind == tWS || toks[j].kind == tComment {
			continue
		}
		return toks[j].text == text
	}
	return false
}

func unsupported(toks []token, sig []int, first, kind string) string {
	switch first {
	case "PRAGMA":
		return "PRAGMA is SQLite-specific"
	case "VACUUM", "REINDEX":
		return first + " is SQLite-specific"
	case "CREATE":
		switch kind {
		case "TRIGGER":
			return "triggers are not supported by DuckDB"
		case "VIRTUAL":
			return "virtual tables are not supported by DuckDB"
		case "INDEX":
			depth := 0
			for _, i := range sig {
				t := toks[i]
				switch {
				case t.kind == tOther && t.text == "(":
					depth++
				case t.kind == tOther && t.text == ")":
					depth--
				case t.kind == tWord && depth == 0 && strings.EqualFold(t.text, "WHERE"):
					return "partial indexes are not supported by DuckDB"
				}
			}
		}
	}
	for _, i := range sig {
		t := toks[i]
		if (t.kind == tWord || t.kind == tIdent) &&
			(strings.EqualFold(strings.Trim(t.text, `"`), "sqlite_sequence") ||
				strings.EqualFold(strings.Trim(t.text, `"`), "sqlite_master") ||
				strings.EqualFold(strings.Trim(t.text, `"`), "sqlite_schema")) {
			return "SQLite internal table"
		}
	}
	return ""
}

func commentOut(stmt, reason string) string {
	lead := stmt[:len(stmt)-len(strings.TrimLeft(stmt, " \t\r\n"))]
	if !strings.Contains(lead, "\n") {
		lead += "\n"
	}
	var b strings.Builder
	b.WriteString(lead)
	b.WriteString("-- [skipped: " + reason + "]\n")
	for line := range strings.SplitSeq(strings.TrimSpace(stmt), "\n") {
		b.WriteString("-- ")
		b.WriteString(strings.TrimRight(line, "\r"))
		b.WriteByte('\n')
	}
	return b.String()
}

// unistr converts the body of a SQLite unistr('...') literal, which uses
// \uXXXX / \UXXXXXXXX escapes, into a DuckDB expression built from string
// literals and chr() calls.
func unistr(lit string) string {
	body := lit[1 : len(lit)-1] // still has '' escapes
	var parts []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			parts = append(parts, "'"+cur.String()+"'")
			cur.Reset()
		}
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		if c != '\\' || i+1 >= len(body) {
			cur.WriteByte(c)
			continue
		}
		n := body[i+1]
		width := 0
		switch n {
		case '\\':
			cur.WriteByte('\\')
			i++
			continue
		case 'u':
			width = 4
		case 'U':
			width = 8
		case '+':
			width = 6
		}
		if width > 0 && i+2+width <= len(body) {
			if v, err := strconv.ParseUint(body[i+2:i+2+width], 16, 32); err == nil {
				flush()
				parts = append(parts, "chr("+strconv.FormatUint(v, 10)+")")
				i += 1 + width
				continue
			}
		}
		cur.WriteByte(c)
	}
	flush()
	if len(parts) == 0 {
		return "''"
	}
	return strings.Join(parts, " || ")
}
