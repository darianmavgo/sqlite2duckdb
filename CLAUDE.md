# CLAUDE.md

## Building

Always build binaries into `bin/` (git-ignored), never next to the source or in the repo root:

```sh
go build -o ./bin ./...
```

For a single command, use `go build -o ./bin/<name> ./cmd/<name>`. Don't leave built binaries anywhere else.
