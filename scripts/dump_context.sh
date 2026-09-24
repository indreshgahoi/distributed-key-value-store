#!/usr/bin/env bash
# Flattens this repo into two plain-text files sized for pasting as LLM
# context (e.g. Gemini) - one for source code, one for prose docs. Each
# file's content is wrapped in a clear "FILE: <path>" header so the model
# can still tell where one file ends and the next begins once everything
# is concatenated.
#
# Uses `git ls-files` so anything gitignored (data/, the compiled
# kv-server binary, etc.) is automatically excluded, and untracked scratch
# files never leak into the dump. Only tracked files are considered.
#
# Usage: scripts/dump_context.sh [output-dir]
#   output-dir defaults to ./context (gitignored - these are generated,
#   throwaway artifacts, not something to commit)
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

OUT_DIR="${1:-context}"
CODE_FILE="$OUT_DIR/code.txt"
DOCS_FILE="$OUT_DIR/docs.txt"

mkdir -p "$OUT_DIR"
: > "$CODE_FILE"
: > "$DOCS_FILE"

dump_file() {
	local f="$1" out="$2"
	{
		printf '%s\n' "================================================================================"
		printf 'FILE: %s\n' "$f"
		printf '%s\n' "================================================================================"
		cat "$f"
		printf '\n'
	} >> "$out"
}

while IFS= read -r f; do
	case "$f" in
	*.md)
		dump_file "$f" "$DOCS_FILE"
		;;
	*.go | go.mod | go.sum | *.sh)
		dump_file "$f" "$CODE_FILE"
		;;
	*)
		# Skip everything else on purpose: LICENSE, bench/*.txt baselines,
		# .gitignore - noise for an LLM context dump, not code or docs.
		;;
	esac
done < <(git ls-files)

printf 'Code -> %s (%s lines, %s)\n' "$CODE_FILE" "$(wc -l <"$CODE_FILE")" "$(du -h "$CODE_FILE" | cut -f1)"
printf 'Docs -> %s (%s lines, %s)\n' "$DOCS_FILE" "$(wc -l <"$DOCS_FILE")" "$(du -h "$DOCS_FILE" | cut -f1)"
