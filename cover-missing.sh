#!/usr/bin/env bash
# Show uncovered Go coverage blocks inline in the terminal.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: cover-missing.sh [OPTIONS] [profile]

Show uncovered Go coverage blocks inline in the terminal.

Options:
  -p, --profile PATH   Coverage profile to read (default: coverage.txt)
  -f, --file FILE      Only include entries whose path contains FILE. Repeatable.
  -h, --help           Show this help and exit.
EOF
}

PROFILE="coverage.txt"
PROFILE_SPECIFIED=0
FILTERS=()

while (($#)); do
  case "$1" in
    -p|--profile)
      if (($# < 2)); then
        echo "Missing argument for $1" >&2
        usage >&2
        exit 1
      fi
      PROFILE="$2"
      PROFILE_SPECIFIED=1
      shift 2
      ;;
    -f|--file)
      if (($# < 2)); then
        echo "Missing argument for $1" >&2
        usage >&2
        exit 1
      fi
      FILTERS+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -* )
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      if ((PROFILE_SPECIFIED == 0)); then
        PROFILE="$1"
        PROFILE_SPECIFIED=1
        shift
      else
        echo "Unexpected argument: $1" >&2
        usage >&2
        exit 1
      fi
      ;;
  esac
done

if (($#)); then
  echo "Unexpected argument(s): $*" >&2
  usage >&2
  exit 1
fi

if [[ ! -f "$PROFILE" ]]; then
  echo "Coverage profile not found: $PROFILE" >&2
  exit 1
fi

# Discover current module import path and local dir
# (e.g., MODULE_PATH=github.com/metailurini/rindb, MODULE_DIR=/abs/path/to/repo)
read -r MODULE_PATH MODULE_DIR < <(go list -m -f '{{.Path}} {{.Dir}}' 2>/dev/null || echo " _")
if [[ -z "${MODULE_PATH:-}" || -z "${MODULE_DIR:-}" || "$MODULE_PATH" == "_" ]]; then
  MODULE_PATH=""
  MODULE_DIR=""
fi

map_to_fs_path() {
  local p="$1"
  # If it already exists, keep it
  [[ -f "$p" ]] && { echo "$p"; return; }
  # Try relative to cwd
  [[ -f "./$p" ]] && { echo "./$p"; return; }
  # If we have module info and path starts with module import path, remap
  if [[ -n "$MODULE_PATH" && "$p" == "$MODULE_PATH/"* && -n "$MODULE_DIR" ]]; then
    local rest="${p#${MODULE_PATH}/}"
    [[ -f "$MODULE_DIR/$rest" ]] && { echo "$MODULE_DIR/$rest"; return; }
  fi
  # Some tools stick a "file=" prefix; strip it
  if [[ "$p" == file=* ]]; then
    local q="${p#file=}"
    map_to_fs_path "$q" && return
  fi
  # Give up; return as-is
  echo ""
}

matches_filter() {
  local real="$1"
  local original="$2"
  if ((${#FILTERS[@]} == 0)); then
    return 0
  fi
  local filter
  for filter in "${FILTERS[@]}"; do
    if [[ -n "$real" && "$real" == *"$filter"* ]]; then
      return 0
    fi
    if [[ "$original" == *"$filter"* ]]; then
      return 0
    fi
  done
  return 1
}

RED="$(printf '\033[31m')"
BOLD="$(printf '\033[1m')"
DIM="$(printf '\033[2m')"
RESET="$(printf '\033[0m')"

# Parse the profile and print zero-count blocks
awk 'NR==1 { next } {
  split($1, a, ":"); file=a[1]
  split(a[2], b, ",")
  split(b[1], s, "."); split(b[2], e, ".")
  numStmts=$2; cnt=$3
  if (cnt+0 == 0) {
    printf("%s\t%d\t%d\t%d\t%d\n", file, s[1], s[2], e[1], e[2])
  }
}' "$PROFILE" | while IFS=$'\t' read -r file sline scol eline ecol; do
  real="$(map_to_fs_path "$file")"
  if ! matches_filter "$real" "$file"; then
    continue
  fi
  if [[ -z "$real" || ! -f "$real" ]]; then
    echo -e "${DIM}Skipping (file not found): ${file}:${sline}-${eline}${RESET}"
    continue
  fi

  echo
  echo -e "${BOLD}${real}:${sline}-${eline}${RESET}  ${DIM}(missed block; start col ${scol}, end col ${ecol})${RESET}"

  awk -v sl="$sline" -v el="$eline" -v RED="$RED" -v RESET="$RESET" '
    NR >= sl && NR <= el { printf("%6d | %s%s%s\n", NR, RED, $0, RESET) }
  ' "$real"
done

echo
echo -e "${DIM}Tip:${RESET} Per-function summary: \`go tool cover -func="$PROFILE"\`. HTML view: \`go tool cover -html="$PROFILE"\`."
