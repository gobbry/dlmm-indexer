#!/usr/bin/env bash
# show-me: write a page into ./show-me/ in the current project, open it as a
# plain file:// URL, and reload the tab that already has it.
#
#   show.sh <slug> [--pdf]
#   show.sh --where          print the folder that would be used
#   show.sh --clean          delete that folder
#
# Folder resolution: $SHOW_ME_DIR, else <git root>/show-me, else ./show-me,
# except when run from $HOME, which uses ~/.show-me so it never litters home.
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -n "${SHOW_ME_DIR:-}" ]; then
  ROOT="$SHOW_ME_DIR"
elif repo=$(git rev-parse --show-toplevel 2>/dev/null) && [ -n "$repo" ]; then
  ROOT="$repo/show-me"
elif [ "$PWD" = "$HOME" ]; then
  ROOT="$HOME/.show-me"
else
  ROOT="$PWD/show-me"
fi
ASSETS="$ROOT/_assets"

setup() {
  mkdir -p "$ASSETS"
  # Self-ignoring folder: git never sees these pages, and the repo's own
  # .gitignore is left untouched.
  [ -f "$ROOT/.gitignore" ] || printf '*\n' >"$ROOT/.gitignore"
  [ "$SKILL_DIR/base.css" -nt "$ASSETS/base.css" ] 2>/dev/null && cp "$SKILL_DIR/base.css" "$ASSETS/base.css"
  [ -f "$ASSETS/base.css" ] || cp "$SKILL_DIR/base.css" "$ASSETS/base.css"
  return 0
}

slug=""; want_pdf=0
for arg in "$@"; do
  case "$arg" in
    --where) setup; echo "$ROOT"; exit 0 ;;
    --clean)
      case "$(basename "$ROOT")" in
        show-me|.show-me) rm -rf "$ROOT"; echo "removed $ROOT"; exit 0 ;;
        *) echo "show.sh: refusing to delete $ROOT" >&2; exit 1 ;;
      esac ;;
    --pdf) want_pdf=1 ;;
    -*)    echo "show.sh: unknown flag $arg" >&2; exit 2 ;;
    *)     slug="${arg%.html}"; slug="${slug##*/}" ;;
  esac
done
[ -n "$slug" ] || { echo "usage: show.sh <slug> [--pdf] | --where | --clean" >&2; exit 2; }

setup

page="$ROOT/$slug.html"
[ -f "$page" ] || { echo "show.sh: $page does not exist — write it first" >&2; exit 1; }
url="file://$page"
needle=$(printf '%s' "$page" | sed 's/ /%20/g')

running() { osascript -e "tell application \"System Events\" to (name of processes) contains \"$1\"" 2>/dev/null; }

hits=0
for app in "Brave Browser" "Google Chrome" "Microsoft Edge" "Chromium"; do
  [ "$(running "$app")" = "true" ] || continue
  n=$(osascript 2>/dev/null <<OSA || true
tell application "$app"
  set hits to 0
  repeat with w in windows
    repeat with t in tabs of w
      if URL of t contains "$needle" then
        tell t to reload
        set hits to hits + 1
      end if
    end repeat
  end repeat
  return hits
end tell
OSA
)
  hits=$(( hits + ${n:-0} ))
done

if [ "$(running Safari)" = "true" ]; then
  n=$(osascript 2>/dev/null <<OSA || true
tell application "Safari"
  set hits to 0
  repeat with w in windows
    repeat with t in tabs of w
      if URL of t contains "$needle" then
        set URL of t to (URL of t)
        set hits to hits + 1
      end if
    end repeat
  end repeat
  return hits
end tell
OSA
)
  hits=$(( hits + ${n:-0} ))
fi

if [ "$hits" -gt 0 ]; then
  echo "reloaded $hits tab(s): $url"
else
  open "$url"
  echo "opened $url"
fi

if [ "$want_pdf" = 1 ]; then
  chromium=""
  for c in "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser" \
           "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
           "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge" \
           "/Applications/Chromium.app/Contents/MacOS/Chromium"; do
    [ -x "$c" ] && { chromium="$c"; break; }
  done
  if [ -n "$chromium" ]; then
    "$chromium" --headless --disable-gpu --no-pdf-header-footer \
      --print-to-pdf="$ROOT/$slug.pdf" "$url" >/dev/null 2>&1
    echo "pdf: $ROOT/$slug.pdf"
    open "$ROOT/$slug.pdf"
  else
    echo "no Chromium-based browser found — use Cmd+P > Save as PDF on the open page" >&2
  fi
fi
