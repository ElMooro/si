#!/usr/bin/env bash
# justhodl-engines-off-check.sh - are all JustHodl engines off?  Run from Git Bash on Windows:
#
#     bash justhodl-engines-off-check.sh
#
# It asks GitHub to run ops 6545 on the AWS runner (the only machine with AWS keys), waits for it, and prints
# the verdict.  ops 6545 reads every Lambda's concurrency, who ran in the last 20 min / 24 h, SageMaker, EC2,
# ECS, RDS, App Runner and Step Functions.  If it finds anything on, it has the spend governor re-apply the
# freeze (restored exactly on Nov 1) and checks again.
#
# GitHub login: uses the one Git already has (Git Credential Manager, the ElMooro account).  The token is
# never printed.  Needs only git and curl, both part of Git for Windows.  Exit 0 = all off, 1 = something on.
# shellcheck disable=SC2016  # backticks in sed patterns are literal markdown from the report
set -uo pipefail

REPO="ElMooro/si"
WORKFLOW="run-ops-direct.yml"
SCRIPT="STAGED/ops_6545_fleet_off_check.py"
REPORT="aws/ops/reports/latest/ops_6545_fleet_off_check.md"
API="https://api.github.com/repos/$REPO"

die() { printf 'ERROR: %s\n' "$*" >&2; exit 2; }

TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
if [ -z "$TOKEN" ]; then
  TOKEN="$(printf 'protocol=https\nhost=github.com\n\n' | GIT_TERMINAL_PROMPT=0 git credential fill 2>/dev/null \
           | sed -n 's/^password=//p')"
fi
[ -n "$TOKEN" ] || die "no GitHub login found. Run:  git credential-manager github login   then run this again."

gh_api() {  # gh_api METHOD PATH [JSON-BODY]; prints the body, then the HTTP code on the last line
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -X "$method" -H "Authorization: Bearer $TOKEN" -H "Accept: application/vnd.github+json" \
         -H "Content-Type: application/json" -H "X-GitHub-Api-Version: 2022-11-28" -d "$body" \
         -w '\n%{http_code}' "$API$path"
  else
    curl -sS -X "$method" -H "Authorization: Bearer $TOKEN" -H "Accept: application/vnd.github+json" \
         -H "X-GitHub-Api-Version: 2022-11-28" -w '\n%{http_code}' "$API$path"
  fi
}
http_code() { tail -n 1; }
http_body() { sed '$d'; }
# first match anywhere (GitHub may send the JSON on one line, so grep -m1 alone is not enough)
first_num()  { grep -oE "\"$1\": *[0-9]+" | head -n 1 | grep -oE '[0-9]+$'; }
first_str()  { grep -oE "\"$1\": *(\"[^\"]*\"|null)" | head -n 1 | sed -E 's/^"[^"]+": *//; s/"//g'; }

newest_run_id() {
  gh_api GET "/actions/workflows/$WORKFLOW/runs?per_page=1&event=workflow_dispatch" | http_body | first_num id
}

before_id="$(newest_run_id)"
[ -n "$before_id" ] || before_id=0
started="$(date -u +%Y-%m-%dT%H:%M:%S)"

resp="$(gh_api POST "/actions/workflows/$WORKFLOW/dispatches" \
        "{\"ref\":\"main\",\"inputs\":{\"script\":\"$SCRIPT\"}}")"
code="$(printf '%s\n' "$resp" | http_code)"
[ "$code" = "204" ] || die "GitHub refused the run (HTTP $code): $(printf '%s\n' "$resp" | http_body | head -c 300)"
echo "Started the engines-off check on the AWS runner ($(date -u +%H:%M) UTC). This takes 3-8 minutes..."

run_id=""
for _ in $(seq 1 30); do
  sleep 6
  id="$(newest_run_id)"
  if [ -n "$id" ] && [ "$id" -gt "$before_id" ]; then run_id="$id"; break; fi
done
[ -n "$run_id" ] || die "the run did not appear within 3 minutes - see https://github.com/$REPO/actions/workflows/$WORKFLOW"

status="" conclusion=""
for _ in $(seq 1 120); do
  run="$(gh_api GET "/actions/runs/$run_id" | http_body)"
  status="$(printf '%s' "$run" | first_str status)"
  conclusion="$(printf '%s' "$run" | first_str conclusion)"
  [ "$status" = "completed" ] && break
  printf '.'
  sleep 15
done
echo
[ "$status" = "completed" ] || die "still running after 30 minutes - see https://github.com/$REPO/actions/runs/$run_id"

sleep 5  # the report commit lands at the end of the same job
md="$(curl -sS -H "Authorization: Bearer $TOKEN" -H "Accept: application/vnd.github.raw" \
      "$API/contents/$REPORT?ref=main")"
finished="$(printf '%s\n' "$md" | grep -m1 -oE 'Finished:\*\* *[0-9T:-]+' | grep -oE '[0-9]{4}-[0-9T:-]+')"
if [ -z "$finished" ] || [[ "$finished" < "$started" ]]; then
  echo "The run finished ($conclusion) but its report did not reach the repo. Full log:"
  echo "  https://github.com/$REPO/actions/runs/$run_id"
  exit 1
fi

echo
echo "================ JustHodl engines check  ($finished UTC) ================"
verdict="$(printf '%s\n' "$md" | grep -E 'ALL ENGINES OFF|STILL ON' | tail -n 1 | sed -E 's/^- `[0-9:]+` //')"
printf '%s\n\n' "${verdict:-no verdict line - open the full report}"
printf '%s\n' "$md" | grep -E 'governor v|last governor tick|every non-exempt|non-exempt functions that can run|invocations [0-9]{2}:[0-9]{2}-|in the last 24 h|endpoint |running:|concurrency unreadable|cannot prove|Cost Explorer shows|still billing|exempt by design|re-held after|governor tick:|governor force' \
  | sed -E 's/^- `[0-9:]+` /  /' | cut -c1-240
echo
echo "Full report: https://github.com/$REPO/blob/main/$REPORT"
echo "Run log:     https://github.com/$REPO/actions/runs/$run_id"
case "$verdict" in *"ALL ENGINES OFF"*) exit 0 ;; *) exit 1 ;; esac
