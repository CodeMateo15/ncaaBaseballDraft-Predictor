#!/bin/bash
#
# Scrape every year in config.YEARS, start to finish, unattended.
#
#   ./scrape_all.sh
#
# Run it in your own terminal, not from a tool that caps process lifetime -- the
# whole job is ~11 hours of mostly waiting. Safe to interrupt with Ctrl-C at any
# point: cached pages cost no requests, so re-running resumes where it stopped.
#
# Pacing is handled inside run.py by the rolling hourly request budget
# (ncaa/budget.py), which persists to cache/_request_log.json and is therefore
# respected across restarts. This script only handles the case where the site
# blocks us anyway: it waits an hour and tries again, up to MAX_ATTEMPTS.
#
# Exit codes from run.py:
#   0  finished, CSVs written, all acceptance gates passed
#   1  finished but something failed validation -- read the output, do not retry
#   2  stopped early (our budget, or a 403 block) -- retry later

set -u

PYTHON="${PYTHON:-/opt/anaconda3/bin/python}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-24}"
WAIT_MINUTES="${WAIT_MINUTES:-60}"

cd "$(dirname "$0")" || exit 1

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  echo
  echo "=============================================================="
  echo " attempt $attempt/$MAX_ATTEMPTS  --  $(date '+%Y-%m-%d %H:%M:%S')"
  echo "=============================================================="

  # A large per-run budget on purpose: the hourly cap is the real protection and
  # it paces this run internally. The 400-request default exists to stop an
  # *interactive* run before it can walk into the wall.
  "$PYTHON" run.py --request-budget 100000
  status=$?

  case "$status" in
    0)
      echo
      echo "DONE -- CSVs written to out/ and all gates passed."
      exit 0
      ;;
    2)
      echo
      echo "Stopped early (budget spent or blocked). Waiting ${WAIT_MINUTES}m before retrying."
      echo "Progress is cached; nothing will be refetched."
      sleep $((WAIT_MINUTES * 60))
      ;;
    *)
      echo
      echo "run.py exited $status -- this is a real failure, not a rate stop."
      echo "Read the output above and out/reports/ before retrying."
      exit "$status"
      ;;
  esac
done

echo "Gave up after $MAX_ATTEMPTS attempts."
exit 2
