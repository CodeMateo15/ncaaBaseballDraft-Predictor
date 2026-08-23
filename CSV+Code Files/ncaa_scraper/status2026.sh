#!/bin/bash
# Progress of the 2026 live scrape. Safe to run any time -- reads the cache and
# the log only, and makes no network request.
D="/Users/mateobiggs/ncaaBaseballDraft-Predictor/CSV+Code Files/ncaa_scraper/cache/d1/2026"
LOG=/tmp/scrape2026.log
TOTAL=308

cached=$(ls "$D" 2>/dev/null | grep -c '_batting.rows.json.gz')
first=$(grep "=== session 1/.* cached | " $LOG 2>/dev/null | tail -1)
base=$(echo "$first" | sed -n 's/.*| \([0-9]*\)\/308 cached.*/\1/p')
started=$(echo "$first" | sed -n 's/.*cached | \([0-9:]*\) ===.*/\1/p')
: "${base:=$cached}"

sessions=$(grep -c "=== session .* rc=" $LOG 2>/dev/null)
blocks=$(grep -c "^=== 403\." $LOG 2>/dev/null)
# Totals since the current resumption only.
slice=$(awk "/=== session 1\/.* cached \| /{f=1} f" $LOG 2>/dev/null | tail -n +1)
r_sessions=$(echo "$slice" | grep -c "=== session .* rc=")
r_blocks=$(echo "$slice" | grep -c "^=== 403\.")
gained=$((cached - base))
left=$((TOTAL - cached))

# Elapsed since the first session started, in minutes.
if [ -n "$started" ]; then
  now_s=$(date +%s)
  st_s=$(date -j -f "%H:%M" "$started" +%s 2>/dev/null)
  today_mid=$(date -j -f "%H:%M" "00:00" +%s 2>/dev/null)
  [ -n "$st_s" ] && elapsed=$(( (now_s - st_s) / 60 ))
  [ "$elapsed" -lt 0 ] 2>/dev/null && elapsed=$((elapsed + 1440))
fi

echo "───── 2026 scrape ─────"
# The final push runs from a different script; checking only for the supervisor
# reported "not running" while final2026.sh was actively waiting for the window.
if ps -ax -o command= | grep -q '^/bin/bash \./final2026\.sh$'; then
  if ps -ax -o command= | grep -q "^/opt/anaconda3/bin/python3 -u run.py --year 2026"; then
    echo "status     final push: fetching now"
  else
    echo "status     final push: waiting for the rolling window to clear"
  fi
elif ps -ax -o command= | grep -q '^/bin/bash \./supervise2026\.sh$'; then
  if pgrep -f "run.py --year 2026" | while read p; do ps -o command= -p "$p" 2>/dev/null | grep -q "^/opt/anaconda3"; done; then :; fi
  if ps -ax -o command= | grep -q "^/opt/anaconda3/bin/python3 -u run.py --year 2026"; then
    echo "status     fetching now"
  else
    echo "status     waiting between sessions"
  fi
else
  echo "status     not running"
fi
printf "teams      %d/%d cached  (%d%%)\n" "$cached" "$TOTAL" $((cached * 100 / TOTAL))
printf "this run   +%d teams fetched over %s min\n" "$gained" "${elapsed:-?}"
printf "sessions   %d this run (%d blocked)  |  %d all time (%d blocked)\n" \
  "${r_sessions:-0}" "${r_blocks:-0}" "${sessions:-0}" "${blocks:-0}"
printf "remaining  %d teams\n" "$left"
if [ "${gained:-0}" -gt 0 ] && [ -n "$elapsed" ] && [ "$elapsed" -gt 0 ]; then
  rate=$(( gained * 60 / elapsed ))
  [ "$rate" -gt 0 ] && printf "pace       ~%d teams/hr -> ~%d hr left\n" "$rate" $(( left / rate ))
fi
echo "last       $(grep -E 'team-seasons \(|STOPPED:' $LOG | tail -1 | sed 's/^ *//')"
