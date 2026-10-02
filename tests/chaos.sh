#!/usr/bin/env bash
# Chaos test: one destination's server dies mid-session and comes back.
#
# Twitch + YouTube publish to server A (1935); Facebook publishes to server B
# (1936). B is killed while on air and restarted a few seconds later.
#
# Expect
#  - A's two recordings stay byte-identical and full length (unaffected)
#  - Facebook shows reconnects ≥ 1 and resumes (B has a second recording)
#  - no lag on the surviving destinations
set -euo pipefail
cd "$(dirname "$0")/.."
DELAY=${1:-8}
RUN=${2:-45}
OUT=tests/out
BIN=./target/debug/restream-cli
MM=./tools/bin/mediamtx
cargo build -q -p restream-cli
rm -rf "$OUT"; mkdir -p "$OUT"

"$MM" tests/mediamtx.yml   > "$OUT/mediamtx-a.log" 2>&1 & A=$!
"$MM" tests/mediamtx-b.yml > "$OUT/mediamtx-b.log" 2>&1 & B=$!
trap 'kill $A $B 2>/dev/null || true' EXIT
sleep 1

sed 's|^url = "rtmp://127.0.0.1:1935/live"|url = "rtmp://127.0.0.1:1935/live"|' tests/e2e-profile.toml \
 | awk 'BEGIN{n=0} /^\[\[destinations\]\]/{n++} { if (n==3 && $0 ~ /^url/) print "url = \"rtmp://127.0.0.1:1936/live\""; else print }' \
 > "$OUT/chaos-profile.toml"

RUST_LOG=info "$BIN" run "$OUT/chaos-profile.toml" --delay "$DELAY" --for "$RUN" --record "$OUT" > "$OUT/restream.log" 2>&1 &
RS=$!

sleep $((DELAY + 10))
echo "killing server B (Facebook)"
kill $B; wait $B 2>/dev/null || true
sleep 6
echo "restarting server B"
"$MM" tests/mediamtx-b.yml >> "$OUT/mediamtx-b.log" 2>&1 & B=$!
wait $RS
sleep 3
kill $A $B; wait $A $B 2>/dev/null || true

fail=0
hv=(); ha=()
for p in twitch youtube; do
  f=$(ls "$OUT/srv/live/$p/"*.mp4 | head -1)
  hv+=("$(ffmpeg -v error -i "$f" -map 0:v -c copy -f h264 - | md5sum | cut -d' ' -f1)")
  ha+=("$(ffmpeg -v error -i "$f" -map 0:a -c copy -f adts - | md5sum | cut -d' ' -f1)")
  echo "$p: $(ffprobe -v error -show_entries format=duration -of csv=p=0 "$f")s"
done
[ "${hv[0]}" = "${hv[1]}" ] && [ "${ha[0]}" = "${ha[1]}" ] && echo "PASS: survivors identical" || { echo "FAIL: survivors differ"; fail=1; }

LOG=$(sed 's/\x1b\[[0-9;]*m//g' "$OUT/restream.log")
echo "$LOG" | grep -E 'Facebook' | grep -E 'connection lost|connect failed|connected' | cut -c1-140
nfb=$(ls "$OUT/srv-b/live/facebook/"*.mp4 2>/dev/null | wc -l)
echo "Facebook recordings on server B: $nfb (expect 2: before and after the kill)"
[ "$nfb" -ge 2 ] && echo "PASS: Facebook rejoined" || { echo "FAIL: Facebook did not rejoin"; fail=1; }
if echo "$LOG" | grep -E '^\s+(Twitch|YouTube) ' | grep -qiE 'lagging|reconnecting'; then echo "FAIL: survivors lagged"; fail=1; else echo "PASS: survivors never lagged"; fi
exit $fail
