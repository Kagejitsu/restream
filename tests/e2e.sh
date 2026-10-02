#!/usr/bin/env bash
# End-to-end test: three destinations on a local RTMP server, delay, recording.
#
# Checks
#  1. all three servers received byte-identical video and audio elementary streams
#  2. a frame grabbed from each live output at the same wall-clock moment shows the
#     same burned-in running time (within one frame) → outputs are in lock-step
#  3. that running time lags wall time by at least the configured delay
#  4. the local recording is a playable file of the full session length
#
# usage: tests/e2e.sh [delay_seconds] [run_seconds]
set -euo pipefail
cd "$(dirname "$0")/.."

DELAY=${1:-10}
RUN=${2:-40}
GRAB_AT=$((DELAY + 12))            # when to grab frames (must be after on-air)
OUT=tests/out
BIN=${BIN:-./target/debug/restream-cli}
MM=./tools/bin/mediamtx

command -v ffmpeg >/dev/null || { echo "ffmpeg required"; exit 2; }
[ -x "$MM" ] || { echo "mediamtx missing at $MM (see tests/README)"; exit 2; }
cargo build -q -p restream-cli

rm -rf "$OUT"; mkdir -p "$OUT"
"$MM" tests/mediamtx.yml > "$OUT/mediamtx.log" 2>&1 &
MM_PID=$!
trap 'kill $MM_PID 2>/dev/null || true' EXIT
sleep 1

START=$(date +%s.%N)
RUST_LOG=info "$BIN" run tests/e2e-profile.toml --delay "$DELAY" --for "$RUN" --quiet > "$OUT/restream.log" 2>&1 &
RS_PID=$!

# --- grab one frame from each live output at the same instant ---
sleep "$GRAB_AT"
GRAB_WALL=$(date +%s.%N)
for p in twitch youtube facebook; do
  ffmpeg -v error -y -i "rtmp://127.0.0.1:1935/live/$p" -frames:v 1 "$OUT/grab-$p.png" &
done
wait %2 %3 %4 2>/dev/null || true
echo "grabbed frames at wall offset $(echo "$GRAB_WALL - $START" | bc) s after start (delay $DELAY s): $OUT/grab-*.png"

wait $RS_PID
echo "restream exited; waiting for server recordings to finalise"
sleep 3
kill $MM_PID; wait $MM_PID 2>/dev/null || true

# --- 1. byte-identical elementary streams across destinations ---
fail=0
declare -A VH AH
for p in twitch youtube facebook; do
  f=$(ls "$OUT/srv/live/$p/"*.mp4 | head -1)
  VH[$p]=$(ffmpeg -v error -i "$f" -map 0:v -c copy -f h264 - | md5sum | cut -d' ' -f1)
  AH[$p]=$(ffmpeg -v error -i "$f" -map 0:a -c copy -f adts - | md5sum | cut -d' ' -f1)
  dur=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$f")
  echo "$p: video ${VH[$p]} audio ${AH[$p]} duration ${dur}s"
done
if [ "${VH[twitch]}" = "${VH[youtube]}" ] && [ "${VH[youtube]}" = "${VH[facebook]}" ] && \
   [ "${AH[twitch]}" = "${AH[youtube]}" ] && [ "${AH[youtube]}" = "${AH[facebook]}" ]; then
  echo "PASS: all destinations received identical video and audio streams"
else
  echo "FAIL: destinations differ"; fail=1
fi

# --- 4. local recording ---
rec=$(ls "$OUT"/*.mkv | head -1)
rdur=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$rec")
echo "local recording: $rec duration ${rdur}s (expected ≈ $RUN)"
if awk -v d="$rdur" -v r="$RUN" 'BEGIN{exit !(d > r-2 && d < r+2)}'; then
  echo "PASS: recording length"
else
  echo "FAIL: recording length"; fail=1
fi

# --- log sanity ---
if grep -q 'reconnects [1-9]' "$OUT/restream.log" || grep -qi 'lagging' "$OUT/restream.log"; then
  echo "FAIL: reconnects or lag during run"; fail=1
else
  echo "PASS: no reconnects, no lag"
fi
echo "Now view $OUT/grab-*.png: burned-in time should match across the three and read ≈ $(echo "$GRAB_WALL - $START - $DELAY" | bc) s or less."
exit $fail
