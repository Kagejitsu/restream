#!/usr/bin/env bash
# Assemble a self-contained Windows dist/ folder ON LINUX from the cross-built
# release binaries and the GStreamer MSVC SDK in the Wine prefix
# (same layout as tools/win-bundle.ps1): restream.exe, restream-cli.exe,
# runtime DLLs next to them, plugins in gst-plugins\, the user guide.
#
# usage: tools/linux-bundle.sh [dist-dir]
set -euo pipefail
cd "$(dirname "$0")/.."
GST=${GST_MSVC_ROOT:-$HOME/.local/share/gst-msvc}
REL=target/x86_64-pc-windows-msvc/release
DIST=${1:-dist}
[ -f "$REL/restream.exe" ] || { echo "build first: tools/xwin-build.sh --release"; exit 2; }

rm -rf "$DIST"; mkdir -p "$DIST/gst-plugins"
cp "$REL/restream.exe" "$REL/restream-cli.exe" "$DIST/"
cp "$GST"/bin/*.dll "$DIST/"
cp "$GST/bin/gst-inspect-1.0.exe" "$DIST/" 2>/dev/null || true

plugins=(coreelements app typefindfunctions playback
  videoconvertscale videorate videotestsrc audiotestsrc audioconvert audioresample level
  videoparsersbad audioparsers flv matroska isomp4 multifile jpeg pango
  x264 openh264 fdkaac voaacenc libav
  mediafoundation winks directshow wasapi2 wasapi decklink
  nvcodec amfcodec qsv d3d11 d3d12 va
  srt rtmp2 dtls sctp)
for p in "${plugins[@]}"; do
  f="$GST/lib/gstreamer-1.0/gst$p.dll"
  if [ -f "$f" ]; then cp "$f" "$DIST/gst-plugins/"; else echo "  (no plugin $p)"; fi
done
cp docs/USER-GUIDE.md "$DIST/"
cat > "$DIST/README.txt" <<'EOF'
Restream - delayed multi-destination restreamer

  restream.exe      the app (double-click)
  restream-cli.exe  command-line runner, e.g.  restream-cli encoders
  gst-plugins\      GStreamer plugins (found automatically)
  USER-GUIDE.md     how to set it up

First run: Windows SmartScreen may warn because the exe is not code-signed.
Choose "More info" -> "Run anyway".
EOF
du -sh "$DIST" | cut -f1 | xargs -I{} echo "dist ready: {} in $DIST"
