#!/usr/bin/env bash
# Cross-compile the Windows (x86_64-pc-windows-msvc) build from Linux.
#
# One-time setup (see tools/xwin-setup.sh):
#   - rustup toolchain with target x86_64-pc-windows-msvc, cargo-xwin
#   - clang-cl / lld-link / llvm-lib (Arch: clang, lld, llvm)
#   - GStreamer MSVC x86_64 installed into a Wine prefix (its .pc files
#     rewritten to the Linux path), so pkg-config can resolve the import libs
#
# usage: tools/xwin-build.sh [--release] [extra cargo args]
set -euo pipefail
cd "$(dirname "$0")/.."
GST=${GST_MSVC_ROOT:-$HOME/.local/share/gst-msvc}
[ -f "$GST/lib/pkgconfig/gstreamer-1.0.pc" ] || { echo "GStreamer MSVC SDK not found at $GST (run tools/xwin-setup.sh)"; exit 2; }
export PATH="$HOME/.cargo/bin:$PATH"
export PKG_CONFIG_ALLOW_CROSS=1
export PKG_CONFIG_PATH_x86_64_pc_windows_msvc="$GST/lib/pkgconfig"
export PKG_CONFIG_LIBDIR_x86_64_pc_windows_msvc="$GST/lib/pkgconfig"
exec cargo xwin build --target x86_64-pc-windows-msvc -p restream-gui -p restream-cli -p restream-launcher "$@"
