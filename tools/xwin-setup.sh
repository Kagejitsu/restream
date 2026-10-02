#!/usr/bin/env bash
# One-time setup for cross-compiling the Windows build on Linux and smoke
# testing it under Wine. Everything lands under $HOME; nothing system-wide.
#
#   1. user-space rustup with the x86_64-pc-windows-msvc target + cargo-xwin
#   2. a Wine prefix with the GStreamer MSVC x86_64 SDK (Inno Setup installer,
#      run silently) — gives both the import libs for linking and the runtime
#      for running the .exe under Wine
#   3. a space-free symlink to the SDK and pkg-config prefixes pointing at it
#
# Needs: curl, wine, clang-cl, lld-link, llvm-lib (Arch: clang lld llvm).
set -euo pipefail
GST_VER=${GST_VER:-1.28.7}
PREFIX="$HOME/.local/share/wineprefixes/restream"
LINK="$HOME/.local/share/gst-msvc"
TMP=${TMPDIR:-/tmp}/restream-xwin-setup
mkdir -p "$TMP"

# 1. toolchain
if [ ! -x "$HOME/.cargo/bin/rustup" ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o "$TMP/rustup-init.sh"
  sh "$TMP/rustup-init.sh" -y --no-modify-path --default-toolchain stable --profile minimal
fi
export PATH="$HOME/.cargo/bin:$PATH"
rustup target add x86_64-pc-windows-msvc
command -v cargo-xwin >/dev/null || cargo install cargo-xwin

# 2. GStreamer MSVC SDK into a Wine prefix
exe="gstreamer-1.0-msvc-x86_64-$GST_VER.exe"
url="https://gstreamer.freedesktop.org/data/pkg/windows/$GST_VER/msvc"
if [ ! -f "$TMP/$exe" ]; then
  curl -fL -o "$TMP/$exe" "$url/$exe"
  curl -fsSL -o "$TMP/$exe.sha256sum" "$url/$exe.sha256sum"
  (cd "$TMP" && sha256sum -c "$exe.sha256sum")
fi
export WINEPREFIX="$PREFIX" WINEDEBUG=-all
mkdir -p "$PREFIX"
wineboot -u
wine "$TMP/$exe" /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /TYPE=full /LOG='C:\gst-install.log' || true
SDK="$PREFIX/drive_c/Program Files/gstreamer/1.0/msvc_x86_64"
[ -f "$SDK/lib/pkgconfig/gstreamer-1.0.pc" ] || { echo "install failed; see $PREFIX/drive_c/gst-install.log"; exit 1; }

# 3. space-free path for pkg-config
ln -sfn "$SDK" "$LINK"
sed -i "s|^prefix=.*|prefix=$LINK|" "$LINK/lib/pkgconfig/"*.pc
PKG_CONFIG_LIBDIR="$LINK/lib/pkgconfig" pkg-config --libs gstreamer-1.0 >/dev/null
echo "ready. build with tools/xwin-build.sh [--release]; run under Wine with tools/bin/restream-cli-wine"
