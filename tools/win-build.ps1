# Build Restream on Windows (run from a "x64 Native Tools" or normal PowerShell).
#
# Prerequisites (one-time):
#   1. Rust (https://rustup.rs) with the default x86_64-pc-windows-msvc toolchain
#   2. Visual Studio 2022 Build Tools, "Desktop development with C++"
#   3. GStreamer 1.26+ **MSVC** 64-bit, BOTH the runtime and the development
#      installers from https://gstreamer.freedesktop.org/download/
#      (choose "Complete" in the installer so all plugins are present).
#      The MinGW builds do NOT link with the MSVC Rust target.
#
# usage:  powershell -ExecutionPolicy Bypass -File tools\win-build.ps1 [-Release]
param([switch]$Release)
$ErrorActionPreference = "Stop"

$gst = $env:GSTREAMER_1_0_ROOT_MSVC_X86_64
if (-not $gst) { $gst = "C:\Program Files\gstreamer\1.0\msvc_x86_64\" }
if (-not (Test-Path "$gst\lib\pkgconfig\gstreamer-1.0.pc")) {
  throw "GStreamer MSVC development files not found under $gst (set GSTREAMER_1_0_ROOT_MSVC_X86_64)"
}
$env:PKG_CONFIG_PATH = "$gst\lib\pkgconfig"
$env:PATH = "$gst\bin;" + $env:PATH

Set-Location (Join-Path $PSScriptRoot "..")
if ($Release) { cargo build --release -p restream-gui -p restream-cli -p restream-launcher } else { cargo build -p restream-gui -p restream-cli -p restream-launcher }
Write-Host ""
Write-Host "Built. To run from the tree the GStreamer bin dir must be on PATH (this shell has it)."
Write-Host "To make a self-contained folder:  tools\win-bundle.ps1"
