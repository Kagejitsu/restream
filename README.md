# Restream

Delayed multi-destination RTMP restreamer with simultaneous recording.
Windows desktop app (also runs on Linux for development).

```
capture ─ encode once ─┬─ record (undelayed, crash-safe MKV / MP4)
                       └─ delay ring (≥3 min) ─ fan-out ─ Twitch / YouTube / Facebook / … (identical bytes, lock-step)
```

- **Inputs**: Blackmagic Web Presenter (UVC), DeckLink, OBS Virtual Camera
  (DirectShow), webcams (Media Foundation / Kernel Streaming), SRT/RTMP from OBS.
- **Delay** in the encoded domain (cheap, exact), On-Air monitor of the delayed output.
- **Fan-out**: one encode, N RTMP/RTMPS destinations, per-destination
  reconnect and keyframe resync; a slow destination never affects the others.
- **GUI**: egui. **Core**: Rust + GStreamer. **RTMP**: own client over `rml_rtmp` (+ rustls for RTMPS).

## Layout

| crate | role |
| --- | --- |
| `crates/core` | GStreamer pipeline, delay ring, releaser + destination tasks, recording, monitor, engine state machine |
| `crates/rtmp` | FLV tag helpers, RTMP/RTMPS publishing client |
| `crates/gui` | desktop app |
| `crates/cli` | headless runner (dev, soak tests, servers) |
| `tests/` | `e2e.sh` (3 destinations, byte-identical check, delay check), `chaos.sh` (kill one destination's server) |
| `tools/` | Windows build/bundle scripts, Inno Setup installer |
| `docs/USER-GUIDE.md` | operator manual |

## Build (Linux, development)

GStreamer ≥ 1.20 with dev headers, plus plugins: good, bad, ugly (x264), libav, fdkaac.

```
cargo build
cargo test -p restream-core -p restream-rtmp      # unit + integration (needs x264enc, ffprobe)
tests/e2e.sh 10 40                                 # needs tools/bin/mediamtx (see tests/README.md)
tests/chaos.sh
./target/debug/restream                            # GUI
./target/debug/restream-cli run tests/e2e-profile.toml --for 30
```

## Build (Windows, release)

See `tools/win-build.ps1` (prerequisites in the header), then
`tools/win-bundle.ps1` for a self-contained `dist\`, then `tools/installer.iss`
with Inno Setup.

## Guarantees the tests prove

- All destinations receive byte-identical video and audio elementary streams (`tests/e2e.sh`).
- The burned-in clock on every output lags wall time by at least the configured delay.
- A destination whose server dies does not disturb the others and rejoins when it returns (`tests/chaos.sh`).
- Recording can be started/stopped mid-session, is finalised on drain, and remuxes to MP4 when asked (`crates/core/tests`).
