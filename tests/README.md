# Tests

`e2e.sh` and `chaos.sh` need a local RTMP server. They use the portable
[mediamtx](https://github.com/bluenviron/mediamtx) binary at `tools/bin/mediamtx`
(gitignored):

```
mkdir -p tools/bin && cd tools/bin
V=$(curl -fsSL https://api.github.com/repos/bluenviron/mediamtx/releases/latest | grep -m1 tag_name | sed 's/.*"v\([^"]*\)".*/\1/')
curl -fsSL "https://github.com/bluenviron/mediamtx/releases/download/v$V/mediamtx_v${V}_linux_amd64.tar.gz" | tar xz mediamtx
```

- `e2e.sh [delay] [seconds]` — three destinations on one server, burn-in clock,
  recording. Asserts byte-identical elementary streams across destinations,
  full-length recording, no reconnects/lag, and leaves `tests/out/grab-*.png`
  (one frame per destination grabbed at the same instant) for a visual delay check.
- `chaos.sh [delay] [seconds]` — Facebook on a second server that is killed
  and restarted mid-session; asserts the survivors are untouched and the victim rejoins.
- `cargo test -p restream-core --test recording` — dynamic recording start/stop,
  MP4 remux, live preview + On-Air monitor frames.
