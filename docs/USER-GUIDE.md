# Restream — user guide

Restream takes one video+audio input, encodes it **once**, holds it in a
stream-delay buffer, and sends the *identical* stream to every destination
(Twitch, YouTube, Facebook, anything RTMP/RTMPS) at the same moment, while
recording the undelayed program to disk.

## The one-minute version

1. **Destinations** tab: add your platforms. Pick a preset, paste the stream
   key (it is stored in Windows Credential Manager, not in the profile file).
2. **Input** tab: choose the video source and the audio source.
3. **Delay** tab: set the delay (league minimum 3 min = 180 s).
4. **Recording** tab: choose a folder; tick "record automatically".
5. **Save** the profile, then **GO LIVE**.

The top bar shows `FILLING DELAY … on air in m:ss`, then `ON AIR`. Destinations
connect about three seconds before the first frame goes out.

## Buttons

| Button | What it does |
| --- | --- |
| GO LIVE | Starts capture, encoding, recording and the delay buffer. |
| STOP (drain) | Stops the input now. Viewers still receive the buffered minutes, then the streams end cleanly. Normal end of a show. |
| PANIC / CUT | Throws away everything in the buffer and disconnects immediately. Viewers never see the last minutes. Asks for confirmation. |
| REC | Start/stop a recording during a session (independent of streaming). |
| On-Air monitor | Decodes the delayed output so you see exactly what viewers see. Costs one video decoder; off by default. |

## Inputs

| Source | Notes |
| --- | --- |
| Blackmagic Web Presenter (USB-C) | Appears as a webcam: choose **Media Foundation** and pick it. Set the capture format to what the Web Presenter outputs (e.g. 1920×1080 @ 60). Its audio comes in as a USB audio device: choose **WASAPI** and pick it. |
| DeckLink | **DeckLink** video (mode `auto` or e.g. `1080p5994`, connection SDI/HDMI) and **DeckLink** audio with the same device number. |
| OBS Virtual Camera | **DirectShow**, device name `OBS Virtual Camera`. **It carries no audio.** For audio pick a WASAPI device: a virtual cable (VB-Cable) that OBS monitors to, or the output device OBS plays to. Output devices (speakers, headphones, a cable's playback side) are listed with *(loopback: what it plays)* and are captured in loopback mode automatically; a capture device (microphone, line-in, the Web Presenter's USB audio) is used directly. |
| Webcams / other capture cards | **Media Foundation** (preferred) or **Kernel Streaming**. |
| OBS over the network (recommended for OBS) | **Network**: `srt://:9000?mode=listener`. In OBS: Settings → Stream → Service *Custom*, Server `srt://<this PC's IP>:9000?mode=caller`, key empty. One link carries video **and** audio in sync. Set audio to **From video source**. |

**Audio offset** (Input tab) shifts audio relative to video in milliseconds and
can be changed live. Positive = audio later.

**Program gain** (Input tab, and the *Gain* box in the status bar) adjusts the
audio level in dB before the encoder, so every stream, the recording and the
meters follow it. Live.

**Audio monitor** plays the program audio (after gain, undelayed) on an output
device you choose in the Input tab: headphones, a second interface, a virtual
cable. The *Monitor* tick box and its volume in the status bar work live; the
monitor volume never affects the stream.

**TEST ONLY: burn a timecode clock** overlays a running clock on the picture,
visible to every viewer and in recordings. Use it in a rehearsal to check that
all platforms show the same time and that the delay is what you expect, then
turn it off. While it is on, the running-specs line says "CLOCK BURN-IN ON".

## Encoder

While a session runs, the line under the top bar ("RUNNING WITH: …") shows
exactly what is on air: resolution, frame rate, bitrate, the encoder in use
(friendly name and element), audio codec, delay, destinations and whether
recording is on. It turns orange when the configured encoder failed and
another one was substituted; *auto* then remembers the working encoder and
tries it first next time ("forget" in the Encoder tab resets that).

- *auto* picks the best hardware encoder present (NVIDIA NVENC, AMD AMF, Intel
  QSV, Media Foundation) and falls back to x264 on the CPU.
- Keyframe interval 2 s is what Twitch/YouTube/Facebook want. Keep it.
- Bitrate: every destination gets the same stream, so stay within the strictest
  platform (Twitch: 6000 kbps, 8000 for partners). While on air the **Bitrate**
  box in the top bar changes the running encoder immediately; the Encoder tab
  value is the starting point for the next session.
- Preset: x264 `veryfast`…`slow`; NVENC `p4`…`p7`; AMF `quality`; QSV/VA `1`–`7`.

## Delay

Everything after the encoder is small (a 3-minute 8 Mbps buffer is ~180 MB), so
long delays are cheap. The *RAM cap* is a safety limit; if it is too small for
the delay the status bar shows **OVERFLOW** and the oldest data is dropped.

All destinations and the On-Air monitor read the same buffer, so they cannot
drift apart. If one platform's connection cannot keep up, that destination is
marked **LAGGING**, skips to the next keyframe and rejoins the common timeline;
the others are not affected. If a connection drops it reconnects with backoff
(1 s → 30 s) and resumes at the live cursor.

## Recording

Recording is the live, undelayed program using the same encode. MKV is
crash-safe (playable even if the PC dies mid-show). Choose MP4 to have the file
remuxed to MP4 when the session ends (no re-encode). *Split every N minutes*
rolls files on keyframes.

## Profiles

Profiles live in `%APPDATA%\Restream\profiles\*.toml`; stream keys are in
Windows Credential Manager under `restream/<profile>/<destination>`.
Logs: `%LOCALAPPDATA%\Restream\logs\`. The Logs tab's **Copy diagnostics**
puts the profile (keys blanked) and the recent log on the clipboard for support.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| "no usable video encoder" | GStreamer plugins missing: run `restream-cli encoders` from the install folder. |
| Destination stays *reconnecting* | Wrong server/key, or the platform's ingest rejects the stream (bitrate/keyframe). Hover the destination for the error. RTMPS needs the port in the URL for Facebook (`:443`). |
| ON AIR but the platform shows nothing | The platform is still buffering; most show the stream 10–30 s after the first frame arrives. |
| Audio out of sync | Set *Audio offset*. Negative if audio is late. |
| Camera or microphone "Could not open" / "Failed to start" with nothing else wrong | Windows privacy: Settings → Privacy & security → Camera (and Microphone) → turn on access and "Let desktop apps access your camera/microphone". Also make sure no other app (Windows Hello, a browser tab, OBS itself for a webcam) is holding the device. |
| OBS Virtual Camera has no audio | By design. See Inputs. |

## Command line (optional)

`restream-cli run profile.toml [--delay S] [--for S]` runs a profile headless with
a live status readout; `restream-cli devices` and `restream-cli encoders` list
what the machine offers. Keys in CLI profiles may be `${ENV_VAR}` references.
