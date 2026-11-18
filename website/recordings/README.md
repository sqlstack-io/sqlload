# CLI recordings

Terminal recordings of `sqlload` for the README, rendered with
[VHS](https://github.com/charmbracelet/vhs) from the `.tape` scripts in `tapes/`. The tapes and
scripts are the source; the rendered GIF, MP4 and PNG files go to `out/`, which is not committed.

| Demo | Shows |
|------|-------|
| `plan-load` | In the playground project, `sqlload plan`, then `sqlload load`, then a load that stops on a misspelled `cursor_column`. |

The README uses `plan-load`, copied to `.github/demos/plan-load.gif`. It runs locally on DuckDB,
with the project `sqlload playground` writes.

## Requirements

- `vhs`, `ttyd` and `ffmpeg` on `PATH`, plus a Chromium or Chrome that VHS can find (it downloads
  one if none is on `PATH`).
- DejaVu Sans Mono, the font the tapes use.
- `sqlload` on `PATH`, or `SQLLOAD_BIN` pointing at a directory that contains the `sqlload` to
  record. Record with the latest release, so the output matches what users see.

## Record

```bash
cd website/recordings
./record.sh plan-load     # out/plan-load.gif, out/plan-load.mp4 and out/plan-load.png
cp out/plan-load.gif ../../.github/demos/
```

`record.sh` runs `prepare.sh <demo>` to build a clean project under `scratch/<demo>/`, then renders
`tapes/<demo>.tape`. Shared terminal settings and the colour theme, which matches the website's
code blocks, are in `tapes/common.tape`.

Look at every frame before publishing a recording: output such as timings and memory changes
between runs.
