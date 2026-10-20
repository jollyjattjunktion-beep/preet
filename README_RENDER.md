# Preet Sports — Flat Render Update

All updated backend modules and Render/Docker configuration files are placed directly in this directory/ZIP root. No `src/` Python package is needed.

## Fix for `ModuleNotFoundError: No module named 'src'`

- `server.py` imports `commentator` from `human_commentator.py` in the same folder.
- `cricket_streamer.py` imports `commentator` from that same root-level file.
- `human_commentator.py` preserves the CREX source commentary text exactly and only chooses the delivery badge, TTS rate, and pitch. It does not call Gemini or rewrite the line.

## What to copy to GitHub

Copy these files to the root of your existing repository (not inside a new folder):

- `server.py`
- `cricket_streamer.py`
- `human_commentator.py`
- `Dockerfile`
- `requirements.txt`
- `render.yaml`
- `Procfile`
- `.dockerignore`
- `.gitignore`
- `.env.example`

Keep your existing webpage assets (`index.html`, `app.js`, `style.css`). The server supports either the root-level assets or the existing `web/` directory.

**To make literally every app file sit in one folder:** in your existing GitHub repository, move `web/index.html`, `web/app.js`, and `web/style.css` into the repository root beside `server.py`, then remove the now-empty `web/` directory. The updated server detects the root-level `index.html` automatically and serves `/static/app.js` and `/static/style.css` from that same root. These three UI files are not included in this ZIP because they were not among the files attached to this chat; move the versions already in your repository.

## Render settings

1. Render Dashboard → your service → **Settings**.
2. Runtime: **Docker**.
3. Dockerfile path: `./Dockerfile`.
4. Docker context: `.`.
5. Health Check Path: `/api/status`.
6. Environment variables: `PORT=10000`, `PYTHONUNBUFFERED=1`.
7. Do not commit `.env`. If using Sarvam, set a newly rotated `SARVAM_API_KEY` in Render Environment; Edge TTS does not need this key.
8. Push these root files to GitHub, then Render → **Manual Deploy → Deploy latest commit**.

## Verify before deployment

Run this in the repository root:

```bash
python -m py_compile server.py cricket_streamer.py human_commentator.py
python -c "from human_commentator import commentator; print(commentator.humanize('A short CREX source line', {'commentary':'A short CREX source line','runs':'4','over':'0.4'})['spoken_text'])"
```

The second command should print `A short CREX source line`, proving that the helper returns the source text without rewriting it.

## Audio behavior

- Web TTS audio is generated as in-memory MP3 bytes and sent to the browser; no permanent audio archive is written.
- At startup, the current/latest CREX delivery is synchronized and can be spoken; older history is not replayed.
- The speech queue holds one waiting event and replaces stale waiting deliveries with the newest ball.
- A completed over is summarized once when CREX has the delivery badges needed to calculate the recap.

Browser autoplay may require clicking/interacting with the webpage before the browser will play audio.

## Security

A `.env` containing a TTS secret was uploaded in this conversation. Rotate/revoke that secret and store its replacement only in Render's Environment settings. This ZIP does not include `.env`.
