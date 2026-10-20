"""Web Server & Real-Time Broadcast Hub for Cricket AI Broadcaster.

Provides modern Web UI, WebSocket live feed, and in-browser audio streaming.
Voice audio is synthesized and streamed straight to the browser in memory —
never written to disk.
"""
import os
import json
import base64
import asyncio
import logging
import re
from pathlib import Path
from typing import Dict, Any, Set, Optional

from dotenv import load_dotenv

load_dotenv()

import aiohttp
from aiohttp import web

# Import existing core modules
from cricket_streamer import CrexParser, create_tts, get_live_matches, resolve_match_url
from human_commentator import commentator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("WebServer")

BASE_DIR = Path(__file__).resolve().parent
# Flat layout is preferred: index.html/app.js/style.css can live beside server.py.
# Backward compatibility: if UI assets remain in the existing web/ directory, use it.
WEB_DIR = BASE_DIR if (BASE_DIR / "index.html").is_file() else (BASE_DIR / "web")


# ---------------- Current-ball-only speech helpers ----------------
def _normal_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def _ball_over_index(ball: Dict[str, Any]) -> Optional[int]:
    """Read a CREX delivery label such as 33.6 and return its zero-based over."""
    label = _normal_space(ball.get("over"))
    match = re.search(r"(\d+)(?:\.(\d+))?", label)
    return int(match.group(1)) if match else None


def _ball_order(ball: Dict[str, Any], original_index: int = 0) -> tuple[int, int]:
    """Numeric order for over.ball labels; ties prefer earlier DOM entries (usually newest)."""
    label = _normal_space(ball.get("over"))
    match = re.search(r"(\d+)(?:\.(\d+))?", label)
    if not match:
        return (-1, -original_index)
    over = int(match.group(1))
    delivery = int(match.group(2) or 0)
    return (over * 6 + delivery, -original_index)


def _latest_source_ball(balls: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pick one newest source delivery, never turn a page of historic balls into a voice queue."""
    if not balls:
        return None
    return max(enumerate(balls), key=lambda pair: _ball_order(pair[1], pair[0]))[1]


def _ball_identity(ball: Dict[str, Any]) -> str:
    """Stable delivery identity that doesn't change if CREX refreshes the commentary wording."""
    over = _normal_space(ball.get("over")).lower()
    matchup = _normal_space(ball.get("matchup")).lower()
    runs = _normal_space(ball.get("runs")).upper()
    if not over or over.lower() == "live":
        text = _normal_space(ball.get("commentary"))[:100].lower()
        return f"{over}|{matchup}|{runs}|{text}"
    return f"{over}|{matchup}|{runs}"


def _delivery_stats(token: Any) -> tuple[int, bool, int, int, int]:
    """Return (runs, legal_ball, fours, sixes, wickets) from the source's delivery badge."""
    raw = _normal_space(token).lower().replace(" ", "")
    nums = re.findall(r"\d+", raw)
    n = int(nums[0]) if nums else 1
    if "wd" in raw or "wide" in raw:
        return (max(1, n), False, 0, 0, 0)
    if "nb" in raw or "noball" in raw:
        return (max(1, n), False, 0, 0, 0)
    if raw in {"w", "wkt", "out"} or "wicket" in raw:
        return (0, True, 0, 0, 1)
    if "lb" in raw or raw.endswith("b"):
        return (max(1, n), True, 0, 0, 0)
    if raw.isdigit():
        runs = int(raw)
        return (runs, True, int(runs == 4), int(runs == 6), 0)
    return (0, True, 0, 0, 0)


def _over_announcement(over_index: int, balls: list[Dict[str, Any]], lang: str) -> Optional[str]:
    """Create a deterministic completed-over recap from CREX's source delivery badges."""
    rows = [b for b in balls if _ball_over_index(b) == over_index]
    # Dedupe repeated DOM wrappers before computing stats.
    unique: dict[str, Dict[str, Any]] = {}
    for b in rows:
        unique.setdefault(_ball_identity(b), b)
    rows = list(unique.values())
    if not rows:
        return None

    runs = fours = sixes = wickets = legal = 0
    for ball in rows:
        n, is_legal, f, s, w = _delivery_stats(ball.get("runs"))
        runs += n
        legal += int(is_legal)
        fours += f
        sixes += s
        wickets += w

    spoken_over = over_index + 1
    if lang in ("hi", "hinglish"):
        return (f"ओवर {spoken_over} पूरा हुआ। इस ओवर में {runs} रन, "
                f"{fours} चौके, {sixes} छक्के और {wickets} विकेट।")
    four_word = "four" if fours == 1 else "fours"
    six_word = "six" if sixes == 1 else "sixes"
    wicket_word = "wicket" if wickets == 1 else "wickets"
    return (f"Over {spoken_over} complete. {runs} runs, {fours} {four_word}, "
            f"{sixes} {six_word} and {wickets} {wicket_word}.")


def _source_badge(ball: Dict[str, Any]) -> str:
    token = _normal_space(ball.get("runs")).upper().replace(" ", "")
    if token in {"W", "WKT", "OUT"} or "WICKET" in token:
        return "WICKET"
    if token == "6":
        return "SIX"
    if token == "4":
        return "FOUR"
    if "WD" in token or "WIDE" in token:
        return "WIDE"
    if "NB" in token or "NOBALL" in token:
        return "NO BALL"
    return "BALL"


class BroadcastHub:
    def __init__(self):
        self.active_websockets: Set[web.WebSocketResponse] = set()
        self.is_running = False
        self.broadcast_task: Optional[asyncio.Task] = None
        self.youtube_engine = None
        self.youtube_task: Optional[asyncio.Task] = None
        self.latest_match_state: Dict[str, Any] = {}
        self.latest_commentary = ""

    async def broadcast_ws(self, msg_type: str, data: Any):
        """Pushes real-time JSON payload to all connected browser clients."""
        payload = json.dumps({"type": msg_type, "data": data})
        stale = []
        for ws in self.active_websockets:
            try:
                await ws.send_str(payload)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.active_websockets.discard(ws)

    async def start_broadcast(self, url: str, lang: str = "hi", stream_key: str = "",
                               tts_provider: str = "edge", tts_api_key: str = "", tts_speaker: str = ""):
        if self.is_running:
            return {"ok": False, "error": "Broadcast is already running"}

        url = url.strip()
        if not url:
            return {"ok": False, "error": "Match link or search query is required."}

        # Smart Match Resolution: Accepts CREX (any tab/year), Cricbuzz, Cricinfo, or plain text
        resolved_url, resolve_note = resolve_match_url(url)
        target_url = resolved_url if resolved_url else url
        source_lang = "hi" if lang in ("hi", "hinglish") else "en"

        # Pre-flight check: request CREX's actual source-language commentary.
        parser = CrexParser(target_url, lang=source_lang)
        try:
            html_text = await parser.fetch_html_async()
            if not html_text:
                return {"ok": False, "error": "Unable to fetch content from match URL."}
            init_data = parser.parse(html_text)
            self.latest_match_state = init_data
        except ValueError as ve:
            # If direct target fails or returned 404, attempt fuzzy match fallback using active fixtures
            fallback_url, fallback_note = resolve_match_url(url, force_fuzzy=True)
            if fallback_note and fallback_url != target_url:
                logger.info(f"Direct link returned 404 ({ve}), auto-resolved to live match: {fallback_url}")
                target_url = fallback_url
                resolve_note = fallback_note
                try:
                    parser = CrexParser(target_url, lang=source_lang)
                    html_text = await parser.fetch_html_async()
                    init_data = parser.parse(html_text)
                    self.latest_match_state = init_data
                except Exception as fe:
                    return {"ok": False, "error": f"Match link failed: {fe}"}
            else:
                return {"ok": False, "error": str(ve)}
        except Exception as e:
            logger.warning(f"Pre-flight connection error: {e}")
            return {"ok": False, "error": f"Failed to connect to match URL: {e}"}

        self.is_running = True
        self.broadcast_task = asyncio.create_task(self._run_broadcast_pipeline(
            target_url, lang, stream_key, parser=parser,
            tts_provider=tts_provider, tts_api_key=tts_api_key, tts_speaker=tts_speaker
        ))

        # Launch YouTube Live RTMP stream if stream_key is provided
        if stream_key.strip():
            try:
                from cricket_streamer import CricketStreamingEngine
                self.youtube_engine = CricketStreamingEngine(
                    url=target_url,
                    mode="youtube",
                    stream_key=stream_key.strip(),
                    language=lang,
                    tts_provider=tts_provider,
                    tts_api_key=tts_api_key or None,
                    tts_speaker=tts_speaker or None
                )
                self.youtube_task = asyncio.create_task(self.youtube_engine.run())
                logger.info(f"YouTube Live RTMP broadcast engine launched for stream key: {stream_key[:4]}****")
            except Exception as ye:
                logger.error(f"Failed to start YouTube streaming engine: {ye}")

        await self.broadcast_ws("status", {
            "is_running": True,
            "has_youtube": bool(stream_key.strip()),
            "target": "YouTube Live & Web" if stream_key.strip() else "Web Browser"
        })
        if resolve_note:
            await self.broadcast_ws("match_resolved", {
                "original": url,
                "resolved_url": target_url,
                "note": resolve_note
            })
        if self.latest_match_state:
            await self.broadcast_ws("match_state", self.latest_match_state)
        return {
            "ok": True,
            "resolved_url": target_url,
            "note": resolve_note,
            "youtube_streaming": bool(stream_key.strip())
        }

    async def stop_broadcast(self):
        if not self.is_running:
            return {"ok": False, "error": "Broadcast is not running"}

        self.is_running = False

        # Stop YouTube Live engine if active
        if self.youtube_engine:
            self.youtube_engine.is_running = False
        if self.youtube_task and not self.youtube_task.done():
            self.youtube_task.cancel()
            try:
                await self.youtube_task
            except asyncio.CancelledError:
                pass
        self.youtube_task = None
        self.youtube_engine = None

        # Stop Web broadcast pipeline
        if self.broadcast_task and not self.broadcast_task.done():
            self.broadcast_task.cancel()
            try:
                await self.broadcast_task
            except asyncio.CancelledError:
                pass
        self.broadcast_task = None
        await self.broadcast_ws("status", {"is_running": False})
        return {"ok": True}

    async def _run_broadcast_pipeline(self, url: str, lang: str, stream_key: str, parser: Optional[CrexParser] = None,
                                       tts_provider: str = "edge", tts_api_key: str = "", tts_speaker: str = ""):
        logger.info(
            "Starting current-ball-only broadcast for URL: %s (Lang: %s, Voice: %s)",
            url, lang.upper(), tts_provider
        )
        if parser is None:
            parser = CrexParser(url, lang=lang)
        tts = create_tts(language=lang, provider=tts_provider,
                         api_key=tts_api_key or None, speaker=tts_speaker or None)

        # At most one waiting event. New source deliveries replace any not-yet-played event.
        ball_queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        seen_feed_keys: set[str] = set()
        last_latest_key: Optional[str] = None
        last_latest_ball: Optional[Dict[str, Any]] = None
        last_latest_state: Dict[str, Any] = {}
        latest_event_version = 0
        pending_over_announcements: Dict[str, str] = {}
        announced_over_ids: set[str] = set()

        async def queue_latest(event: Dict[str, Any]) -> None:
            if ball_queue.full():
                try:
                    dropped = ball_queue.get_nowait()
                    logger.info("Discarding queued stale ball %s; prioritizing %s",
                                _normal_space(dropped.get("ball", {}).get("over")),
                                _normal_space(event.get("ball", {}).get("over")))
                except asyncio.QueueEmpty:
                    pass
            ball_queue.put_nowait(event)

        async def broadcast_audio(text: str, badge: str, rate: Optional[str],
                                  pitch: Optional[str], kind: str) -> Optional[float]:
            """Synthesize to bytes and broadcast a data URI; no audio files are written."""
            mp3_data = await tts.speak_mp3(text, rate=rate, pitch=pitch)
            if not mp3_data:
                logger.warning("TTS returned empty audio for event type %s", kind)
                return None
            duration = max(1.2, len(mp3_data) / 16000.0)
            audio_url = "data:audio/mpeg;base64," + base64.b64encode(mp3_data).decode("ascii")
            self.latest_commentary = text
            await self.broadcast_ws("commentary", {
                "text": text,
                "badge": badge,
                "audio_url": audio_url,
                "duration": round(duration, 1),
                "kind": kind,
            })
            logger.info("Broadcast in-memory audio (%s, approx %.1fs): %s", kind, duration, text)
            return duration

        async def voice_worker():
            """Speak pending over summaries and the single newest delivery, never a ball backlog."""
            event: Optional[Dict[str, Any]] = None
            while self.is_running:
                if event is None:
                    try:
                        event = await asyncio.wait_for(ball_queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    except asyncio.CancelledError:
                        break

                current_event, event = event, None
                try:
                    # A completed-over announcement is sticky until spoken, even if the next
                    # ball replaces the waiting queue item. This prevents summaries vanishing.
                    for summary in current_event.get("over_announcements", []):
                        over_id = summary["id"]
                        if over_id in announced_over_ids:
                            continue
                        duration = await broadcast_audio(
                            summary["text"], "OVER COMPLETE", "+5%", "+0Hz", "over_summary"
                        )
                        if duration is not None:
                            announced_over_ids.add(over_id)
                            pending_over_announcements.pop(over_id, None)
                            # Allow the browser player to finish the summary before the next ball.
                            await asyncio.sleep(duration)

                    # New source delivery arrived while a summary/TTS call was running.
                    # Drop this old ball; the next queue item carries the newest one.
                    if current_event["version"] != latest_event_version:
                        logger.info("Dropping stale synthesized delivery %s", current_event.get("ball", {}).get("over"))
                        continue

                    ball = current_event["ball"]
                    ball_over = _normal_space(ball.get("over")) or "Live"
                    source_text = _normal_space(ball.get("commentary"))
                    if not source_text:
                        logger.info("Skipping delivery %s because CREX has no source commentary text yet", ball_over)
                        continue

                    # Use the commentator only to classify the delivery for badge/rate/pitch.
                    # Crucially, its rewritten `spoken_text` is NOT used: audio is the source
                    # CREX commentary text verbatim (apart from whitespace normalization).
                    metadata = commentator.humanize(
                        source_text,
                        ball_info={**current_event["fresh_data"], **ball,
                                   "runs": ball.get("runs", ""), "over": ball_over},
                        lang=lang
                    )
                    metadata = metadata or {}
                    badge = metadata.get("badge") or _source_badge(ball)
                    rate = metadata.get("rate") or "+8%"
                    pitch = metadata.get("pitch") or "+0Hz"
                    matchup = _normal_space(ball.get("matchup"))

                    # The poll loop already published this source line to the visual feed.

                    mp3_data = await tts.speak_mp3(source_text, rate=rate, pitch=pitch)
                    if not mp3_data:
                        logger.warning("TTS returned empty audio for ball %s", ball_over)
                        continue
                    if current_event["version"] != latest_event_version:
                        logger.info("TTS completed for old ball %s; discarding audio before broadcast", ball_over)
                        continue

                    duration = max(1.2, len(mp3_data) / 16000.0)
                    audio_url = "data:audio/mpeg;base64," + base64.b64encode(mp3_data).decode("ascii")
                    self.latest_commentary = source_text
                    await self.broadcast_ws("commentary", {
                        "text": source_text,
                        "badge": badge,
                        "audio_url": audio_url,
                        "duration": round(duration, 1),
                        "kind": "ball",
                    })
                    logger.info("On-air current ball %s: %s", ball_over, source_text)

                    # Wait for either the approximate clip duration or a new delivery. If a new
                    # delivery arrives first, immediately prioritize it; the browser replaces
                    # the currently playing clip rather than building an audio backlog.
                    try:
                        event = await asyncio.wait_for(ball_queue.get(), timeout=duration)
                    except asyncio.TimeoutError:
                        event = None

                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Voice worker error")
                    event = None

        voice_task = asyncio.create_task(voice_worker(), name="current_ball_voice_worker")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        }
        connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=60)
        consecutive_errors = 0

        try:
            logger.info("Starting CREX poll loop (1-second interval); speech backlog is disabled")
            async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
                while self.is_running:
                    try:
                        html_text = await parser.fetch_html_async(session=session)
                        if not html_text:
                            await asyncio.sleep(1.0)
                            continue

                        fresh_data = parser.parse(html_text)
                        self.latest_match_state = fresh_data
                        await self.broadcast_ws("match_state", fresh_data)

                        raw_balls = fresh_data.get("balls") or []
                        latest_ball = _latest_source_ball(raw_balls)

                        # Keep the visual ball-by-ball feed useful even when speech skips stale
                        # balls. On startup, don't replay the match's full historical feed.
                        if latest_ball and last_latest_key is None:
                            latest_key = _ball_identity(latest_ball)
                            for old_ball in raw_balls:
                                old_key = _ball_identity(old_ball)
                                if old_key != latest_key:
                                    seen_feed_keys.add(old_key)
                            if latest_key not in seen_feed_keys:
                                seen_feed_keys.add(latest_key)
                                await self.broadcast_ws("feed_item", {
                                    "over": _normal_space(latest_ball.get("over")),
                                    "runs": _normal_space(latest_ball.get("runs")),
                                    "matchup": _normal_space(latest_ball.get("matchup")),
                                    "badge": _source_badge(latest_ball),
                                    "commentary": _normal_space(latest_ball.get("commentary")),
                                })

                            last_latest_key = latest_key
                            last_latest_ball = latest_ball
                            last_latest_state = fresh_data
                            latest_event_version += 1
                            await queue_latest({
                                "version": latest_event_version,
                                "ball": latest_ball,
                                "fresh_data": fresh_data,
                                "over_announcements": [],
                            })

                        elif latest_ball:
                            latest_key = _ball_identity(latest_ball)

                            # Add any truly new feed lines in chronological order, but do not
                            # enqueue those old lines for speech. The voice queue gets only the
                            # single latest delivery.
                            new_feed = []
                            for candidate in raw_balls:
                                candidate_key = _ball_identity(candidate)
                                if candidate_key not in seen_feed_keys:
                                    seen_feed_keys.add(candidate_key)
                                    new_feed.append(candidate)
                            for candidate in sorted(new_feed, key=lambda b: _ball_order(b)):
                                await self.broadcast_ws("feed_item", {
                                    "over": _normal_space(candidate.get("over")),
                                    "runs": _normal_space(candidate.get("runs")),
                                    "matchup": _normal_space(candidate.get("matchup")),
                                    "badge": _source_badge(candidate),
                                    "commentary": _normal_space(candidate.get("commentary")),
                                })

                            if latest_key != last_latest_key:
                                old_over = _ball_over_index(last_latest_ball or {}) if last_latest_ball else None
                                new_over = _ball_over_index(latest_ball)
                                if old_over is not None and new_over is not None and new_over > old_over:
                                    over_id = f"{last_latest_state.get('batting_team', '')}:{old_over}"
                                    if over_id not in announced_over_ids and over_id not in pending_over_announcements:
                                        summary_text = _over_announcement(old_over, raw_balls, lang)
                                        if summary_text:
                                            pending_over_announcements[over_id] = summary_text
                                            logger.info("Queued completed-over announcement for over %s", old_over + 1)

                                # Ignore a stale/backwards page snapshot rather than speaking it.
                                if (_ball_order(latest_ball) >= _ball_order(last_latest_ball or {})):
                                    last_latest_key = latest_key
                                    last_latest_ball = latest_ball
                                    latest_event_version += 1
                                    pending_summaries = [
                                        {"id": over_id, "text": summary}
                                        for over_id, summary in pending_over_announcements.items()
                                    ]
                                    await queue_latest({
                                        "version": latest_event_version,
                                        "ball": latest_ball,
                                        "fresh_data": fresh_data,
                                        "over_announcements": pending_summaries,
                                    })
                                else:
                                    logger.info("Ignoring backwards CREX delivery snapshot: %s", latest_ball.get("over"))

                            last_latest_state = fresh_data

                        consecutive_errors = 0
                    except asyncio.CancelledError:
                        raise
                    except ValueError as exc:
                        consecutive_errors += 1
                        logger.warning("Parse/fetch error (%d): %s", consecutive_errors, exc)
                        if consecutive_errors >= 3:
                            await self.broadcast_ws("error", {"message": str(exc)})
                    except Exception as exc:
                        consecutive_errors += 1
                        logger.warning("CREX poll error (%d): %s", consecutive_errors, exc, exc_info=True)
                        if consecutive_errors >= 5:
                            await self.broadcast_ws("error", {"message": f"Connection issue with match feed: {exc}"})
                            consecutive_errors = 0

                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logger.info("Broadcast pipeline cancelled by user")
            raise
        except Exception as exc:
            logger.error("Broadcast pipeline error: %s", exc, exc_info=True)
            await self.broadcast_ws("error", {"message": f"Pipeline error: {exc}"})
        finally:
            self.is_running = False
            if not voice_task.done():
                voice_task.cancel()
                try:
                    await voice_task
                except asyncio.CancelledError:
                    pass
            await self.broadcast_ws("status", {"is_running": False})


hub = BroadcastHub()

# Web Route Handlers
async def handle_index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(WEB_DIR / "index.html")

async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=30.0, autoping=True)
    await ws.prepare(request)
    hub.active_websockets.add(ws)

    # Send initial status
    await ws.send_str(json.dumps({
        "type": "status",
        "data": {"is_running": hub.is_running}
    }))
    if hub.latest_match_state:
        await ws.send_str(json.dumps({
            "type": "match_state",
            "data": hub.latest_match_state
        }))

    try:
        async for msg in ws:
            pass
    finally:
        hub.active_websockets.discard(ws)

    return ws

async def handle_api_status(request: web.Request) -> web.Response:
    return web.json_response({
        "is_running": hub.is_running,
        "match_state": hub.latest_match_state,
        "latest_commentary": hub.latest_commentary
    })

async def handle_api_live_matches(request: web.Request) -> web.Response:
    matches = await asyncio.to_thread(get_live_matches)
    return web.json_response({"ok": True, "matches": matches})

async def handle_api_resolve(request: web.Request) -> web.Response:
    try:
        body = await request.json()
        query = body.get("query", "").strip()
        if not query:
            return web.json_response({"ok": False, "error": "Query is required"}, status=400)
        resolved_url, note = resolve_match_url(query)
        return web.json_response({"ok": True, "resolved_url": resolved_url, "note": note})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def handle_api_start(request: web.Request) -> web.Response:
    try:
        body = await request.json()
        url = body.get("url", "").strip()
        lang = body.get("lang", "hi")
        stream_key = body.get("stream_key", "")
        tts_provider = body.get("tts_provider", "edge").strip() or "edge"
        tts_api_key = body.get("sarvam_api_key", "").strip() or os.getenv("SARVAM_API_KEY", "").strip()
        tts_speaker = body.get("sarvam_speaker", "").strip()

        if not url:
            return web.json_response({"ok": False, "error": "Match URL or team name is required"}, status=400)

        if tts_provider == "sarvam" and not tts_api_key:
            return web.json_response({"ok": False, "error": "Sarvam API key is required to use the Sarvam voice engine"}, status=400)

        res = await hub.start_broadcast(url=url, lang=lang, stream_key=stream_key,
                                         tts_provider=tts_provider, tts_api_key=tts_api_key, tts_speaker=tts_speaker)
        status_code = 200 if res.get("ok") else 400
        return web.json_response(res, status=status_code)
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def handle_api_stop(request: web.Request) -> web.Response:
    res = await hub.stop_broadcast()
    return web.json_response(res)

async def handle_favicon(request: web.Request) -> web.Response:
    return web.Response(status=204)

def create_app() -> web.Application:
    app = web.Application()

    # Routes
    app.router.add_get("/", handle_index)
    app.router.add_get("/favicon.ico", handle_favicon)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/api/status", handle_api_status)
    app.router.add_get("/api/live-matches", handle_api_live_matches)
    app.router.add_post("/api/resolve", handle_api_resolve)
    app.router.add_post("/api/start", handle_api_start)
    app.router.add_post("/api/stop", handle_api_stop)

    # Static assets
    app.router.add_static("/static/", path=WEB_DIR, name="static")

    return app

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000" if os.getenv("RENDER") else "8088"))
    host = os.getenv("HOST", "0.0.0.0")
    app = create_app()
    logger.info(f"Cricket AI Web UI running on http://{host}:{port}")
    web.run_app(app, host=host, port=port)
