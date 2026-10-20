"""Preet Sports source-preserving commentary metadata.

This module deliberately does not generate or rewrite commentary. The spoken words
are taken from CREX's source commentary. The helper only classifies the event and
returns a badge, rate, and pitch for TTS delivery.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional


class HumanCommentator:
    """Compatibility helper for the broadcaster; source text always wins."""

    @staticmethod
    def _token(value: Any) -> str:
        return " ".join(str(value or "").split()).strip()

    @classmethod
    def classify(cls, text: str, runs: Any = "") -> Dict[str, Any]:
        source = cls._token(text)
        token = cls._token(runs).upper().replace(" ", "")
        lower = source.lower()
        over_kind = "ball"

        if token in {"W", "WKT", "OUT"} or re.search(r"\b(wicket|bowled|caught|lbw|run out|stumped|dismissed)\b", lower):
            event, badge, rate, pitch, emotion = "wicket", "⚡ WICKET", "+9%", "+4Hz", "high_energy"
        elif token == "6" or re.search(r"\b(six|sixer|maximum)\b", lower):
            event, badge, rate, pitch, emotion = "six", "🔥 SIX", "+8%", "+3Hz", "high_energy"
        elif token == "4" or re.search(r"\b(four|boundary|chauka)\b", lower):
            event, badge, rate, pitch, emotion = "four", "🎯 FOUR", "+6%", "+2Hz", "medium_energy"
        elif "WD" in token or "WIDE" in token:
            event, badge, rate, pitch, emotion = "wide", "WIDE", "+3%", "+1Hz", "normal"
        elif "NB" in token or "NOBALL" in token or "NO BALL" in token:
            event, badge, rate, pitch, emotion = "noball", "NO BALL", "+3%", "+1Hz", "normal"
        elif token in {"0", "DOT", "DOTBALL"} or re.search(r"\b(no run|dot ball)\b", lower):
            event, badge, rate, pitch, emotion = "dot", "🛡️ DOT BALL", "+1%", "+0Hz", "calm"
        elif token in {"1", "2", "3", "1B", "2B", "1LB", "2LB"}:
            event, badge, rate, pitch, emotion = "single", "🏏 RUNS", "+3%", "+1Hz", "normal"
        else:
            event, badge, rate, pitch, emotion = "ball", "🏏 BALL", "+4%", "+1Hz", "normal"

        return {"event": event, "badge": badge, "rate": rate, "pitch": pitch, "emotion": emotion}

    def humanize(
        self,
        raw_text: str,
        ball_info: Optional[Dict[str, Any]] = None,
        lang: str = "hinglish",
        gemini_api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Keep CREX source words unchanged; return metadata only for delivery."""
        ball_info = ball_info or {}
        source_text = self._token(ball_info.get("commentary") or raw_text)
        token = ball_info.get("runs", "")
        meta = self.classify(source_text, token)
        over_raw = self._token(ball_info.get("over"))
        match = re.search(r"(\d+)\.(\d+)", over_raw)
        ball_no = int(match.group(2)) if match else None
        over_no = int(match.group(1)) if match else None
        token_upper = self._token(token).upper()
        legal_end = ball_no == 6 and not any(x in token_upper for x in ("WD", "WIDE", "NB", "NOBALL", "NO BALL"))
        is_over_start = ball_no == 1
        if legal_end and meta["event"] not in {"wicket", "six", "four"}:
            meta.update({"event": "over_end", "badge": "🏁 OVER COMPLETE", "rate": "+5%", "pitch": "+1Hz"})
        bowler = self._token(ball_info.get("bowler"))
        batter = self._token(ball_info.get("striker") or ball_info.get("batter"))
        matchup = self._token(ball_info.get("matchup"))
        if " to " in matchup:
            left, right = matchup.split(" to ", 1)
            bowler = bowler or left.strip()
            batter = batter or right.strip()
        return {
            "spoken_text": source_text,
            "should_speak": bool(source_text),
            "event": meta["event"],
            "badge": meta["badge"],
            "rate": meta["rate"],
            "pitch": meta["pitch"],
            "emotion": meta["emotion"],
            "bowler": bowler,
            "batter": batter,
            "is_over_start": is_over_start,
            "is_over_end": legal_end,
            "completed_overs": (over_no + 1) if legal_end and over_no is not None else over_no,
        }

    async def humanize_async(self, raw_text: str, ball_info: Optional[Dict[str, Any]] = None,
                             lang: str = "hinglish", gemini_api_key: Optional[str] = None) -> Dict[str, Any]:
        """Async compatibility wrapper; no network or AI rewrite is performed."""
        return self.humanize(raw_text, ball_info, lang, gemini_api_key)


commentator = HumanCommentator()
