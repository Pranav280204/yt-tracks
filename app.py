# app.py — YouTube tracker + auth (login, single-device, admin add-user)
import os
import threading
import logging
import time
import math
import secrets
import bisect
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Tuple
from functools import wraps

import pandas as pd
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, session, g, make_response
)
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import psycopg
from psycopg.rows import dict_row
from werkzeug.security import generate_password_hash, check_password_hash

# -----------------------------
# App & logging
# -----------------------------
# -----------------------------
# App & logging
# -----------------------------
app = Flask(__name__)

# Use ONE secret key — fixed & persistent
app.secret_key = os.getenv(
    "FLASK_SECRET_KEY",
    "xW3p7hU2q9L0zA4nD8rK1vS6bC5yT0j"  # fallback used only in local dev
)

app.permanent_session_lifetime = timedelta(days=30)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("yt-tracker")

IST = ZoneInfo("Asia/Kolkata")


# -----------------------------
# Env & cache TTL
# -----------------------------
API_KEY = os.getenv("YOUTUBE_API_KEY", "")
POSTGRES_URL = os.getenv("DATABASE_URL")
ADMIN_CREATE_SECRET = os.getenv("ADMIN_CREATE_SECRET", "")  # master password for creating users
def require_admin_secret_from_form(form, redirect_endpoint, **redirect_kwargs):
    """
    Check admin_secret in form against ADMIN_CREATE_SECRET.
    If wrong/missing -> flash + redirect.
    If ok or secret not set -> return None.
    """
    if not ADMIN_CREATE_SECRET:
        # If you haven't set it, don't block (or change this to always block if you prefer).
        return None

    admin_secret = (form.get("admin_secret") or "").strip()
    if admin_secret != ADMIN_CREATE_SECRET:
        flash("Admin password required or incorrect.", "danger")
        return redirect(url_for(redirect_endpoint, **redirect_kwargs))

    return None

CHANNEL_CACHE_TTL = int(os.getenv("CHANNEL_CACHE_TTL", "50"))  # seconds; < sampler interval
if not POSTGRES_URL:
    log.warning("DATABASE_URL not set.")

# YouTube client
YOUTUBE = None
if API_KEY:
    try:
        YOUTUBE = build("youtube", "v3", developerKey=API_KEY, cache_discovery=False)
        log.info("YouTube client ready.")
    except Exception as e:
        log.exception("YouTube init failed: %s", e)

# simple in-memory cache for channel totals (to avoid bursts)
_channel_views_cache: Dict[str, Tuple[Optional[int], float]] = {}
_channel_cache_lock = threading.Lock()

def get_channel_total_cached(channel_id: str) -> Optional[int]:
    """Return channel viewCount using cached value if recent; otherwise fetch and update cache."""
    if not channel_id or not YOUTUBE:
        return None
    now_ts = time.time()
    with _channel_cache_lock:
        ent = _channel_views_cache.get(channel_id)
        if ent:
            val, fetched_at = ent
            if now_ts - fetched_at <= CHANNEL_CACHE_TTL:
                return val
    # fetch fresh
    try:
        resp = YOUTUBE.channels().list(part="statistics", id=channel_id, maxResults=1).execute()
        items = resp.get("items", [])
        if not items:
            val = None
        else:
            val = int(items[0].get("statistics", {}).get("viewCount", 0))
    except Exception as e:
        log.exception("Error fetching channel stats for %s: %s", channel_id, e)
        val = None
    with _channel_cache_lock:
        _channel_views_cache[channel_id] = (val, now_ts)
    return val
def get_latest_channel_totals_for(channel_ids: list[str]) -> dict:
    """
    Return dict channel_id -> latest total_views (or None).
    Uses one DB query (DISTINCT ON) which is fast with proper PK/index.
    """
    out: dict = {}
    if not channel_ids:
        return out
    conn = db()
    with conn.cursor() as cur:
        cur.execute("""
          SELECT DISTINCT ON (channel_id) channel_id, total_views
          FROM channel_stats
          WHERE channel_id = ANY(%s)
          ORDER BY channel_id, ts_utc DESC
        """, (channel_ids,))
        for r in cur.fetchall():
            out[r["channel_id"]] = r["total_views"]
    # ensure keys present
    for ch in channel_ids:
        out.setdefault(ch, None)
    return out

def get_latest_sample_per_video(video_ids: list[str]) -> dict:
    """
    Return dict video_id -> row {video_id, ts_utc, views} for the latest ts_utc per video.
    Uses DISTINCT ON(video_id) ORDER BY video_id, ts_utc DESC.
    """
    out: dict = {}
    if not video_ids:
        return out
    conn = db()
    with conn.cursor() as cur:
        cur.execute("""
          SELECT DISTINCT ON (video_id) video_id, ts_utc, views
          FROM views
          WHERE video_id = ANY(%s)
          ORDER BY video_id, ts_utc DESC
        """, (video_ids,))
        for r in cur.fetchall():
            out[r["video_id"]] = r
    # ensure all ids exist
    for vid in video_ids:
        out.setdefault(vid, None)
    return out
# -----------------------------
# DB connection (psycopg3)
# -----------------------------
_db = None
_db_lock = threading.Lock()

def db():
    global _db
    with _db_lock:
        if _db is None or _db.closed:
            _db = psycopg.connect(
                POSTGRES_URL,
                autocommit=True,
                row_factory=dict_row,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
        return _db

def init_db():
    conn = db()
    with conn.cursor() as cur:
        # Users for auth ----------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY,
          username TEXT UNIQUE NOT NULL,
          password_hash TEXT NOT NULL,
          current_session_token TEXT
        );
        """)

        cur.execute("""
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
        """)

        cur.execute("""
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS device_token TEXT;
        """)

        cur.execute("""
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS device_ua TEXT;
        """)

        cur.execute("""
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS device_info TEXT;
        """)

        # Videos ----------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS video_list (
          video_id    TEXT PRIMARY KEY,
          name        TEXT NOT NULL,
          is_tracking BOOLEAN NOT NULL DEFAULT TRUE
        );
        """)

        # ➕ NEW COLUMNS FOR COMPARISON FEATURE
        cur.execute("""
        ALTER TABLE video_list
        ADD COLUMN IF NOT EXISTS compare_video_id TEXT;
        """)

        cur.execute("""
        ALTER TABLE video_list
        ADD COLUMN IF NOT EXISTS compare_offset_days INTEGER;
        """)

        # View samples ----------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS views (
          video_id  TEXT NOT NULL,
          ts_utc    TIMESTAMPTZ NOT NULL,
          date_ist  DATE NOT NULL,
          views     BIGINT NOT NULL,
          likes     BIGINT,
          PRIMARY KEY (video_id, ts_utc),
          FOREIGN KEY (video_id) REFERENCES video_list(video_id) ON DELETE CASCADE
        );
        """)

        # Targets ----------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS targets (
          id           SERIAL PRIMARY KEY,
          video_id     TEXT NOT NULL REFERENCES video_list(video_id) ON DELETE CASCADE,
          target_views BIGINT NOT NULL,
          target_ts    TIMESTAMPTZ NOT NULL,
          note         TEXT
        );
        """)

        # Channel snapshots ----------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS channel_stats (
          channel_id TEXT NOT NULL,
          ts_utc TIMESTAMPTZ NOT NULL,
          total_views BIGINT,
          PRIMARY KEY (channel_id, ts_utc)
        );
        """)

    log.info("DB schema ready.")




# -----------------------------
# Auth helpers
# -----------------------------
def get_current_user():
    uid = session.get("user_id")
    token = session.get("session_token")
    if not uid or not token:
        return None

    conn = db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, username, current_session_token, is_active "
            "FROM users WHERE id=%s",
            (uid,)
        )
        u = cur.fetchone()

    # If user doesn’t exist, token changed, or user is deactivated → logout immediately
    if (
        not u
        or not u["current_session_token"]
        or u["current_session_token"] != token
        or not u["is_active"]       # 👈 NEW: deactivated user triggers logout
    ):
        session.clear()
        return None

    return u


@app.before_request
def load_user():
    # runs every request; store user in g
    g.user = get_current_user()

@app.context_processor
def inject_user():
    # make current_user available in templates
    return {"current_user": getattr(g, "user", None)}

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not g.get("user"):
            flash("Please log in.", "warning")
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapper

# -----------------------------
# YouTube helpers
# -----------------------------
def extract_video_id(link: str) -> str | None:
    try:
        u = urlparse(link.strip())
        host = (u.netloc or "").lower()
        if "youtu.be" in host:
            return u.path.strip("/").split("/")[0] or None
        if "youtube.com" in host or "m.youtube.com" in host:
            if u.path == "/watch":
                qs = parse_qs(u.query)
                return (qs.get("v") or [None])[0]
            parts = u.path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] in {"shorts", "live"}:
                return parts[1]
        return None
    except Exception:
        return None

def fetch_title(video_id: str) -> str:
    if not YOUTUBE:
        return "Unknown"
    try:
        r = YOUTUBE.videos().list(part="snippet", id=video_id, maxResults=1).execute()
        items = r.get("items", [])
        return (items[0]["snippet"]["title"] if items else "Unknown")[:140] or "Unknown"
    except Exception as e:
        log.exception("Title fetch error: %s", e)
        return "Unknown"

# Add near the other globals
_channel_id_cache: Dict[str, Tuple[Optional[str], float]] = {}
_CHANNEL_ID_CACHE_TTL = 60.0  # seconds
# in-memory short cache for built video display (to reduce repeated heavy work)
_video_display_cache: Dict[str, Tuple[dict, float]] = {}
_video_display_cache_lock = threading.Lock()
_VIDEO_DISPLAY_CACHE_TTL = 15.0  # seconds; tune this
# maximum number of days of samples to fetch for display (reduce to speed up)
_MAX_DISPLAY_DAYS = 14

def fetch_channel_id_for_videos(video_ids: list[str]) -> dict:
    """
    Bulk fetch snippet.channelId for up to 50 video_ids.
    Uses short in-memory cache to avoid repeated API calls during bursts.
    Returns map: video_id -> channel_id (may be None).
    """
    out: dict = {}
    if not video_ids:
        return out

    # first consult cache for each id
    now_ts = time.time()
    to_fetch = []
    for vid in video_ids:
        ent = _channel_id_cache.get(vid)
        if ent:
            val, fetched_at = ent
            if now_ts - fetched_at <= _CHANNEL_ID_CACHE_TTL:
                out[vid] = val
                continue
        # not cached or stale
        to_fetch.append(vid)

    # if no API available or nothing to fetch, return what we have
    if (not YOUTUBE) or (not to_fetch):
        # ensure all requested keys exist in out
        for vid in video_ids:
            out.setdefault(vid, None)
        return out

    # fetch in chunks of 50
    try:
        for i in range(0, len(to_fetch), 50):
            chunk = to_fetch[i:i+50]
            r = YOUTUBE.videos().list(part="snippet", id=",".join(chunk), maxResults=50).execute()
            for it in r.get("items", []):
                vid = it["id"]
                ch = it.get("snippet", {}).get("channelId")
                out[vid] = ch
                _channel_id_cache[vid] = (ch, now_ts)
            # mark any missing items as None and cache them
            for ch_vid in chunk:
                if ch_vid not in out:
                    out[ch_vid] = None
                    _channel_id_cache[ch_vid] = (None, now_ts)
    except Exception as e:
        log.exception("fetch_channel_id_for_videos error: %s", e)
        # fallback: ensure all requested keys present
        for vid in video_ids:
            out.setdefault(vid, None)

    # ensure all original video_ids exist in output
    for vid in video_ids:
        out.setdefault(vid, None)
    return out


def fetch_stats_batch(video_ids: list[str]) -> dict:
    if not YOUTUBE or not video_ids:
        return {}
    out = {}
    try:
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i + 50]
            r = YOUTUBE.videos().list(part="statistics", id=",".join(chunk), maxResults=50).execute()
            for it in r.get("items", []):
                vid = it["id"]
                st = it.get("statistics", {})
                views = int(st.get("viewCount", "0") or 0)
                like_raw = st.get("likeCount")
                likes = int(like_raw) if like_raw is not None else None
                out[vid] = {"views": views, "likes": likes}
    except HttpError as e:
        log.error("YouTube (stats) error: %s", e)
    except Exception as e:
        log.exception("Stats fetch error: %s", e)
    return out

# -----------------------------
# Storage helpers
# -----------------------------
def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)

def safe_store(video_id: str, stats: dict):
    tsu = now_utc()
    date_ist = tsu.astimezone(IST).date()
    conn = db()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM views WHERE video_id=%s AND ts_utc=%s", (video_id, tsu))
        cur.execute(
            "INSERT INTO views (video_id, ts_utc, date_ist, views, likes) VALUES (%s, %s, %s, %s, %s)",
            (video_id, tsu, date_ist, int(stats.get("views", 0)), stats.get("likes"))
        )

def interpolate_at(rows: list[dict], target_ts: datetime, key="views") -> Optional[float]:
    """
    Interpolate numeric value at target_ts from chronological rows (with ts_utc and key).
    Returns None if target is before first or after last row.
    """
    if not rows:
        return None
    # exact
    for r in rows:
        if r["ts_utc"] == target_ts:
            return float(r[key] if key in r else r["views"])
    prev = None
    for r in rows:
        if r["ts_utc"] < target_ts:
            prev = r
            continue
        if r["ts_utc"] > target_ts and prev is not None:
            t0 = prev["ts_utc"].timestamp()
            v0 = float(prev[key] if key in prev else prev["views"])
            t1 = r["ts_utc"].timestamp()
            v1 = float(r[key] if key in r else r["views"])
            if t1 == t0:
                return v1
            frac = (target_ts.timestamp() - t0) / (t1 - t0)
            return v0 + frac * (v1 - v0)
        if prev is None:
            return None
    return None

def _time_to_seconds(time_str: str) -> int:
    h, m, s = [int(x) for x in time_str.split(":")]
    return h * 3600 + m * 60 + s


def find_closest_tpl(prev_map: dict, time_part: str, tolerance_seconds: int = 10):
    if not prev_map:
        return None
    target_secs = _time_to_seconds(time_part)
    best = None
    best_delta = None
    for k, tpl in prev_map.items():
        try:
            s = _time_to_seconds(k)
        except Exception:
            continue
        delta = abs(target_secs - s)
        if best is None or delta < best_delta:
            best = tpl
            best_delta = delta
    if best is not None and best_delta is not None and best_delta <= tolerance_seconds:
        return best
    return None

def find_closest_prev(prev_map: dict, time_part: str, max_earlier_seconds: int = 300):
    """
    From prev_map (time_str -> tpl), return the tuple whose time is the closest
    earlier-or-equal to time_part, but only if it is within max_earlier_seconds
    earlier. If exact match exists, returns it. If none found, returns None.

    Example: time_part="22:30:00", will prefer "22:30:00" if present,
    otherwise "22:25:00" (or "22:26/22:27/...") if within max_earlier_seconds.
    """
    if not prev_map:
        return None
    try:
        target_secs = _time_to_seconds(time_part)
    except Exception:
        return None

    # exact
    if time_part in prev_map:
        return prev_map[time_part]

    best = None
    best_delta = None
    for k, tpl in prev_map.items():
        try:
            s = _time_to_seconds(k)
        except Exception:
            continue
        # only consider earlier-or-equal times
        if s > target_secs:
            continue
        delta = target_secs - s
        if delta <= max_earlier_seconds:
            if best is None or delta < best_delta:
                best = tpl
                best_delta = delta
    return best

def process_gains(rows_asc: list[dict]):
    """
    rows_asc: chronological list of dicts with keys ts_utc (aware datetime) and views (int)

    Returns list of tuples:
      (ts_ist_str, views, gain_5min, hourly_gain, gain_24h, hourly_pct_change)

    Implementation uses bisect on an array of timestamps for O(n log n) behavior.
    """
    out = []
    n = len(rows_asc)
    if n == 0:
        return out

    # prepare fast arrays
    ts_list = [r["ts_utc"].timestamp() for r in rows_asc]  # floats seconds
    views_list = [int(r["views"]) for r in rows_asc]

    for i, r in enumerate(rows_asc):
        ts_utc = r["ts_utc"]
        ts_ist = ts_utc.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
        views = views_list[i]

        # gain in last 5 minutes (previous sample)
        gain_5min = None if i == 0 else views - views_list[i - 1]

        # ---------- hourly gain: compute value at (ts - 1 hour) using interpolation when possible ----------
        target_h_ts = (ts_utc - timedelta(hours=1)).timestamp()
        hourly = None

        # search within rows[0..i] for insertion point of target_h_ts
        pos = bisect.bisect_right(ts_list, target_h_ts, 0, i + 1)
        prev_idx = pos - 1
        next_idx = pos if pos <= i else None

        if prev_idx >= 0 and next_idx is not None:
            # interpolation possible between prev_idx and next_idx
            t0 = ts_list[prev_idx]
            v0 = views_list[prev_idx]
            t1 = ts_list[next_idx]
            v1 = views_list[next_idx]
            if t1 == t0:
                ref_val = v1
            else:
                frac = (target_h_ts - t0) / (t1 - t0)
                ref_val = v0 + frac * (v1 - v0)
            try:
                hourly = views - int(round(ref_val))
            except Exception:
                hourly = None
        elif prev_idx >= 0:
            # only previous available (no later row in window), use prev row value
            try:
                hourly = views - views_list[prev_idx]
            except Exception:
                hourly = None
        else:
            hourly = None

        # ---------- 24h gain (similar) ----------
        target_d_ts = (ts_utc - timedelta(days=1)).timestamp()
        posd = bisect.bisect_right(ts_list, target_d_ts, 0, i + 1)
        prev_idx_d = posd - 1
        next_idx_d = posd if posd <= i else None

        if prev_idx_d >= 0 and next_idx_d is not None:
            t0 = ts_list[prev_idx_d]
            v0 = views_list[prev_idx_d]
            t1 = ts_list[next_idx_d]
            v1 = views_list[next_idx_d]
            if t1 == t0:
                ref_day = v1
            else:
                frac = (target_d_ts - t0) / (t1 - t0)
                ref_day = v0 + frac * (v1 - v0)
            try:
                gain_24h = views - int(round(ref_day))
            except Exception:
                gain_24h = None
        elif prev_idx_d >= 0:
            gain_24h = views - views_list[prev_idx_d]
        else:
            gain_24h = None

        # ---------- hourly percent change vs previous row's hourly ----------
        hourly_pct_change = None
        if i > 0:
            # compute previous row's hourly using same logic but bounded to prev index
            prev_idx_row = i - 1
            prev_target_h_ts = (rows_asc[prev_idx_row]["ts_utc"] - timedelta(hours=1)).timestamp()
            pos_prev = bisect.bisect_right(ts_list, prev_target_h_ts, 0, prev_idx_row + 1)
            prev_prev_idx = pos_prev - 1
            prev_next_idx = pos_prev if pos_prev <= prev_idx_row else None

            if prev_prev_idx >= 0 and prev_next_idx is not None:
                t0 = ts_list[prev_prev_idx]
                v0 = views_list[prev_prev_idx]
                t1 = ts_list[prev_next_idx]
                v1 = views_list[prev_next_idx]
                if t1 == t0:
                    ref_prev = v1
                else:
                    frac = (prev_target_h_ts - t0) / (t1 - t0)
                    ref_prev = v0 + frac * (v1 - v0)
                try:
                    prev_hourly = views_list[prev_idx_row] - int(round(ref_prev))
                except Exception:
                    prev_hourly = None
            elif prev_prev_idx >= 0:
                prev_hourly = views_list[prev_idx_row] - views_list[prev_prev_idx]
            else:
                prev_hourly = None

            if hourly is not None and prev_hourly not in (None, 0):
                try:
                    hourly_pct_change = round(((hourly - prev_hourly) / prev_hourly) * 100, 2)
                except Exception:
                    hourly_pct_change = None

        out.append((ts_ist, views, gain_5min, hourly, gain_24h, hourly_pct_change))

    return out


# -----------------------------
# Background sampler + channel snapshots
# -----------------------------
_sampler_started = False
_sampler_lock = threading.Lock()

def sleep_until_next_5min_IST():
    now_ist = datetime.now(IST).replace(microsecond=0)
    next_min = ((now_ist.minute // 5) + 1) * 5
    if next_min >= 60:
        next_tick = now_ist.replace(minute=0, second=0) + timedelta(hours=1)
    else:
        next_tick = now_ist.replace(minute=next_min, second=0)
    time.sleep(max(1, (next_tick - now_ist).total_seconds()))

def sampler_loop():
    """
    Every 5-min tick:
      - fetch stats for tracked videos and store into views
      - fetch channel ids for those videos, get channel totals and insert into channel_stats
    """
    log.info("Sampler loop started (aligned to IST 5-min).")
    while True:
        try:
            sleep_until_next_5min_IST()
            tsu = now_utc()
            conn = db()
            with conn.cursor() as cur:
                cur.execute("SELECT video_id FROM video_list WHERE is_tracking=TRUE ORDER BY video_id")
                vids = [r["video_id"] for r in cur.fetchall()]

            if not vids:
                continue

            stats_map = fetch_stats_batch(vids)
            # store per-video stats
            for vid in vids:
                st = stats_map.get(vid)
                if st:
                    safe_store(vid, st)

            # channel snapshots
            if YOUTUBE:
                ch_map = fetch_channel_id_for_videos(vids)
                unique_chs = {ch for ch in ch_map.values() if ch}
                ch_totals = {}
                for ch in unique_chs:
                    try:
                        total = get_channel_total_cached(ch)
                        ch_totals[ch] = total
                    except Exception:
                        ch_totals[ch] = None
                with conn.cursor() as cur:
                    for ch, total in ch_totals.items():
                        try:
                            cur.execute(
                                "INSERT INTO channel_stats (channel_id, ts_utc, total_views) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                                (ch, tsu, total)
                            )
                        except Exception:
                            log.exception("Insert channel_stats failed for %s", ch)
        except Exception as e:
            log.exception("Sampler error: %s", e)
            time.sleep(5)

def start_background():
    global _sampler_started
    with _sampler_lock:
        if not _sampler_started:
            t = threading.Thread(target=sampler_loop, daemon=True, name="yt-sampler")
            t.start()
            _sampler_started = True
            log.info("Background sampler started.")

# -----------------------------
# Helper: build display data for one video
# -----------------------------
# -----------------------------
# Helper: build display data for one video
# -----------------------------
def build_video_display(vid: str):
    """
    Optimized builder:
     - fetches only recent rows (bounded window)
     - uses process_gains (bisect-based)
     - caches results for a short TTL
     - fetches comparison video rows only for the same window
    """
    nowu = now_utc()

    # try cache
    with _video_display_cache_lock:
        ent = _video_display_cache.get(vid)
        if ent:
            cached_obj, fetched_at = ent
            if time.time() - fetched_at <= _VIDEO_DISPLAY_CACHE_TTL:
                return cached_obj

    conn = db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT video_id, name, is_tracking, compare_video_id, compare_offset_days "
            "FROM video_list WHERE video_id=%s",
            (vid,)
        )
        vrow = cur.fetchone()
        if not vrow:
            return None

        compare_video_id = vrow.get("compare_video_id")
        compare_offset_days = vrow.get("compare_offset_days")
        try:
            if compare_offset_days is not None:
                compare_offset_days = int(compare_offset_days)
        except Exception:
            compare_offset_days = None

        # compute earliest timestamp to fetch
        # +2 days margin to allow interpolation for 24h lookups
        start_utc = (nowu - timedelta(days=_MAX_DISPLAY_DAYS + 2))

        # Fetch raw view rows in window (chronological)
        cur.execute(
            "SELECT ts_utc, views FROM views WHERE video_id=%s AND ts_utc >= %s ORDER BY ts_utc ASC",
            (vid, start_utc)
        )
        all_rows = cur.fetchall()

        # If comparison configured, fetch comp rows in same window (so date alignments work)
        comp_rows = None
        if compare_video_id:
            cur.execute(
                "SELECT ts_utc, views FROM views WHERE video_id=%s AND ts_utc >= %s ORDER BY ts_utc ASC",
                (compare_video_id, start_utc)
            )
            comp_rows = cur.fetchall()

    # prepare outputs / defaults
    daily = {}
    latest_views = None
    latest_ts = None
    latest_ts_iso = None
    latest_ts_ist = None
    compare_meta = None

    if all_rows:
        # process gains efficiently
        processed_all = process_gains(all_rows)

        # group by IST date and build per-day time maps
        grouped = {}
        date_time_map = {}
        for tpl in processed_all:
            ts_ist = tpl[0]  # "YYYY-MM-DD HH:MM:SS"
            date_str, time_part = ts_ist.split(" ")
            grouped.setdefault(date_str, []).append(tpl)
            date_time_map.setdefault(date_str, {})[time_part] = tpl

        # build comp map if available
        comp_date_map = {}
        if comp_rows:
            comp_processed = process_gains(comp_rows)
            for ctpl in comp_processed:
                c_ts = ctpl[0]
                c_date, c_time = c_ts.split(" ")
                comp_date_map.setdefault(c_date, {})[c_time] = ctpl

        # iterate dates newest-first
        dates_sorted = sorted(grouped.keys(), reverse=True)
        for date_str in dates_sorted:
            processed = grouped[date_str]
            prev_day = (datetime.fromisoformat(date_str).date() - timedelta(days=1)).isoformat()
            prev_map = date_time_map.get(prev_day, {})

            display_rows = []
            for tpl in processed:
                # tpl: ts_ist, views, gain5, hourly, gain_24h, hourly_pct
                ts_ist, views, gain_5m, hourly_gain, gain_24h, hourly_pct_unused = tpl
                time_part = ts_ist.split(" ")[1]

                # --- change 24h vs prev day (tolerant match) ---
                prev_tpl = prev_map.get(time_part) or find_closest_tpl(prev_map, time_part, tolerance_seconds=10)
                prev_gain24 = prev_tpl[4] if prev_tpl else None
                pct24 = None
                if prev_gain24 not in (None, 0):
                    try:
                        pct24 = round(((gain_24h or 0) - prev_gain24) / prev_gain24 * 100, 2)
                    except Exception:
                        pct24 = None

                # --- projected (based on yesterday ~22:30) ---
                projected = None
                ref_2230 = find_closest_prev(prev_map, "22:30:00", max_earlier_seconds=300)
                if ref_2230 and pct24 not in (None,):
                    base_views = ref_2230[1]
                    base_gain = ref_2230[4]
                    if base_views is not None and base_gain not in (None, 0):
                        try:
                            projected = int(base_views + base_gain * (1 + pct24 / 100.0))
                        except Exception:
                            projected = None

                # --- comparison diff (if configured) ---
                comp_diff = None
                if compare_video_id and compare_offset_days is not None and comp_date_map:
                    main_date = datetime.fromisoformat(date_str).date()
                    comp_date = (main_date - timedelta(days=compare_offset_days)).isoformat()
                    comp_time_map = comp_date_map.get(comp_date, {})
                    comp_match = find_closest_prev(comp_time_map, time_part, max_earlier_seconds=300)
                    if comp_match:
                        comp_views = comp_match[1]
                        try:
                            comp_diff = views - comp_views
                        except Exception:
                            comp_diff = None

                display_rows.append((ts_ist, views, gain_5m, hourly_gain, gain_24h, pct24, projected, comp_diff))

            # newest-first for UI
            daily[date_str] = list(reversed(display_rows))

        latest_views = all_rows[-1]["views"]
        latest_ts = all_rows[-1]["ts_utc"]
        latest_ts_iso = latest_ts.isoformat() if latest_ts is not None else None
        latest_ts_ist = latest_ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if latest_ts is not None else None

    # channel info (same logic but only for this video)
    channel_info = {
        "channel_id": None,
        "channel_total": None,
        "channel_prev_total": None,
        "channel_gain_since_prev": None,
        "channel_gain_24h": None
    }
    if YOUTUBE:
        ch = fetch_channel_id_for_videos([vid]).get(vid)
        if ch:
            channel_info["channel_id"] = ch
            with conn.cursor() as cur:
                cur.execute("SELECT ts_utc, total_views FROM channel_stats WHERE channel_id=%s ORDER BY ts_utc ASC", (ch,))
                rows = cur.fetchall()
            if rows:
                channel_info["channel_total"] = rows[-1]["total_views"]
                if len(rows) > 1:
                    channel_info["channel_prev_total"] = rows[-2]["total_views"]
                    if channel_info["channel_prev_total"] is not None and channel_info["channel_total"] is not None:
                        channel_info["channel_gain_since_prev"] = channel_info["channel_total"] - channel_info["channel_prev_total"]
                target_24 = rows[-1]["ts_utc"] - timedelta(days=1)
                interp = interpolate_at(rows, target_24, key="total_views")
                ref_24 = int(round(interp)) if interp is not None else None
                if ref_24 is not None and channel_info["channel_total"] is not None:
                    channel_info["channel_gain_24h"] = channel_info["channel_total"] - ref_24

    # targets
    with conn.cursor() as cur:
        cur.execute("SELECT id, target_views, target_ts, note FROM targets WHERE video_id=%s ORDER BY target_ts ASC", (vid,))
        target_rows = cur.fetchall()
    targets_display = []
    for t in target_rows:
        tid = t["id"]; t_views = t["target_views"]; t_ts = t["target_ts"]; note = t["note"]
        remaining_views = t_views - (latest_views or 0)
        remaining_seconds = (t_ts - nowu).total_seconds()
        if remaining_views <= 0:
            status = "reached"; req_hr = req_5m = 0
        elif remaining_seconds <= 0:
            status = "overdue"; req_hr = math.ceil(remaining_views); req_5m = math.ceil(req_hr / 12)
        else:
            status = "active"; hrs = max(remaining_seconds / 3600.0, 1 / 3600); req_hr = math.ceil(remaining_views / hrs); req_5m = math.ceil(req_hr / 12)
        targets_display.append({
            "id": tid,
            "target_views": t_views,
            "target_ts_ist": t_ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S"),
            "note": note,
            "status": status,
            "required_per_hour": req_hr,
            "required_per_5min": req_5m,
            "remaining_views": remaining_views,
            "remaining_seconds": int(remaining_seconds)
        })

    if compare_video_id and compare_offset_days is not None:
        compare_meta = {"compare_video_id": compare_video_id, "offset_days": compare_offset_days}
    else:
        compare_meta = None

    result = {
        "video_id": vrow["video_id"],
        "name": vrow["name"],
        "is_tracking": bool(vrow["is_tracking"]),
        "daily": daily,
        "targets": targets_display,
        "latest_views": latest_views,
        "latest_ts": latest_ts,
        "latest_ts_iso": latest_ts_iso,
        "latest_ts_ist": latest_ts_ist,
        "channel_info": channel_info,
        "compare_meta": compare_meta
    }

    # cache it
    with _video_display_cache_lock:
        _video_display_cache[vid] = (result, time.time())

    return result
# -----------------------------
# Auth routes
# -----------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    # if already logged in, send to home
    if g.get("user"):
        return redirect(url_for("home"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        next_url = request.args.get("next") or url_for("home")

        if not username or not password:
            flash("Enter username and password.", "warning")
            return redirect(url_for("login", next=next_url))

        conn = db()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, password_hash, current_session_token, "
                "is_active, device_token, device_ua, device_info "
                "FROM users WHERE username=%s",
                (username,)
            )
            user = cur.fetchone()

        # invalid username/password
        if not user or not check_password_hash(user["password_hash"], password):
            flash("Invalid credentials.", "danger")
            return redirect(url_for("login", next=next_url))

        # deactivated user
        if not user["is_active"]:
            flash("Your account is deactivated. Contact admin at 1944pranav@gmail.com.", "danger")
            return redirect(url_for("login", next=next_url))

        # ---------- Device lock: mix of cookie token + fingerprint ----------
        ua_now = request.headers.get("User-Agent", "") or ""
        cookie_device = request.cookies.get("device_token")
        stored_device = user.get("device_token")
        stored_ua = (user.get("device_ua") or "")

        # simple fingerprint now: just user-agent (can extend later)
        fingerprint_now = ua_now.strip()
        stored_fingerprint = (user.get("device_info") or "").strip()

        def same_device():
            # 1) Strong match: cookie token matches DB
            if stored_device and cookie_device and cookie_device == stored_device:
                return True
            # 2) Fallback: UA / fingerprint match (handles cookie cleared on same device)
            if stored_fingerprint and fingerprint_now and stored_fingerprint == fingerprint_now:
                return True
            # 3) If no device stored yet, we will bind below, so not "same"
            return False

        if stored_device is None:
            # First login: bind current device
            new_device_token = secrets.token_hex(32)
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET device_token=%s, device_ua=%s, device_info=%s WHERE id=%s",
                    (new_device_token, ua_now, fingerprint_now, user["id"])
                )
            stored_device = new_device_token
        else:
            # Device already bound: ensure this is the same device
            if not same_device():
                flash("Login only allowed from your registered device. Contact admin at 1944pranav@gmail.com to reset.", "danger")
                return redirect(url_for("login", next=next_url))

        # ---------- Session token: latest login wins (on this device) ----------
        session_token = secrets.token_hex(32)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET current_session_token=%s WHERE id=%s",
                (session_token, user["id"])
            )

        # 👇 IMPORTANT: make session persistent
        session.clear()
        session.permanent = True          # <--- add this line
        session["user_id"] = user["id"]
        session["session_token"] = session_token

        # build response so we can set device cookie
        resp = make_response(redirect(next_url))
        # keep device cookie valid ~1 year
        if stored_device:
            resp.set_cookie("device_token", stored_device, max_age=365*24*3600, httponly=True, samesite="Lax")
        return resp

    # GET
    return render_template("login.html")



# -----------------------------
# Stats helpers & routes
# -----------------------------
from zoneinfo import ZoneInfo as _ZoneInfo
EST = _ZoneInfo("America/New_York")

def format_millions(n: Optional[int]) -> str:
    if n is None:
        return "—"
    return f"{n/1_000_000:.2f}M"

def format_count(n: Optional[int]) -> str:
    if n is None:
        return "—"
    # if >=1000 show K for compactness, but keep comma if <100k
    if abs(n) >= 1000:
        # round to nearest thousand for UI compactness (e.g. 150000 -> 150k)
        return f"{int(round(n/1000.0)):,}k"
    return f"{n:,}"

def fetch_daily_gains(video_id: str, days: int = 90):
    """
    Returns list of dicts ordered newest-first:
      [{"est_date": "2025-11-28", "daily_gain": 1234567, "end_views": 56048445}, ...]
    daily_gain computed as MAX(views) - MIN(views) within that EST calendar day.
    """
    conn = db()
    out = []
    with conn.cursor() as cur:
        # aggregate per EST date using Postgres timezone conversion
        cur.execute("""
            SELECT
              ( (ts_utc AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York' )::date AS est_date,
              MAX(views) AS max_v,
              MIN(views) AS min_v
            FROM views
            WHERE video_id = %s
            GROUP BY est_date
            ORDER BY est_date DESC
            LIMIT %s
        """, (video_id, days))
        rows = cur.fetchall()
    for r in rows:
        est_date = r["est_date"].isoformat()
        max_v = r["max_v"]
        min_v = r["min_v"]
        daily_gain = None
        if max_v is not None and min_v is not None:
            try:
                daily_gain = int(max_v - min_v)
            except Exception:
                daily_gain = None
        out.append({"est_date": est_date, "daily_gain": daily_gain, "end_views": int(max_v) if max_v is not None else None})
    return out

def fetch_hourly_for_ist_date(video_id: str, date_ist):
    """
    date_ist: datetime.date in IST timezone to analyze.
    Returns list of 24 dicts for hours 0..23:
      [{"hour": 0, "label": "00:00-01:00", "end_views": 12345, "hour_gain": 2345}, ...]
    Uses interpolation at end of each hour (IST) when needed.
    """
    # build day start/end in IST then convert to UTC for DB fetch
    start_ist = datetime(date_ist.year, date_ist.month, date_ist.day, 0, 0, 0, tzinfo=IST)
    end_ist = start_ist + timedelta(days=1)
    start_utc = start_ist.astimezone(timezone.utc)
    end_utc = end_ist.astimezone(timezone.utc)

    conn = db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ts_utc, views FROM views WHERE video_id=%s AND ts_utc >= %s AND ts_utc < %s ORDER BY ts_utc ASC",
            (video_id, start_utc, end_utc)
        )
        rows = cur.fetchall()

    # convert rows into list for interpolate_at
    rows_asc = [{"ts_utc": r["ts_utc"], "views": int(r["views"])} for r in rows]

    results = []
    prev_end_views = None
    for h in range(24):
        end_hour_ist = start_ist + timedelta(hours=h+1)
        end_hour_utc = end_hour_ist.astimezone(timezone.utc)

        # try interpolation first
        interp = interpolate_at(rows_asc, end_hour_utc, key="views")
        if interp is not None:
            try:
                end_views = int(round(interp))
            except Exception:
                end_views = None
        else:
            # fallback: latest sample <= end_hour_utc
            end_views = None
            for r in reversed(rows_asc):
                if r["ts_utc"] <= end_hour_utc:
                    end_views = int(r["views"])
                    break

        hour_gain = None
        if end_views is not None and prev_end_views is not None:
            try:
                hour_gain = int(end_views - prev_end_views)
            except Exception:
                hour_gain = None

        # set prev_end_views for next iteration
        if end_views is not None:
            prev_end_views = end_views

        label = f"{(h):02d}:00–{(h+1):02d}:00"
        results.append({
            "hour": h,
            "label": label,
            "end_views": end_views,
            "hour_gain": hour_gain
        })

    return results

# Route: stats page
@app.get("/video/<video_id>/stats")
@login_required
def video_stats(video_id):
    # pick IST date from query param ?date=YYYY-MM-DD else default latest IST date available
    sel_date_str = request.args.get("date")
    # get latest IST date available for this video (from DB)
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ts_utc) AS latest_ts FROM views WHERE video_id=%s", (video_id,))
        r = cur.fetchone()
        if not r or not r["latest_ts"]:
            flash("No data available for this video.", "warning")
            return redirect(url_for("video_detail", video_id=video_id))
        latest_ts = r["latest_ts"]
        latest_ist_date = latest_ts.astimezone(IST).date()

    if sel_date_str:
        try:
            sel_date = datetime.fromisoformat(sel_date_str).date()
        except Exception:
            sel_date = latest_ist_date
    else:
        sel_date = latest_ist_date

    # fetch daily list (EST) and hourly for selected IST date
    daily = fetch_daily_gains(video_id, days=180)   # keep recent 180 days
    hourly = fetch_hourly_for_ist_date(video_id, sel_date)

    # build list of dates (IST) available for selector from DB quickly:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ((ts_utc AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata')::date AS ist_date
            FROM views WHERE video_id=%s ORDER BY ist_date DESC
        """, (video_id,))
        date_rows = cur.fetchall()
    ist_dates = [r["ist_date"].isoformat() for r in date_rows]

    # pass to template with helpful formatted labels
    return render_template(
        "video_stats.html",
        video_id=video_id,
        daily=daily,
        hourly=hourly,
        selected_date=sel_date.isoformat(),
        ist_dates=ist_dates
    )

@app.get("/logout")
@login_required
def logout():
    conn = db()
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET current_session_token=NULL WHERE id=%s", (g.user["id"],))
    session.clear()
    flash("Logged out.", "info")
    return redirect(url_for("login"))


@app.route("/admin/users", methods=["GET", "POST"])
def admin_users():
    """
    Admin page:
      - Step 1: Ask for admin password (ADMIN_CREATE_SECRET) on a separate gate page.
      - Step 2: After unlocked (session['admin_ok'] = True), allow:
          * Create users
          * Activate / deactivate users
          * Reset device lock for a user
          * Force logout a user
    """
    conn = db()

    if request.method == "POST":
        action = request.form.get("action") or ""

        # 1) Unlock admin mode (from admin_gate.html)
        if action == "unlock":
            admin_secret = (request.form.get("admin_secret") or "").strip()

            if not ADMIN_CREATE_SECRET:
                flash("ADMIN_CREATE_SECRET not configured on server.", "danger")
                return render_template("admin_gate.html")

            if admin_secret != ADMIN_CREATE_SECRET:
                flash("Invalid admin password.", "danger")
                return render_template("admin_gate.html")

            # mark admin session as unlocked
            session["admin_ok"] = True
            flash("Admin mode unlocked.", "success")
            return redirect(url_for("admin_users"))

        # For all other actions, require unlocked admin session
        if not session.get("admin_ok"):
            flash("Admin access required.", "danger")
            return render_template("admin_gate.html")

        # 2) CREATE USER
        if action == "create":
            username = (request.form.get("username") or "").strip()
            password = request.form.get("password") or ""

            if not username or not password:
                flash("Enter username and password.", "warning")
                return redirect(url_for("admin_users"))

            pw_hash = generate_password_hash(password)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO users (username, password_hash, is_active) "
                        "VALUES (%s, %s, TRUE)",
                        (username, pw_hash)
                    )
                flash(f"User '{username}' created.", "success")
            except Exception as e:
                log.exception("Create user failed: %s", e)
                flash("Could not create user (maybe username already exists).", "danger")

        # 3) TOGGLE ACTIVE (activate / deactivate)
        elif action == "toggle_active":
            try:
                user_id = int(request.form.get("user_id") or "0")
                new_state = request.form.get("new_state")  # "1" or "0"
            except ValueError:
                flash("Invalid user id.", "danger")
                return redirect(url_for("admin_users"))

            is_active = True if new_state == "1" else False
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET is_active=%s WHERE id=%s",
                        (is_active, user_id)
                    )
                flash(("Activated" if is_active else "Deactivated") + f" user id {user_id}.", "info")
            except Exception as e:
                log.exception("Toggle active failed: %s", e)
                flash("Could not update user status.", "danger")

        # 4) RESET DEVICE (clear device lock)
        elif action == "reset_device":
            try:
                user_id = int(request.form.get("user_id") or "0")
            except ValueError:
                flash("Invalid user id.", "danger")
                return redirect(url_for("admin_users"))

            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE users
                        SET device_token = NULL,
                            device_ua = NULL,
                            device_info = NULL
                        WHERE id = %s
                    """, (user_id,))
                flash(f"Device lock reset for user id {user_id}.", "info")
            except Exception as e:
                log.exception("Reset device failed: %s", e)
                flash("Could not reset device.", "danger")

        # 5) FORCE LOGOUT (invalidate current session token)
        elif action == "force_logout":
            try:
                user_id = int(request.form.get("user_id") or "0")
            except ValueError:
                flash("Invalid user id.", "danger")
                return redirect(url_for("admin_users"))

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET current_session_token = NULL WHERE id = %s",
                        (user_id,)
                    )
                flash(f"User id {user_id} has been logged out.", "info")
            except Exception as e:
                log.exception("Force logout failed: %s", e)
                flash("Could not log out user.", "danger")

        return redirect(url_for("admin_users"))

    # ---------- GET ----------
    # If admin mode not unlocked yet -> show gate page
    if not session.get("admin_ok"):
        return render_template("admin_gate.html")

    # If unlocked, show full user list
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              id,
              username,
              is_active,
              (current_session_token IS NOT NULL) AS is_logged_in
            FROM users
            ORDER BY username
        """)
        users = cur.fetchall()

    return render_template("admin_users.html", users=users)




# -----------------------------
# Routes (protected)
# -----------------------------
@app.get("/healthz")
def healthz():
    return "ok", 200

@app.get("/")
@login_required
def home():
    t0 = time.time()
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
        videos = cur.fetchall()
    t_db_videos = time.time()

    video_ids = [v["video_id"] for v in videos]
    vids = []

    # 1) fetch channel ids (may use in-memory API cache)
    ch_map = fetch_channel_id_for_videos(video_ids) if YOUTUBE and video_ids else {}
    t_ch_map = time.time()

    # 2) batch-read latest channel totals from DB (fast)
    unique_chs = sorted({ch for ch in ch_map.values() if ch})
    channel_totals = get_latest_channel_totals_for(unique_chs) if unique_chs else {}
    t_ch_totals = time.time()

    # 3) batch-read latest view sample per video
    latest_samples = get_latest_sample_per_video(video_ids) if video_ids else {}
    t_latest_samples = time.time()

    for v in videos:
        vid = v["video_id"]
        thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
        short_title = v["name"] if len(v["name"]) <= 60 else v["name"][:57] + "..."
        channel_id = ch_map.get(vid)
        channel_total = channel_totals.get(channel_id) if channel_id else None

        latest = latest_samples.get(vid)
        latest_views = latest["views"] if latest else None
        latest_ts = latest["ts_utc"] if latest else None

        vids.append({
            "video_id": vid,
            "name": v["name"],
            "short_title": short_title,
            "thumbnail": thumb,
            "is_tracking": bool(v["is_tracking"]),
            "channel_total_cached": channel_total,
            "latest_views": latest_views,
            "latest_ts": latest_ts
        })

    t_end = time.time()
    # Optional timing logs to help you measure improvements
    log.info("home timings: db_videos=%.3fs ch_map=%.3fs ch_totals=%.3fs latest_samples=%.3fs total=%.3fs",
             t_db_videos - t0, t_ch_map - t_db_videos, t_ch_totals - t_ch_map, t_latest_samples - t_ch_totals, t_end - t0)

    return render_template("home.html", videos=vids)

@app.get("/video/<video_id>")
@login_required
def video_detail(video_id):
    info = build_video_display(video_id)
    if info is None:
        flash("Video not found.", "warning")
        return redirect(url_for("home"))
    info["thumbnail"] = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    return render_template("video_detail.html", v=info)

@app.post("/add_video")
@login_required
def add_video():
    link = (request.form.get("link") or "").strip()
    if not link:
        flash("Paste a YouTube link.", "warning")
        return redirect(url_for("home"))
    video_id = extract_video_id(link)
    if not video_id:
        flash("Invalid YouTube link.", "danger")
        return redirect(url_for("home"))

    title = fetch_title(video_id)
    stats = fetch_stats_batch([video_id]).get(video_id)
    if not stats:
        flash("Could not fetch stats (check API key/quota/video).", "danger")
        return redirect(url_for("home"))

    conn = db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO video_list (video_id, name, is_tracking)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (video_id) DO UPDATE SET name=EXCLUDED.name, is_tracking=TRUE
        """, (video_id, title))
    safe_store(video_id, stats)
    flash(f"Now tracking: {title}", "success")
    return redirect(url_for("video_detail", video_id=video_id))

@app.post("/add_target/<video_id>")
@login_required
def add_target(video_id):
    tv = request.form.get("target_views", "").strip()
    tts = request.form.get("target_ts", "").strip()
    note = (request.form.get("note") or "").strip()
    if not tv or not tts:
        flash("Fill target views and target time.", "warning")
        return redirect(url_for("video_detail", video_id=video_id))
    try:
        target_views = int(tv)
        local_dt = datetime.fromisoformat(tts)
        target_ts_utc = local_dt.replace(tzinfo=IST).astimezone(timezone.utc)
    except Exception:
        flash("Invalid input.", "danger")
        return redirect(url_for("video_detail", video_id=video_id))
    conn = db()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO targets (video_id, target_views, target_ts, note) VALUES (%s, %s, %s, %s)",
                    (video_id, target_views, target_ts_utc, note))
    flash("Target added.", "success")
    return redirect(url_for("video_detail", video_id=video_id))
@app.post("/set_comparison/<video_id>")
@login_required
def set_comparison(video_id):
    """
    Configure comparison for a video:
    - comparison_link: YouTube URL or video id
    - comparison_date: YYYY-MM-DD (IST date for comparison video)
    Logic:
      Let main_latest_date_ist be latest IST date for this video.
      offset_days = main_latest_date_ist - comparison_date
      For any row at date D_main, we compare to D_comp = D_main - offset_days, same time-of-day.
    """
    comp_link = (request.form.get("comparison_link") or "").strip()
    comp_date_str = (request.form.get("comparison_date") or "").strip()

    # If both empty -> clear comparison
    if not comp_link and not comp_date_str:
        conn = db()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE video_list SET compare_video_id=NULL, compare_offset_days=NULL WHERE video_id=%s",
                (video_id,)
            )
        flash("Comparison cleared for this video.", "info")
        return redirect(url_for("video_detail", video_id=video_id))

    # Resolve comparison video id from URL or raw id
    comp_vid = extract_video_id(comp_link) or comp_link
    if not comp_vid:
        flash("Invalid comparison video link / id.", "danger")
        return redirect(url_for("video_detail", video_id=video_id))

    # Parse comparison date (YYYY-MM-DD)
    try:
        comp_date = datetime.fromisoformat(comp_date_str).date()
    except Exception:
        flash("Invalid comparison date (use YYYY-MM-DD).", "danger")
        return redirect(url_for("video_detail", video_id=video_id))

    conn = db()
    with conn.cursor() as cur:
        # latest sample for MAIN video
        cur.execute(
            "SELECT max(ts_utc) AS latest_ts FROM views WHERE video_id=%s",
            (video_id,)
        )
        r = cur.fetchone()

    if not r or not r["latest_ts"]:
        flash("Cannot set comparison: no data yet for this video.", "warning")
        return redirect(url_for("video_detail", video_id=video_id))

    latest_utc = r["latest_ts"]
    latest_ist_date = latest_utc.astimezone(IST).date()

    offset_days = (latest_ist_date - comp_date).days

    conn = db()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE video_list SET compare_video_id=%s, compare_offset_days=%s WHERE video_id=%s",
            (comp_vid, offset_days, video_id)
        )

    flash(f"Comparison set: vs {comp_vid} with offset {offset_days} days.", "success")
    return redirect(url_for("video_detail", video_id=video_id))


@app.get("/remove_target/<int:target_id>")
@login_required
def remove_target(target_id):
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT video_id FROM targets WHERE id=%s", (target_id,))
        r = cur.fetchone()
        if not r:
            flash("Target not found.", "warning")
            return redirect(url_for("home"))
        vid = r["video_id"]
        cur.execute("DELETE FROM targets WHERE id=%s", (target_id,))
    flash("Target removed.", "info")
    return redirect(url_for("video_detail", video_id=vid))

@app.route("/stop_tracking/<video_id>", methods=("GET", "POST"))
def stop_tracking(video_id):
    """
    Toggle tracking state. Accepts GET (link) and POST (form with optional admin_secret).
    If ADMIN_CREATE_SECRET is set, require either session['admin_ok'] or the posted admin_secret.
    """
    conn = db()

    # If this is a POST from the UI, optionally require admin password (unless admin already unlocked)
    if request.method == "POST":
        # if admin mode already unlocked in session, skip password check
        if not session.get("admin_ok"):
            admin_secret = (request.form.get("admin_secret") or "").strip()
            if not ADMIN_CREATE_SECRET:
                flash("Admin password not configured on server.", "danger")
                return redirect(url_for("video_detail", video_id=video_id))
            if admin_secret != ADMIN_CREATE_SECRET:
                flash("Incorrect admin password.", "danger")
                return redirect(url_for("video_detail", video_id=video_id))

    # Toggle tracking state
    with conn.cursor() as cur:
        cur.execute("SELECT is_tracking, name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("home"))
        new_state = not bool(row["is_tracking"])
        cur.execute("UPDATE video_list SET is_tracking=%s WHERE video_id=%s", (new_state, video_id))
        flash(("Resumed" if new_state else "Paused") + f" tracking: {row['name']}", "info")

    # Redirect back to the video detail page (or index if you prefer)
    return redirect(url_for("video_detail", video_id=video_id))


@app.post("/remove_video/<video_id>")
@login_required
def remove_video(video_id):
    # ✅ Require admin password to remove any video
    admin_secret = (request.form.get("admin_secret") or "").strip()
    if ADMIN_CREATE_SECRET and admin_secret != ADMIN_CREATE_SECRET:
        flash("Admin password required or incorrect to remove videos.", "danger")
        return redirect(url_for("video_detail", video_id=video_id))

    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("home"))
        name = row["name"]
        cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
        cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
        flash(f"Removed '{name}' and all data.", "success")
    return redirect(url_for("home"))



@app.get("/export/<video_id>")
@login_required
def export_video(video_id):
    """
    Export all stored rows for a video into Excel.
    Columns (in this order):
      Time (IST), Views, Gain (5 min), Hourly Gain, Gain (24 h),
      Change 24h vs prev day (%), Projected (min) views, Compare diff
    """
    info = build_video_display(video_id)
    if info is None:
        flash("Video not found.", "warning")
        return redirect(url_for("home"))

    rows_for_df = []
    # dates sorted ascending so export is chronological (oldest first)
    dates = sorted(info["daily"].keys())
    for date in dates:
        # info["daily"][date] is newest-first in UI; reverse to earliest-first
        day_rows = list(reversed(info["daily"][date]))

        for tpl in day_rows:
            # tpl might have slightly different shapes depending on your pipeline.
            # Expected canonical order (8 items):
            # (ts, views, gain5, hourly_gain, gain24, pct24, projected, comp_diff)
            ts = tpl[0]
            views = tpl[1] if len(tpl) > 1 else None

            # defaults
            gain5 = hourly_gain = gain24 = pct24 = projected = comp_diff = None

            rest = list(tpl[2:])  # remaining fields after ts and views

            # Try to map by length of rest
            if len(rest) >= 6:
                # rest: gain5, hourly_gain, gain24, pct24, projected, comp_diff, ...
                gain5, hourly_gain, gain24, pct24, projected, comp_diff = rest[:6]
            elif len(rest) == 5:
                # rest: gain5, hourly_gain, gain24, pct24, projected
                gain5, hourly_gain, gain24, pct24, projected = rest
            elif len(rest) == 4:
                # rest: gain5, hourly_gain, gain24, pct24
                gain5, hourly_gain, gain24, pct24 = rest
            elif len(rest) == 3:
                # ambiguous: assume gain5, gain24, pct24 (old shape)
                gain5, gain24, pct24 = rest
                hourly_gain = None
            elif len(rest) == 2:
                # assume gain5, hourly_gain
                gain5, hourly_gain = rest
            elif len(rest) == 1:
                gain5 = rest[0]

            # normalize percentage column to a string with two decimals (empty if None)
            pct24_str = ""
            if pct24 is not None and pct24 != "":
                try:
                    pct24_str = f"{float(pct24):.2f}"
                except Exception:
                    pct24_str = str(pct24)

            rows_for_df.append({
                "Time (IST)": ts,
                "Views": views if views is not None else "",
                "Gain (5 min)": gain5 if gain5 is not None else "",
                "Hourly Gain": hourly_gain if hourly_gain is not None else "",
                "Gain (24 h)": gain24 if gain24 is not None else "",
                "Change 24h vs prev day (%)": pct24_str,
                "Projected (min) views": projected if projected is not None else "",
                "Compare diff": comp_diff if comp_diff is not None else ""
            })

    # Build dataframe
    df_views = pd.DataFrame(rows_for_df)

    # Targets sheet (same as before)
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, target_views, target_ts, note FROM targets WHERE video_id=%s ORDER BY target_ts ASC", (video_id,))
        target_rows = cur.fetchall()

    nowu = now_utc()
    targets_rows_for_df = []
    for t in target_rows:
        tid = t["id"]
        t_views = t["target_views"]
        t_ts = t["target_ts"]
        note = t["note"]
        latest_views = info.get("latest_views")
        remaining_views = t_views - (latest_views or 0)
        remaining_seconds = (t_ts - nowu).total_seconds()
        if remaining_views <= 0:
            status = "Reached"
            req_hr = 0
            req_5m = 0
        elif remaining_seconds <= 0:
            status = "Overdue"
            req_hr = math.ceil(remaining_views)
            req_5m = math.ceil(req_hr / 12)
        else:
            status = "Active"
            hrs = max(remaining_seconds / 3600.0, 1/3600)
            req_hr = math.ceil(remaining_views / hrs)
            req_5m = math.ceil(req_hr / 12)
        targets_rows_for_df.append({
            "Target ID": tid,
            "Target views": t_views,
            "Target time (IST)": t_ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S"),
            "Status": status,
            "Remaining views": remaining_views,
            "Required / hr": req_hr,
            "Required / 5min": req_5m,
            "Note": note
        })

    df_targets = pd.DataFrame(targets_rows_for_df)

    # Write to Excel
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        # ensure column order in file by creating DataFrame with exact keys in order above
        cols_order = [
            "Time (IST)", "Views", "Gain (5 min)", "Hourly Gain",
            "Gain (24 h)", "Change 24h vs prev day (%)",
            "Projected (min) views", "Compare diff"
        ]
        # if df_views lacks any column (edge case), create them to preserve order
        for c in cols_order:
            if c not in df_views.columns:
                df_views[c] = ""
        df_views[cols_order].to_excel(writer, index=False, sheet_name="Views")

        if not df_targets.empty:
            df_targets.to_excel(writer, index=False, sheet_name="Targets")

    bio.seek(0)
    safe = "".join(c for c in info["name"] if c.isalnum() or c in " _-").rstrip() or "export"
    return send_file(
        bio,
        as_attachment=True,
        download_name=f"{safe}_views.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# Bootstrap
init_db()
start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
