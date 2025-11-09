import os
import threading
import logging
import time
from datetime import datetime, timedelta
from io import BytesIO
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo

import pandas as pd
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file
)
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import psycopg
from psycopg.rows import dict_row

# -----------------------------
# Flask & Logging Setup
# -----------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(24))
logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

IST = ZoneInfo("Asia/Kolkata")

# -----------------------------
# Env & External Services
# -----------------------------
API_KEY = os.getenv("YOUTUBE_API_KEY", "")
YOUTUBE = None
if API_KEY:
    try:
        YOUTUBE = build("youtube", "v3", developerKey=API_KEY, cache_discovery=False)
        logger.info("YouTube client initialized.")
    except Exception as e:
        logger.exception("Failed to initialize YouTube client: %s", e)

POSTGRES_URL = os.getenv("DATABASE_URL")
if not POSTGRES_URL:
    logger.warning("DATABASE_URL not set. Please configure your Postgres connection string.")

# -----------------------------
# Database Connection (global)
# -----------------------------
_db_conn = None
_db_lock = threading.Lock()

def get_db():
    """
    Returns a live psycopg3 connection with autocommit and TCP keepalives.
    Reconnects if the existing connection is closed.
    """
    global _db_conn
    with _db_lock:
        if _db_conn is None or _db_conn.closed:
            logger.info("Opening DB connection...")
            _db_conn = psycopg.connect(
                POSTGRES_URL,
                autocommit=True,
                row_factory=dict_row,
                # TCP keepalives (works on most managed Postgres)
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
        return _db_conn

def init_db():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS video_list (
            video_id   TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            is_tracking BOOLEAN NOT NULL DEFAULT TRUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS views (
            video_id TEXT NOT NULL,
            date     DATE NOT NULL,
            ts       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            views    BIGINT NOT NULL,
            likes    BIGINT,
            PRIMARY KEY (video_id, ts),
            FOREIGN KEY (video_id) REFERENCES video_list(video_id) ON DELETE CASCADE
        );
        """)
    logger.info("Database schema ready.")

# -----------------------------
# YouTube Helpers
# -----------------------------
def extract_video_id(link: str) -> str | None:
    """
    Supports:
      - https://www.youtube.com/watch?v=VIDEOID
      - https://youtu.be/VIDEOID
      - m.youtube.com, youtube.com/shorts/VIDEOID
      - With extra params like &t=30s
    """
    try:
        u = urlparse(link.strip())
        host = (u.netloc or "").lower()

        if "youtu.be" in host:
            # Path starts with /VIDEOID
            vid = u.path.strip("/").split("/")[0]
            return vid or None

        if "youtube.com" in host:
            # /watch?v=ID  or /shorts/ID
            if u.path == "/watch":
                qs = parse_qs(u.query)
                return (qs.get("v") or [None])[0]
            parts = u.path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] in {"shorts", "live"}:
                return parts[1]
        return None
    except Exception:
        return None

def fetch_video_title(video_id: str) -> str:
    if not YOUTUBE:
        return "Unknown"
    try:
        resp = YOUTUBE.videos().list(
            part="snippet",
            id=video_id,
            maxResults=1
        ).execute()
        items = resp.get("items", [])
        title = (items[0]["snippet"]["title"] if items else "Unknown")[:100]
        return title or "Unknown"
    except HttpError as e:
        logger.error("YouTube API error (title) for %s: %s", video_id, e)
        return "Unknown"
    except Exception as e:
        logger.exception("Error fetching title: %s", e)
        return "Unknown"

def fetch_views_batch(video_ids: list[str]) -> dict:
    """
    Returns: { video_id: { 'views': int, 'likes': int|None } }
    """
    if not YOUTUBE or not video_ids:
        return {}
    out = {}
    # YouTube videos.list can take up to 50 IDs per call
    try:
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i+50]
            resp = YOUTUBE.videos().list(
                part="statistics",
                id=",".join(chunk),
                maxResults=50
            ).execute()
            for it in resp.get("items", []):
                vid = it["id"]
                stats = it.get("statistics", {})
                views = int(stats.get("viewCount", "0") or 0)
                likes = stats.get("likeCount")
                likes = int(likes) if likes is not None else None
                out[vid] = {"views": views, "likes": likes}
    except HttpError as e:
        logger.error("YouTube API error (stats batch): %s", e)
    except Exception as e:
        logger.exception("Error fetching stats: %s", e)
    return out

# -----------------------------
# Storage Helpers
# -----------------------------
def ist_now():
    return datetime.now(IST).replace(microsecond=0)

def safe_store(video_id: str, stats: dict):
    """
    Defensive 'delete+insert' at (video_id, ts) second precision.
    """
    ts = ist_now()
    date_only = ts.date()
    views = int(stats.get("views", 0))
    likes = stats.get("likes")

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM views WHERE video_id=%s AND ts=%s",
            (video_id, ts)
        )
        cur.execute(
            "INSERT INTO views (video_id, date, ts, views, likes) VALUES (%s, %s, %s, %s, %s)",
            (video_id, date_only, ts, views, likes)
        )

def process_gains(rows: list[dict]) -> list[tuple[str, int, int | None, int | None]]:
    """
    Input: rows (ASC by ts), single day.
    Output: list of (ts_str, views, gain_5min, hourly_gain)
    """
    out = []
    # Build lookup by timestamp for efficient hourly reference
    by_ts = [r["ts"] for r in rows]
    for idx, r in enumerate(rows):
        ts = r["ts"]
        views = r["views"]

        # Gain since last poll (same day)
        if idx == 0:
            gain = None
        else:
            gain = views - rows[idx - 1]["views"]

        # Hourly gain: diff vs most recent row at/just before ts-1h (same day)
        target = ts - timedelta(hours=1)
        # Find the rightmost ts <= target
        ref_idx = None
        for j in range(idx, -1, -1):
            if rows[j]["ts"] <= target:
                ref_idx = j
                break
        hourly = (views - rows[ref_idx]["views"]) if ref_idx is not None else None

        out.append((ts.strftime("%Y-%m-%d %H:%M:%S"), views, gain, hourly))
    return out

# -----------------------------
# Background Sampler (singleton)
# -----------------------------
_sampler_started = False
_sampler_lock = threading.Lock()

def _sleep_until_next_5_min_boundary():
    """
    Aligns to IST clock: 00, 05, 10, 15, ...
    """
    now = ist_now()
    minute = now.minute
    next_minute = ((minute // 5) + 1) * 5
    if next_minute >= 60:
        # Jump to next hour
        next_time = now.replace(minute=0, second=0) + timedelta(hours=1)
    else:
        next_time = now.replace(minute=next_minute, second=0)
    delta = (next_time - now).total_seconds()
    time.sleep(max(1, delta))  # avoid zero/negative

def _sampler_loop():
    logger.info("Background sampler started (5-min cadence, IST).")
    while True:
        try:
            _sleep_until_next_5_min_boundary()

            # Get active videos
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute("SELECT video_id FROM video_list WHERE is_tracking=TRUE ORDER BY video_id")
                rows = cur.fetchall()
            video_ids = [r["video_id"] for r in rows]

            if not video_ids:
                continue

            stats_map = fetch_views_batch(video_ids)
            for vid in video_ids:
                stats = stats_map.get(vid)
                if stats:
                    safe_store(vid, stats)
        except Exception as e:
            logger.exception("Sampler loop error: %s", e)
            # brief backoff to avoid tight crash loops
            time.sleep(5)

def start_background():
    global _sampler_started
    with _sampler_lock:
        if not _sampler_started:
            t = threading.Thread(target=_sampler_loop, daemon=True, name="yt-tracker-sampler")
            t.start()
            _sampler_started = True
            logger.info("Sampler thread started.")

# -----------------------------
# Routes
# -----------------------------
@app.route("/", methods=["GET"])
def index():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
        videos = cur.fetchall()

    enriched = []
    with conn.cursor() as cur:
        for v in videos:
            video_id = v["video_id"]
            # Get distinct dates that have data for this video (newest first)
            cur.execute(
                "SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC",
                (video_id,)
            )
            dates = [r["date"] for r in cur.fetchall()]

            daily = {}
            for d in dates:
                cur.execute(
                    "SELECT ts, views FROM views WHERE video_id=%s AND date=%s ORDER BY ts ASC",
                    (video_id, d)
                )
                rows = cur.fetchall()
                if not rows:
                    continue
                # Ensure Python datetime timezone-naive but IST-consistent (we stored naive IST)
                # Already naive: ts is stored as naive local (IST-time) timestamps.
                # Just pass through.
                processed = process_gains(rows)
                daily[d.strftime("%Y-%m-%d")] = processed

            enriched.append({
                "video_id": video_id,
                "name": v["name"],
                "is_tracking": bool(v["is_tracking"]),
                "daily_data": daily
            })

    return render_template("index.html", videos=enriched)

@app.route("/add_video", methods=["POST"])
def add_video():
    link = (request.form.get("link") or "").strip()
    if not link:
        flash("Please paste a YouTube link.", "warning")
        return redirect(url_for("index"))

    video_id = extract_video_id(link)
    if not video_id:
        flash("Couldn't extract a valid YouTube video ID from that link.", "danger")
        return redirect(url_for("index"))

    title = fetch_video_title(video_id)
    # Fetch immediate stats to start tracking with a first row
    stats = fetch_views_batch([video_id]).get(video_id)
    if not stats:
        flash("Could not fetch YouTube stats (check API key / quota / video availability).", "danger")
        return redirect(url_for("index"))

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO video_list (video_id, name, is_tracking)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (video_id) DO UPDATE SET name=EXCLUDED.name, is_tracking=TRUE
        """, (video_id, title))

    # Store first snapshot
    try:
        safe_store(video_id, stats)
    except Exception as e:
        logger.exception("Failed to store first snapshot for %s: %s", video_id, e)

    flash(f"Now tracking: {title}", "success")
    return redirect(url_for("index"))

@app.route("/stop_tracking/<video_id>")
def stop_tracking(video_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT is_tracking, name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("index"))
        new_state = not bool(row["is_tracking"])
        cur.execute("UPDATE video_list SET is_tracking=%s WHERE video_id=%s", (new_state, video_id))
        flash(("Resumed" if new_state else "Paused") + f" tracking: {row['name']}", "info")
    return redirect(url_for("index"))

@app.route("/remove_video/<video_id>")
def remove_video(video_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("index"))
        name = row["name"]
        cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
        cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
        flash(f"Removed '{name}' and all its historical data.", "success")
    return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export_video(video_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("index"))
        name = row["name"]

        cur.execute("SELECT ts, views FROM views WHERE video_id=%s ORDER BY ts ASC", (video_id,))
        data = cur.fetchall()

    if not data:
        flash("No data to export for this video yet.", "warning")
        return redirect(url_for("index"))

    # Build DataFrame (Time as string in IST, Views)
    df = pd.DataFrame([{
        "Time": r["ts"].strftime("%Y-%m-%d %H:%M:%S"),
        "Views": r["views"]
    } for r in data])

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Views")
    bio.seek(0)

    safe_name = "".join(c for c in name if c.isalnum() or c in " _-").rstrip()
    filename = f"{safe_name}_views.xlsx" if safe_name else "export_views.xlsx"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# -----------------------------
# App bootstrap
# -----------------------------
init_db()
start_background()

if __name__ == "__main__":
    # For local dev: flask run or python app.py
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
