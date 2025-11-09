import os
import re
import threading
import time
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs

from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from googleapiclient.discovery import build
import pandas as pd
import psycopg  # core driver for psycopg3

# ---------------------- Config ----------------------
IST = ZoneInfo("Asia/Kolkata")
DATABASE_URL = os.getenv("DATABASE_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY") or os.getenv("YOUTUBE_APIKEY") or os.getenv("YOUTUBE_API_KEY_V3")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env var is required")
if not YOUTUBE_API_KEY:
    raise RuntimeError("YOUTUBE_API_KEY env var is required")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")  # replace in prod

# ---------------------- DB Pool (with fallback) ----------------------
# Prefer psycopg_pool if installed; else use a tiny fallback with the same .connection() API.
try:
    from psycopg_pool import ConnectionPool
    pool = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=5,
        kwargs={
            "autocommit": True,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
            "options": "-c timezone=UTC",
        },
    )
    app.logger.info("Using psycopg ConnectionPool")
except ModuleNotFoundError:
    class _MiniPool:
        def __init__(self, conninfo): self.conninfo = conninfo
        def connection(self):
            return psycopg.connect(
                self.conninfo,
                autocommit=True,
                options="-c timezone=UTC",
            )
    pool = _MiniPool(DATABASE_URL)
    app.logger.warning("psycopg_pool not found; using fallback mini-pool")

# ---------------------- DB Bootstrap ----------------------
SCHEMA_SQL = """
-- videos being tracked
CREATE TABLE IF NOT EXISTS video_list (
    id SERIAL PRIMARY KEY,
    video_id TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5-minute snapshots
CREATE TABLE IF NOT EXISTS views (
    id BIGSERIAL PRIMARY KEY,
    video_id TEXT NOT NULL REFERENCES video_list(video_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    views BIGINT,
    likes BIGINT
);

-- In case an older/broken table exists, add missing columns safely
ALTER TABLE views
    ADD COLUMN IF NOT EXISTS ts TIMESTAMPTZ NOT NULL,
    ADD COLUMN IF NOT EXISTS views BIGINT,
    ADD COLUMN IF NOT EXISTS likes BIGINT;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_views_vid_ts ON views(video_id, ts);
"""

def init_db():
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)

# ---------------------- YouTube API ----------------------
def yt_client():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

YOUTUBE_ID_RX = re.compile(r"^[A-Za-z0-9_-]{11}$")

def extract_video_id(url_or_id: str) -> str | None:
    s = url_or_id.strip()
    if YOUTUBE_ID_RX.match(s):
        return s
    try:
        u = urlparse(s)
        if u.netloc.endswith("youtube.com"):
            qs = parse_qs(u.query)
            if "v" in qs and qs["v"]:
                return qs["v"][0][:11]
            parts = [p for p in u.path.split("/") if p]
            if parts and YOUTUBE_ID_RX.match(parts[-1]):
                return parts[-1]
        if u.netloc.endswith("youtu.be"):
            path = u.path.strip("/")
            if YOUTUBE_ID_RX.match(path[:11]):
                return path[:11]
    except Exception:
        pass
    return None

def fetch_video_stats(video_id: str):
    """Return (title, views, likes) or None on failure."""
    try:
        yt = yt_client()
        resp = yt.videos().list(part="snippet,statistics", id=video_id).execute()
        items = resp.get("items", [])
        if not items:
            return None
        item = items[0]
        title = item["snippet"]["title"]
        stats = item.get("statistics", {})
        views = int(stats.get("viewCount", 0))
        likes = int(stats.get("likeCount", 0)) if "likeCount" in stats else None
        return title, views, likes
    except Exception as e:
        app.logger.exception(f"YouTube API error for {video_id}: {e}")
        return None

# ---------------------- Advisory Lock ----------------------
# Prevent multiple trackers across dynos/processes.
ADVISORY_LOCK_KEY = 814_220_415_337_129_001  # arbitrary 64-bit number

def try_advisory_lock(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        return cur.fetchone()[0]

# ---------------------- Background Tracker ----------------------
stop_event = threading.Event()
tracker_thread = None

def seconds_to_next_5min_boundary_IST(now_utc: datetime | None = None) -> float:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    minute = now_ist.minute
    if minute % 5 == 0 and now_ist.second < 3:
        return 0.0
    next_block_min = (minute // 5 + 1) * 5
    if next_block_min >= 60:
        target = (now_ist.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    else:
        target = now_ist.replace(minute=next_block_min, second=0, microsecond=0)
    delta = target - now_ist
    return max(delta.total_seconds(), 0.0)

def insert_snapshot(conn, video_id: str, ts_utc: datetime, views_val: int | None, likes_val: int | None):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM views WHERE video_id=%s AND ts=%s", (video_id, ts_utc))
        cur.execute(
            "INSERT INTO views (video_id, ts, views, likes) VALUES (%s, %s, %s, %s)",
            (video_id, ts_utc, views_val, likes_val),
        )

def poll_once(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT video_id FROM video_list WHERE active = TRUE ORDER BY created_at ASC")
        videos = [r[0] for r in cur.fetchall()]
    if not videos:
        return
    ts_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    for vid in videos:
        stats = fetch_video_stats(vid)
        if not stats:
            app.logger.warning(f"Stats unavailable for {vid}; skipping this round.")
            continue
        title, views_val, likes_val = stats
        try:
            insert_snapshot(conn, vid, ts_utc, views_val, likes_val)
            app.logger.info(f"Saved snapshot {vid} @ {ts_utc.isoformat()} views={views_val} likes={likes_val}")
        except Exception as e:
            app.logger.exception(f"DB write failed for {vid}: {e}")

def tracker_loop():
    app.logger.info("Tracker thread starting...")
    with pool.connection() as conn:
        if not try_advisory_lock(conn):
            app.logger.info("Another instance holds the tracker lock; this thread will idle.")
            while not stop_event.is_set():
                time.sleep(5)
            return

        app.logger.info("Advisory lock acquired. Background tracking is active.")
        while not stop_event.is_set():
            try:
                wait_sec = seconds_to_next_5min_boundary_IST()
                if wait_sec > 0:
                    stop_event.wait(wait_sec)
                    if stop_event.is_set():
                        break
                poll_once(conn)
                stop_event.wait(2)
            except Exception as e:
                app.logger.exception(f"Tracker loop error: {e}")
                stop_event.wait(10)
    app.logger.info("Tracker thread stopped.")

def start_tracker_thread():
    global tracker_thread
    if tracker_thread and tracker_thread.is_alive():
        return
    t = threading.Thread(target=tracker_loop, name="yt-tracker", daemon=True)
    t.start()
    tracker_thread = t

# ---------------------- Flask Views ----------------------
@app.route("/", methods=["GET"])
def index():
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT video_id, title, active, created_at FROM video_list ORDER BY created_at DESC")
        videos = cur.fetchall()

        data_per_video = {}
        for (vid, title, active, created_at) in videos:
            cur.execute(
                """
                SELECT ts, views, likes
                FROM views
                WHERE video_id=%s
                ORDER BY ts ASC
                """,
                (vid,),
            )
            rows = cur.fetchall()
            per_day = {}
            last_view = None
            ts_list = [r[0] for r in rows]
            view_list = [r[1] for r in rows]
            j = 0
            for i, (ts, v, likes) in enumerate(rows):
                ts_ist = ts.astimezone(IST)
                day_key = ts_ist.date().isoformat()

                gain_5min = None if last_view is None else v - last_view
                last_view = v

                cutoff = ts - timedelta(hours=1)
                while j < i and ts_list[j] < cutoff:
                    j += 1
                hourly_growth = None if j >= i else v - view_list[j]

                item = {
                    "ts": ts_ist.strftime("%Y-%m-%d %H:%M:%S"),
                    "views": v,
                    "likes": likes,
                    "gain5": gain_5min,
                    "growth60": hourly_growth,
                }
                per_day.setdefault(day_key, []).append(item)

            data_per_video[vid] = {
                "video_id": vid,
                "title": title,
                "active": active,
                "created_at": created_at.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S"),
                "per_day": per_day,
            }

    return render_template("index.html", videos=data_per_video)

@app.route("/add", methods=["POST"])
def add_video():
    url = request.form.get("url", "").strip()
    vid = extract_video_id(url)
    if not vid:
        flash("Invalid YouTube URL or ID.", "error")
        return redirect(url_for("index"))

    stats = fetch_video_stats(vid)
    if not stats:
        flash("Could not fetch video details from YouTube API.", "error")
        return redirect(url_for("index"))
    title, views_val, likes_val = stats

    try:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO video_list (video_id, title, active) VALUES (%s, %s, TRUE) "
                "ON CONFLICT (video_id) DO UPDATE SET title=EXCLUDED.title",
                (vid, title),
            )
            ts_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            insert_snapshot(conn, vid, ts_utc, views_val, likes_val)
        flash(f"Added: {title}", "success")
    except Exception as e:
        app.logger.exception(f"Add video failed: {e}")
        flash("Database error while adding video.", "error")

    return redirect(url_for("index"))

@app.route("/pause/<video_id>", methods=["POST"])
def pause_video(video_id):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE video_list SET active=FALSE WHERE video_id=%s", (video_id,))
    flash("Tracking paused.", "info")
    return redirect(url_for("index"))

@app.route("/resume/<video_id>", methods=["POST"])
def resume_video(video_id):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE video_list SET active=TRUE WHERE video_id=%s", (video_id,))
    flash("Tracking resumed.", "success")
    return redirect(url_for("index"))

@app.route("/remove/<video_id>", methods=["POST"])
def remove_video(video_id):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
    flash("Video and all historical data removed.", "warning")
    return redirect(url_for("index"))

@app.route("/export/<video_id>.xlsx", methods=["GET"])
def export_excel(video_id):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist, views
            FROM views
            WHERE video_id=%s
            ORDER BY ts ASC
        """, (video_id,))
        rows = cur.fetchall()

        cur.execute("SELECT title FROM video_list WHERE video_id=%s", (video_id,))
        r = cur.fetchone()
        title = r[0] if r else video_id

    if not rows:
        flash("No data to export for this video.", "error")
        return redirect(url_for("index"))

    df = pd.DataFrame(rows, columns=["Time (IST)", "Views"])
    safe_title = re.sub(r"[^A-Za-z0-9 _-]+", "", title).strip().replace(" ", "_")
    fname = f"{safe_title}_views.xlsx"
    path = os.path.join("/tmp", fname)
    df.to_excel(path, index=False)

    return send_file(
        path,
        as_attachment=True,
        download_name=fname,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@app.route("/healthz")
def healthz():
    return {"ok": True, "time": datetime.now(IST).isoformat()}

# ---------------------- Bootstrap (Flask 2.x/3.x safe) ----------------------
BOOT_DONE = False
def bootstrap():
    global BOOT_DONE
    if BOOT_DONE:
        return
    init_db()
    if os.getenv("DISABLE_TRACKER", "0") != "1":
        start_tracker_thread()
    BOOT_DONE = True

# Run bootstrap at import time so gunicorn workers are ready
bootstrap()

# ---------------------- Graceful shutdown ----------------------
def _shutdown():
    stop_event.set()
    if tracker_thread and tracker_thread.is_alive():
        tracker_thread.join(timeout=3)

import atexit
atexit.register(_shutdown)

if __name__ == "__main__":
    # Safe to call again; guarded by BOOT_DONE
    bootstrap()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
