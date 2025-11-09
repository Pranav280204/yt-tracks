import os
import re
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd

from googleapiclient.discovery import build
import psycopg
from psycopg.rows import dict_row

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# === CONFIG ===
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

DATABASE_URL = os.getenv("DATABASE_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not DATABASE_URL or not YOUTUBE_API_KEY:
    raise RuntimeError("Set DATABASE_URL and YOUTUBE_API_KEY")

IST = ZoneInfo("Asia/Kolkata")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("yt-tracker")

# YouTube client (built once)
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

# APScheduler (started idempotently)
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
_scheduler_started = False

# A constant for advisory lock (any 64-bit signed int). Keep stable across deploys.
ADVISORY_LOCK_KEY = 9876543210123

# === DATABASE ===
def get_db():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    # Ensure we always use the same schema + timezone
    conn.execute("CREATE SCHEMA IF NOT EXISTS yt_tracker;")
    conn.execute("SET search_path TO yt_tracker, public;")
    conn.execute("SET TIME ZONE 'Asia/Kolkata';")
    return conn

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yt_tracker.video_list (
                    video_id TEXT PRIMARY KEY,
                    title   TEXT NOT NULL,
                    added_at TIMESTAMPTZ DEFAULT NOW(),
                    paused  BOOLEAN DEFAULT FALSE
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yt_tracker.views (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT REFERENCES yt_tracker.video_list(video_id) ON DELETE CASCADE,
                    timestamp TIMESTAMPTZ NOT NULL,
                    date DATE NOT NULL,
                    views BIGINT,
                    likes BIGINT,
                    UNIQUE(video_id, timestamp)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yt_tracker.meta (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );
            """)
            conn.commit()
            logger.info("Database schema ready.")

# === YOUTUBE ===
def extract_video_id(url: str):
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"(?:embed\/)([0-9A-Za-z_-]{11})",
        r"(?:shorts\/)([0-9A-Za-z_-]{11})",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

def fetch_video_stats(video_id: str):
    try:
        res = youtube.videos().list(part="statistics,snippet", id=video_id).execute()
        if not res.get("items"):
            return None, None, None
        item = res["items"][0]
        stats = item.get("statistics", {})
        title = item["snippet"]["title"]
        views = int(stats.get("viewCount", 0))
        likes = int(stats.get("likeCount", 0)) if "likeCount" in stats else None
        return title, views, likes
    except Exception as e:
        logger.error(f"YouTube API error for {video_id}: {e}")
        return None, None, None

# === SCHEDULER TICK ===
def _aligned_5min(dt: datetime) -> datetime:
    # Align to clock minute (*/5) with zero seconds
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)

def tick():
    """Runs every 5 minutes. Uses an advisory lock so only one worker runs."""
    now = datetime.now(IST)
    ts = now.replace(second=0, microsecond=0)  # already on */5 due to cron; safe to normalize
    date = ts.date()

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                # Try to take the advisory lock
                cur.execute("SELECT pg_try_advisory_lock(%s);", (ADVISORY_LOCK_KEY,))
                locked = cur.fetchone()["pg_try_advisory_lock"]
                if not locked:
                    logger.debug("Another worker holds the tick lock; skipping this run.")
                    return

                # Load active videos
                cur.execute("SELECT video_id FROM yt_tracker.video_list WHERE NOT paused;")
                videos = [r["video_id"] for r in cur.fetchall()]

                updated = 0

                for vid in videos:
                    title, views, likes = fetch_video_stats(vid)
                    if views is None:
                        continue

                    # Insert/update in one place to avoid duplicates or missing likes refresh
                    cur.execute("""
                        INSERT INTO yt_tracker.views (video_id, timestamp, date, views, likes)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (video_id, timestamp) DO UPDATE
                        SET views = EXCLUDED.views,
                            likes = COALESCE(EXCLUDED.likes, yt_tracker.views.likes)
                    """, (vid, ts, date, views, likes))
                    updated += 1

                # Save last tick time to meta (handy for UI/diagnostics)
                cur.execute("""
                    INSERT INTO yt_tracker.meta (k, v)
                    VALUES ('last_tick_ist', %s)
                    ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
                """, (ts.isoformat(),))

            conn.commit()
        logger.info(f"[tick] {updated} videos updated at {ts.isoformat()}")
    except Exception as e:
        logger.error(f"[tick] error: {e}")
    finally:
        # Best-effort unlock
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (ADVISORY_LOCK_KEY,))
                conn.commit()
        except Exception:
            pass

def start_scheduler_once():
    global _scheduler_started
    if _scheduler_started:
        return
    init_db()  # ensure schema exists before scheduling
    # Run exactly on */5 (00,05,10,...)
    scheduler.add_job(tick, CronTrigger(minute="*/5"))
    scheduler.start()
    _scheduler_started = True
    logger.info("APScheduler started: every 5 minutes.")

# Start scheduler at import time (works under Gunicorn as well).
start_scheduler_once()

# Also kick it in case of some edge server setups that import lazily
@app.before_first_request
def _kick_scheduler():
    start_scheduler_once()

# === ROUTES ===
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        url = (request.form.get("youtube_url") or "").strip()
        if not url:
            flash("Enter a URL.", "error")
            return redirect(url_for("index"))

        vid = extract_video_id(url)
        if not vid:
            flash("Invalid YouTube URL.", "error")
            return redirect(url_for("index"))

        title, views, likes = fetch_video_stats(vid)
        if not title:
            flash("Video not found or private.", "error")
            return redirect(url_for("index"))

        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO yt_tracker.video_list (video_id, title, paused)
                        VALUES (%s, %s, FALSE)
                        ON CONFLICT (video_id) DO UPDATE SET paused = FALSE, title = EXCLUDED.title
                    """, (vid, title))

                    # Seed an initial row aligned to current 5-min bucket for a neat chart
                    now_ist = datetime.now(IST)
                    seed_ts = _aligned_5min(now_ist)
                    cur.execute("""
                        INSERT INTO yt_tracker.views (video_id, timestamp, date, views, likes)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (video_id, timestamp) DO UPDATE
                        SET views = EXCLUDED.views,
                            likes = COALESCE(EXCLUDED.likes, yt_tracker.views.likes)
                    """, (vid, seed_ts, seed_ts.date(), views, likes))
                conn.commit()
            flash(f"Tracking: {title}", "success")
        except Exception as e:
            logger.error(f"DB error on add: {e}")
            flash("Database error.", "error")
        return redirect(url_for("index"))

    # GET: Dashboard
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT video_id, title, paused, added_at
                    FROM yt_tracker.video_list
                    ORDER BY added_at DESC
                """)
                videos = cur.fetchall()

                # last tick time (if available)
                cur.execute("SELECT v FROM yt_tracker.meta WHERE k = 'last_tick_ist';")
                row = cur.fetchone()
                last_tick = row["v"] if row else None

        video_data = []
        for v in videos:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT date, timestamp, views, likes
                        FROM yt_tracker.views
                        WHERE video_id = %s
                        ORDER BY timestamp
                    """, (v["video_id"],))
                    rows = cur.fetchall()

            if not rows:
                continue

            df = pd.DataFrame(rows)
            daily_groups = df.groupby("date")

            tabs = []
            for date_key, g in daily_groups:
                g = g.sort_values("timestamp").copy()

                # 5-min gain
                g["gain_5min"] = g["views"].diff().fillna(0).astype(int)

                # hourly rolling (relative to 60 min earlier)
                hourly = []
                for _, r in g.iterrows():
                    ago = r["timestamp"] - timedelta(minutes=60)
                    past = g[g["timestamp"] <= ago]
                    gain = r["views"] - past.iloc[-1]["views"] if not past.empty else 0
                    hourly.append(gain)
                g["hourly_rate"] = hourly

                g["time_str"] = g["timestamp"].dt.strftime("%H:%M")
                g["views_str"] = g["views"].apply(lambda x: f"{x:,}")

                tabs.append({
                    "date": date_key.strftime("%Y-%m-%d"),
                    "date_display": date_key.strftime("%b %d, %Y"),
                    "rows": g.to_dict("records")
                })

            video_data.append({
                "video_id": v["video_id"],
                "title": v["title"],
                "paused": v["paused"],
                "daily_tabs": tabs
            })

        return render_template("index.html", videos=video_data, last_tick=last_tick)
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        flash("Error loading data.", "error")
        return render_template("index.html", videos=[], last_tick=None)

@app.route("/pause/<video_id>")
def pause(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE yt_tracker.video_list SET paused = TRUE WHERE video_id = %s", (video_id,))
            conn.commit()
        flash("Paused.", "info")
    except Exception as e:
        logger.error(f"Pause error: {e}")
        flash("Failed to pause.", "error")
    return redirect(url_for("index"))

@app.route("/resume/<video_id>")
def resume(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE yt_tracker.video_list SET paused = FALSE WHERE video_id = %s", (video_id,))
            conn.commit()
        flash("Resumed.", "success")
    except Exception as e:
        logger.error(f"Resume error: {e}")
        flash("Failed to resume.", "error")
    return redirect(url_for("index"))

@app.route("/remove/<video_id>")
def remove(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM yt_tracker.video_list WHERE video_id = %s", (video_id,))
            conn.commit()
        flash("Removed.", "info")
    except Exception as e:
        logger.error(f"Remove error: {e}")
        flash("Failed to remove.", "error")
    return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT title FROM yt_tracker.video_list WHERE video_id = %s", (video_id,))
                row = cur.fetchone()
                if not row:
                    raise ValueError("Video not found")
                title = row["title"]

                cur.execute("""
                    SELECT timestamp AS "Time (IST)", views AS "Views"
                    FROM yt_tracker.views
                    WHERE video_id = %s
                    ORDER BY timestamp
                """, (video_id,))
                rows = cur.fetchall()

        df = pd.DataFrame(rows)
        safe_title = re.sub(r"[^A-Za-z0-9_-]+", "_", title)[:30]
        filename = f"{safe_title}_views.xlsx"
        df.to_excel(filename, index=False, engine="openpyxl")
        return send_file(filename, as_attachment=True, download_name=filename)
    except Exception as e:
        logger.error(f"Export error: {e}")
        flash("Export failed.", "error")
        return redirect(url_for("index"))

@app.route("/health")
def health():
    return {"ok": True}, 200

# === LOCAL DEV ENTRYPOINT ===
if __name__ == "__main__":
    # Local dev: Flask built-in server; scheduler already started
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
