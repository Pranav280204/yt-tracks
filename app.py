import os
import re
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
from googleapiclient.discovery import build
import psycopg
from psycopg.rows import dict_row

# === CONFIG ===
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")

DATABASE_URL = os.getenv("DATABASE_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not DATABASE_URL or not YOUTUBE_API_KEY:
    raise RuntimeError("Set DATABASE_URL and YOUTUBE_API_KEY")

IST = ZoneInfo("Asia/Kolkata")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

tracker_thread = None
stop_event = threading.Event()

# === DATABASE ===
def get_db():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    conn.execute("SET search_path TO yt_tracker, public;")
    conn.execute("SET TIME ZONE 'Asia/Kolkata';")
    return conn

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS yt_tracker;")
            cur.execute("SET search_path TO yt_tracker, public;")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS yt_tracker.video_list (
                    video_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    paused BOOLEAN DEFAULT FALSE
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yt_tracker.views (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT REFERENCES yt_tracker.video_list(video_id) ON DELETE CASCADE,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    date DATE NOT NULL,
                    views BIGINT,
                    likes BIGINT,
                    UNIQUE(video_id, timestamp)
                );
            """)
            conn.commit()
            logger.info("Database schema ready.")

# === YOUTUBE ===
def extract_video_id(url):
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"(?:embed\/)([0-9A-Za-z_-]{11})",
        r"(?:shorts\/)([0-9A-Za-z_-]{11})",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m: return m.group(1)
    return None

def fetch_video_stats(video_id):
    try:
        res = youtube.videos().list(part="statistics,snippet", id=video_id).execute()
        if not res["items"]: return None, None, None
        item = res["items"][0]
        stats = item["statistics"]
        title = item["snippet"]["title"]
        return title, int(stats.get("viewCount", 0)), int(stats.get("likeCount", 0))
    except Exception as e:
        logger.error(f"YouTube API error: {e}")
        return None, None, None

# === TRACKER ===
def get_next_5min_ist():
    now = datetime.now(IST)
    mins = (now.minute // 5) * 5
    next_t = now.replace(minute=mins, second=0, microsecond=0)
    if next_t <= now:
        next_t += timedelta(minutes=5)
    return next_t

def tracker_loop():
    logger.info("Tracker thread started.")
    while not stop_event.is_set():
        next_run = get_next_5min_ist()
        sleep_s = (next_run - datetime.now(IST)).total_seconds()
        if sleep_s > 0:
            stop_event.wait(sleep_s)
        if stop_event.is_set(): break

        now = datetime.now(IST)
        ts, date = now, now.date()

        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT video_id FROM yt_tracker.video_list WHERE NOT paused")
                    videos = [r["video_id"] for r in cur.fetchall()]

            for vid in videos:
                title, views, likes = fetch_video_stats(vid)
                if views is None: continue

                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM yt_tracker.views WHERE video_id = %s AND timestamp = %s", (vid, ts))
                        cur.execute("""
                            INSERT INTO yt_tracker.views (video_id, timestamp, date, views, likes)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (vid, ts, date, views, likes))
                    conn.commit()
                logger.info(f"Updated {vid}: {views:,} views")
        except Exception as e:
            logger.error(f"Tracker error: {e}")

def start_tracker():
    global tracker_thread
    if tracker_thread is None or not tracker_thread.is_alive():
        stop_event.clear()
        tracker_thread = threading.Thread(target=tracker_loop, daemon=True)
        tracker_thread.start()

# === ROUTES ===
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        url = request.form.get("youtube_url", "").strip()
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
                        INSERT INTO yt_tracker.video_list (video_id, title)
                        VALUES (%s, %s)
                        ON CONFLICT (video_id) DO UPDATE SET paused = FALSE
                    """, (vid, title))
                    now = datetime.now(IST)
                    cur.execute("""
                        INSERT INTO yt_tracker.views (video_id, timestamp, date, views, likes)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (video_id, timestamp) DO NOTHING
                    """, (vid, now, now.date(), views, likes))
                conn.commit()
            flash(f"Tracking: {title}", "success")
        except Exception as e:
            logger.error(f"DB error: {e}")
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

            if not rows: continue
            df = pd.DataFrame(rows)
            daily = df.groupby("date")

            tabs = []
            for date, g in daily:
                g = g.sort_values("timestamp").copy()
                g["gain_5min"] = g["views"].diff().fillna(0).astype(int)
                g["time_str"] = g["timestamp"].dt.strftime("%H:%M")
                g["views_str"] = g["views"].apply(lambda x: f"{x:,}")

                hourly = []
                for _, r in g.iterrows():
                    ago = r["timestamp"] - timedelta(minutes=60)
                    past = g[g["timestamp"] <= ago]
                    gain = r["views"] - past.iloc[-1]["views"] if not past.empty else 0
                    hourly.append(gain)
                g["hourly_rate"] = hourly

                tabs.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "date_display": date.strftime("%b %d, %Y"),
                    "rows": g.to_dict("records")
                })

            video_data.append({
                "video_id": v["video_id"],
                "title": v["title"],
                "paused": v["paused"],
                "daily_tabs": tabs
            })

        return render_template("index.html", videos=video_data)
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        flash("Error loading data.", "error")
        return render_template("index.html", videos=[])

@app.route("/pause/<video_id>")
def pause(video_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE yt_tracker.video_list SET paused = TRUE WHERE video_id = %s", (video_id,))
        conn.commit()
    flash("Paused.", "info")
    return redirect(url_for("index"))

@app.route("/resume/<video_id>")
def resume(video_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE yt_tracker.video_list SET paused = FALSE WHERE video_id = %s", (video_id,))
        conn.commit()
    flash("Resumed.", "success")
    return redirect(url_for("index"))

@app.route("/remove/<video_id>")
def remove(video_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM yt_tracker.video_list WHERE video_id = %s", (video_id,))
        conn.commit()
    flash("Removed.", "info")
    return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT title FROM yt_tracker.video_list WHERE video_id = %s", (video_id,))
                row = cur.fetchone()
                if not row: raise ValueError()
                title = row["title"]

                cur.execute("""
                    SELECT timestamp, views
                    FROM yt_tracker.views
                    WHERE video_id = %s
                    ORDER BY timestamp
                """, (video_id,))
                rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=["Time (IST)", "Views"])
        filename = f"{title.replace(' ', '_')[:30]}_views.xlsx"
        df.to_excel(filename, index=False, engine="openpyxl")
        return send_file(filename, as_attachment=True, download_name=filename)
    except:
        flash("Export failed.", "error")
        return redirect(url_for("index"))

# === START ===
if __name__ == "__main__":
    init_db()
    start_tracker()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
