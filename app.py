import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import logging
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
from googleapiclient.discovery import build
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

# === CONFIG ===
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key-change-in-prod")

# Environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not DATABASE_URL or not YOUTUBE_API_KEY:
    raise RuntimeError("Set DATABASE_URL and YOUTUBE_API_KEY in environment")

# IST Timezone
IST = ZoneInfo("Asia/Kolkata")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global YouTube client
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

# Background thread control
tracker_thread = None
stop_event = threading.Event()

# === DATABASE HELPERS ===
def get_db():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    conn.execute("SET TIME ZONE 'Asia/Kolkata';")
    return conn

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS video_list (
                    video_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    paused BOOLEAN DEFAULT FALSE
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS views (
                    id SERIAL PRIMARY KEY,
                    video_id TEXT REFERENCES video_list(video_id) ON DELETE CASCADE,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    date DATE NOT NULL,
                    views BIGINT,
                    likes BIGINT,
                    UNIQUE(video_id, timestamp)
                );
            """)
            conn.commit()

# === YOUTUBE HELPERS ===
def extract_video_id(url):
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"(?:embed\/)([0-9A-Za-z_-]{11})",
        r"(?:shorts\/)([0-9A-Za-z_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def fetch_video_stats(video_id):
    try:
        response = youtube.videos().list(
            part="statistics,snippet",
            id=video_id
        ).execute()
        if not response["items"]:
            return None, None, None
        item = response["items"][0]
        stats = item["statistics"]
        title = item["snippet"]["title"]
        views = int(stats.get("viewCount", 0))
        likes = int(stats.get("likeCount", 0))
        return title, views, likes
    except Exception as e:
        logger.error(f"YouTube API error for {video_id}: {e}")
        return None, None, None

# === BACKGROUND TRACKER ===
def get_next_5min_ist():
    now = datetime.now(IST)
    minutes = (now.minute // 5) * 5
    next_time = now.replace(minute=minutes, second=0, microsecond=0)
    if next_time <= now:
        next_time += timedelta(minutes=5)
    return next_time

def tracker_loop():
    logger.info("Starting YouTube tracker thread...")
    while not stop_event.is_set():
        next_run = get_next_5min_ist()
        sleep_seconds = (next_run - datetime.now(IST)).total_seconds()
        if sleep_seconds > 0:
            logger.info(f"Sleeping {sleep_seconds:.0f}s until {next_run.strftime('%H:%M:%S IST')}")
            stop_event.wait(sleep_seconds)

        if stop_event.is_set():
            break

        now_ist = datetime.now(IST)
        timestamp = now_ist
        date = now_ist.date()

        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT video_id FROM video_list WHERE NOT paused
                    """)
                    active_videos = [row["video_id"] for row in cur.fetchall()]

            for video_id in active_videos:
                title, views, likes = fetch_video_stats(video_id)
                if views is None:
                    logger.warning(f"Failed to fetch stats for {video_id}")
                    continue

                try:
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            # Delete any existing entry for this timestamp
                            cur.execute("""
                                DELETE FROM views WHERE video_id = %s AND timestamp = %s
                            """, (video_id, timestamp))

                            # Insert new snapshot
                            cur.execute("""
                                INSERT INTO views (video_id, timestamp, date, views, likes)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (video_id, timestamp, date, views, likes))
                        conn.commit()
                    logger.info(f"Updated {video_id}: {views:,} views")
                except Exception as e:
                    logger.error(f"DB insert error for {video_id}: {e}")

        except Exception as e:
            logger.error(f"Tracker loop error: {e}")

    logger.info("Tracker thread stopped.")

def start_tracker():
    global tracker_thread
    if tracker_thread is None or not tracker_thread.is_alive():
        stop_event.clear()
        tracker_thread = threading.Thread(target=tracker_loop, daemon=True)
        tracker_thread.start()
        logger.info("Tracker thread started.")

# === ROUTES ===
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        url = request.form.get("youtube_url", "").strip()
        if not url:
            flash("Please enter a YouTube URL.", "error")
            return redirect(url_for("index"))

        video_id = extract_video_id(url)
        if not video_id:
            flash("Invalid YouTube URL. Please check and try again.", "error")
            return redirect(url_for("index"))

        title, views, likes = fetch_video_stats(video_id)
        if not title:
            flash("Could not fetch video. It may be private or deleted.", "error")
            return redirect(url_for("index"))

        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO video_list (video_id, title)
                        VALUES (%s, %s)
                        ON CONFLICT (video_id) DO UPDATE SET paused = FALSE
                    """, (video_id, title))
                    # Insert first snapshot
                    now_ist = datetime.now(IST)
                    cur.execute("""
                        INSERT INTO views (video_id, timestamp, date, views, likes)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (video_id, timestamp) DO NOTHING
                    """, (video_id, now_ist, now_ist.date(), views, likes))
                conn.commit()
            flash(f"Started tracking: {title}", "success")
        except Exception as e:
            logger.error(f"Add video error: {e}")
            flash("Database error. Try again.", "error")

        return redirect(url_for("index"))

    # GET: Show dashboard
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT video_id, title, paused, added_at
                    FROM video_list
                    ORDER BY added_at DESC
                """)
                videos = cur.fetchall()

        video_data = []
        for video in videos:
            with get_db() as conn:
                with conn.cursor() as cur:
                    # Get daily data
                    cur.execute("""
                        SELECT date, timestamp, views, likes
                        FROM views
                        WHERE video_id = %s
                        ORDER BY timestamp
                    """, (video["video_id"],))
                    rows = cur.fetchall()

            if not rows:
                continue

            df = pd.DataFrame(rows)
            daily_groups = df.groupby("date")

            daily_tabs = []
            for date, group in daily_groups:
                group = group.sort_values("timestamp")
                group["gain_5min"] = group["views"].diff().fillna(0).astype(int)
                group["time_str"] = group["timestamp"].dt.strftime("%H:%M")
                group["views_str"] = group["views"].apply(lambda x: f"{x:,}")

                # Hourly rate: last 60 mins
                hourly_rates = []
                for _, row in group.iterrows():
                    hour_ago = row["timestamp"] - timedelta(minutes=60)
                    past = group[group["timestamp"] <= hour_ago]
                    if not past.empty:
                        gain = row["views"] - past.iloc[-1]["views"]
                        hourly_rates.append(gain)
                    else:
                        hourly_rates.append(0)
                group["hourly_rate"] = hourly_rates

                daily_tabs.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "date_display": date.strftime("%b %d, %Y"),
                    "rows": group.to_dict("records")
                })

            video_data.append({
                "video_id": video["video_id"],
                "title": video["title"],
                "paused": video["paused"],
                "daily_tabs": daily_tabs
            })

        return render_template("index.html", videos=video_data)

    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        flash("Error loading dashboard.", "error")
        return render_template("index.html", videos=[])


@app.route("/pause/<video_id>")
def pause(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_list SET paused = TRUE WHERE video_id = %s
                """, (video_id,))
                conn.commit()
        flash("Tracking paused.", "info")
    except:
        flash("Error pausing video.", "error")
    return redirect(url_for("index"))

@app.route("/resume/<video_id>")
def resume(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE video_list SET paused = FALSE WHERE video_id = %s
                """, (video_id,))
                conn.commit()
        flash("Tracking resumed.", "success")
    except:
        flash("Error resuming video.", "error")
    return redirect(url_for("index"))

@app.route("/remove/<video_id>")
def remove(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM video_list WHERE video_id = %s", (video_id,))
                conn.commit()
        flash("Video removed permanently.", "info")
    except:
        flash("Error removing video.", "error")
    return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export(video_id):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT title FROM video_list WHERE video_id = %s
                """, (video_id,))
                row = cur.fetchone()
                if not row:
                    flash("Video not found.", "error")
                    return redirect(url_for("index"))
                title = row["title"]

                cur.execute("""
                    SELECT timestamp, views
                    FROM views
                    WHERE video_id = %s
                    ORDER BY timestamp
                """, (video_id,))
                rows = cur.fetchall()

        if not rows:
            flash("No data to export.", "error")
            return redirect(url_for("index"))

        df = pd.DataFrame(rows, columns=["Time (IST)", "Views"])
        filename = f"{title.replace(' ', '_')[:30]}_views.xlsx"
        output = pd.ExcelWriter(filename, engine="openpyxl")
        df.to_excel(output, index=False)
        output.close()

        return send_file(
            filename,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        logger.error(f"Export error: {e}")
        flash("Export failed.", "error")
        return redirect(url_for("index"))

# === STARTUP ===
if __name__ == "__main__":
    init_db()
    start_tracker()
    # For Render/Railway: use gunicorn
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
