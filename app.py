# app.py
import os
import threading
import logging
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
import pandas as pd
from flask import Flask, render_template, send_file, request, redirect, url_for, flash
from googleapiclient.discovery import build
import psycopg
from psycopg.rows import dict_row
from zoneinfo import ZoneInfo  # Python 3.9+

# === CONFIG ===
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(24).hex())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === ENV VARS ===
DATABASE_URL = os.getenv("DATABASE_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not DATABASE_URL or not YOUTUBE_API_KEY:
    raise RuntimeError("Set DATABASE_URL and YOUTUBE_API_KEY")

IST = ZoneInfo("Asia/Kolkata")
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

# === DB ===
db_conn = None
_background_thread = None
_thread_lock = threading.Lock()

def get_db():
    global db_conn
    if db_conn is None or db_conn.closed:
        db_conn = psycopg.connect(
            DATABASE_URL,
            row_factory=dict_row,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        db_conn.execute("SET TIME ZONE 'Asia/Kolkata';")
    return db_conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS views (
            video_id TEXT NOT NULL,
            date DATE NOT NULL,
            timestamp TEXT NOT NULL,
            views BIGINT NOT NULL,
            likes BIGINT NOT NULL,
            PRIMARY KEY (video_id, timestamp)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_tracking INTEGER DEFAULT 1
        );
    """)
    logger.info("Tables ready")

# === YOUTUBE ===
def extract_video_id(link):
    parsed = urlparse(link)
    if parsed.hostname in ("youtube.com", "www.youtube.com"):
        return parse_qs(parsed.query).get("v", [None])[0]
    if parsed.hostname == "youtu.be":
        return parsed.path[1:] if len(parsed.path) > 1 else None
    return None

def fetch_video_title(vid):
    try:
        resp = youtube.videos().list(part="snippet", id=vid).execute()
        return resp["items"][0]["snippet"]["title"][:100] if resp["items"] else "Unknown"
    except:
        return "Unknown"

def fetch_views(ids):
    if not ids: return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(ids)).execute()
        return {item["id"]: {
            "views": int(item["statistics"].get("viewCount", 0)),
            "likes": int(item["statistics"].get("likeCount", 0))
        } for item in resp.get("items", [])}
    except Exception as e:
        logger.error(f"API error: {e}")
        return {}

# === BACKGROUND POLLING ===
def safe_store(vid, stats):
    cur = get_db().cursor()
    now = datetime.now(IST)
    ts = now.strftime("%Y-%m-%d %H:%M:00")
    date = now.strftime("%Y-%m-%d")
    cur.execute("DELETE FROM views WHERE video_id=%s AND timestamp=%s", (vid, ts))
    cur.execute("""
        INSERT INTO views (video_id, date, timestamp, views, likes)
        VALUES (%s, %s, %s, %s, %s)
    """, (vid, date, ts, stats["views"], stats["likes"]))

def run_poll():
    logger.info("RUNNING POLL")
    cur = get_db().cursor()
    cur.execute("SELECT video_id FROM video_list WHERE is_tracking=1")
    ids = [r["video_id"] for r in cur.fetchall()]
    if not ids:
        logger.info("No videos to track")
        return
    stats = fetch_views(ids)
    for vid in ids:
        if vid in stats:
            safe_store(vid, stats[vid])
            logger.info(f"STORED {vid}: {stats[vid]['views']:,} views")

def background_task():
    logger.info("BACKGROUND TASK STARTED")
    while True:
        now = datetime.now(IST)
        seconds_into_5min = (now.minute % 5) * 60 + now.second
        wait = max(1, 300 - seconds_into_5min)
        logger.info(f"Next poll in {wait}s → { (now + timedelta(seconds=wait)).strftime('%H:%M:%S') }")
        time.sleep(wait)
        try:
            run_poll()
        except Exception as e:
            logger.error(f"Poll failed: {e}")
            time.sleep(60)

def start_background():
    global _background_thread
    with _thread_lock:
        if _background_thread is None or not _background_thread.is_alive():
            _background_thread = threading.Thread(target=background_task, daemon=False)
            _background_thread.start()
            logger.info("BACKGROUND THREAD STARTED")

# === ROUTES ===
@app.before_request
def ensure_background():
    start_background()  # Restart on every visit

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        link = request.form.get("video_link", "").strip()
        if not link:
            flash("Enter YouTube link", "error")
            return redirect(url_for("index"))
        vid = extract_video_id(link)
        if not vid:
            flash("Invalid link", "error")
            return redirect(url_for("index"))
        title = fetch_video_title(vid)
        stats = fetch_views([vid])
        if vid not in stats:
            flash("Can't fetch stats", "error")
            return redirect(url_for("index"))
        cur = get_db().cursor()
        cur.execute("""
            INSERT INTO video_list (video_id, name, is_tracking)
            VALUES (%s, %s, 1)
            ON CONFLICT (video_id) DO UPDATE SET name=%s, is_tracking=1
        """, (vid, title, title))
        safe_store(vid, stats[vid])
        flash(f"Added: {title}", "success")
        return redirect(url_for("index"))

    # GET
    videos = []
    cur = get_db().cursor()
    cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
    for row in cur.fetchall():
        vid = row["video_id"]
        cur.execute("SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC", (vid,))
        dates = [r["date"] for r in cur.fetchall()]
        daily = {}
        for d in dates:
            cur.execute("""
                SELECT timestamp, views FROM views
                WHERE video_id=%s AND date=%s
                ORDER BY timestamp
            """, (vid, d))
            rows = cur.fetchall()
            processed = []
            for i, r in enumerate(rows):
                gain = 0
                if i > 0:
                    gain = r["views"] - rows[i-1]["views"]
                hourly = 0
                ts_dt = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
                one_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
                cur.execute("""
                    SELECT views FROM views WHERE video_id=%s AND timestamp <= %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (vid, one_ago))
                prev = cur.fetchone()
                if prev:
                    hourly = r["views"] - prev["views"]
                processed.append((r["timestamp"][11:16], f"{r['views']:,}", f"+{gain:,}" if gain > 0 else "0", f"+{hourly:,}/hr"))
            daily[d] = processed
        videos.append({
            "video_id": vid,
            "name": row["name"],
            "daily_data": daily,
            "is_tracking": bool(row["is_tracking"])
        })
    return render_template("index.html", videos=videos)

@app.route("/toggle/<video_id>")
def toggle(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT is_tracking FROM video_list WHERE video_id=%s", (video_id,))
    current = cur.fetchone()["is_tracking"]
    cur.execute("UPDATE video_list SET is_tracking=%s WHERE video_id=%s", (0 if current else 1, video_id))
    flash("Paused" if current else "Resumed")
    return redirect(url_for("index"))

@app.route("/remove/<video_id>")
def remove(video_id):
    cur = get_db().cursor()
    cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
    cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
    flash("Removed")
    return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
    name = cur.fetchone()["name"]
    cur.execute("SELECT timestamp, views FROM views WHERE video_id=%s ORDER BY timestamp", (video_id,))
    df = pd.DataFrame([{"Time": r["timestamp"], "Views": r["views"]} for r in cur.fetchall()])
    fname = "export.xlsx"
    df.to_excel(fname, index=False)
    return send_file(fname, as_attachment=True, download_name=f"{name}_views.xlsx")

@app.route("/ping")
def ping():
    return "OK", 200

# === START ===
init_db()
start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
