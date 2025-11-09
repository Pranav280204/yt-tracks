import os
import threading
import logging
import time
from datetime import datetime, timedelta, timezone
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
# App & logging
# -----------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(24))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("yt-tracker")

IST = ZoneInfo("Asia/Kolkata")

# -----------------------------
# Env
# -----------------------------
API_KEY = os.getenv("YOUTUBE_API_KEY", "")
POSTGRES_URL = os.getenv("DATABASE_URL")
if not POSTGRES_URL:
    log.warning("DATABASE_URL not set.")

YOUTUBE = None
if API_KEY:
    try:
        YOUTUBE = build("youtube", "v3", developerKey=API_KEY, cache_discovery=False)
        log.info("YouTube client ready.")
    except Exception as e:
        log.exception("YouTube init failed: %s", e)

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
    """Schema: store timestamps in UTC (timestamptz). Compute IST date on insert."""
    conn = db()
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS video_list (
          video_id    TEXT PRIMARY KEY,
          name        TEXT NOT NULL,
          is_tracking BOOLEAN NOT NULL DEFAULT TRUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS views (
          video_id  TEXT NOT NULL,
          ts_utc    TIMESTAMPTZ NOT NULL,   -- canonical storage in UTC
          date_ist  DATE NOT NULL,          -- convenience for day grouping
          views     BIGINT NOT NULL,
          likes     BIGINT,
          PRIMARY KEY (video_id, ts_utc),
          FOREIGN KEY (video_id) REFERENCES video_list(video_id) ON DELETE CASCADE
        );
        """)
    log.info("DB schema ready.")

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
        return (items[0]["snippet"]["title"] if items else "Unknown")[:100] or "Unknown"
    except HttpError as e:
        log.error("YouTube (title) %s: %s", video_id, e)
        return "Unknown"
    except Exception as e:
        log.exception("Title fetch error: %s", e)
        return "Unknown"

def fetch_stats_batch(video_ids: list[str]) -> dict:
    if not YOUTUBE or not video_ids:
        return {}
    out = {}
    try:
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i+50]
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
    """
    Store a sample at second precision. UTC in DB, IST date computed.
    """
    tsu = now_utc()
    date_ist = tsu.astimezone(IST).date()
    conn = db()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM views WHERE video_id=%s AND ts_utc=%s", (video_id, tsu))
        cur.execute(
            "INSERT INTO views (video_id, ts_utc, date_ist, views, likes) VALUES (%s, %s, %s, %s, %s)",
            (video_id, tsu, date_ist, int(stats.get("views", 0)), stats.get("likes"))
        )

def process_gains(rows_asc: list[dict]) -> list[tuple[str, int, int | None, int | None]]:
    """
    Input: rows ascending by ts_utc (chronological).
    Output rows (still chronological) with (ts_ist_str, views, gain_5min, hourly_gain).
    """
    out = []
    for i, r in enumerate(rows_asc):
        ts_ist = r["ts_utc"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
        views = r["views"]
        gain = None if i == 0 else views - rows_asc[i-1]["views"]

        # hourly gain vs latest row <= ts - 1h
        target = r["ts_utc"] - timedelta(hours=1)
        ref_idx = None
        for j in range(i, -1, -1):
            if rows_asc[j]["ts_utc"] <= target:
                ref_idx = j
                break
        hourly = None if ref_idx is None else (views - rows_asc[ref_idx]["views"])
        out.append((ts_ist, views, gain, hourly))
    return out

# -----------------------------
# Background sampler
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
    log.info("Sampler loop started (aligned to IST 5-min).")
    while True:
        try:
            sleep_until_next_5min_IST()
            conn = db()
            with conn.cursor() as cur:
                cur.execute("SELECT video_id FROM video_list WHERE is_tracking=TRUE ORDER BY video_id")
                vids = [r["video_id"] for r in cur.fetchall()]
            if not vids:
                continue
            stats_map = fetch_stats_batch(vids)
            for vid in vids:
                st = stats_map.get(vid)
                if st:
                    safe_store(vid, st)
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
# Routes
# -----------------------------
@app.get("/healthz")
def healthz():
    return "ok", 200

@app.get("/")
def index():
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
        videos = cur.fetchall()

    enriched = []
    with conn.cursor() as cur:
        for v in videos:
            vid = v["video_id"]
            # latest dates first
            cur.execute(
                "SELECT DISTINCT date_ist FROM views WHERE video_id=%s ORDER BY date_ist DESC",
                (vid,)
            )
            dates = [r["date_ist"] for r in cur.fetchall()]
            daily = {}
            for d in dates:
                # get rows ASC to compute gains correctly
                cur.execute(
                    "SELECT ts_utc, views FROM views WHERE video_id=%s AND date_ist=%s ORDER BY ts_utc ASC",
                    (vid, d)
                )
                asc_rows = cur.fetchall()
                if not asc_rows:
                    continue
                processed = process_gains(asc_rows)  # chronological
                # display newest first
                daily[d.strftime("%Y-%m-%d")] = list(reversed(processed))
            enriched.append({
                "video_id": vid,
                "name": v["name"],
                "is_tracking": bool(v["is_tracking"]),
                "daily_data": daily
            })
    return render_template("index.html", videos=enriched)

@app.post("/add_video")
def add_video():
    link = (request.form.get("link") or "").strip()
    if not link:
        flash("Paste a YouTube link.", "warning")
        return redirect(url_for("index"))
    video_id = extract_video_id(link)
    if not video_id:
        flash("Invalid YouTube link.", "danger")
        return redirect(url_for("index"))

    title = fetch_title(video_id)
    stats = fetch_stats_batch([video_id]).get(video_id)
    if not stats:
        flash("Could not fetch stats (check API key/quota/video).", "danger")
        return redirect(url_for("index"))

    conn = db()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO video_list (video_id, name, is_tracking)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (video_id) DO UPDATE SET name=EXCLUDED.name, is_tracking=TRUE
        """, (video_id, title))
    safe_store(video_id, stats)

    flash(f"Now tracking: {title}", "success")
    return redirect(url_for("index"))

@app.get("/stop_tracking/<video_id>")
def stop_tracking(video_id):
    conn = db()
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

@app.get("/remove_video/<video_id>")
def remove_video(video_id):
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("index"))
        name = row["name"]
        cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
        cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
        flash(f"Removed '{name}' and all data.", "success")
    return redirect(url_for("index"))

@app.get("/export/<video_id>")
def export_video(video_id):
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("index"))
        name = row["name"]
        cur.execute("SELECT ts_utc, views FROM views WHERE video_id=%s ORDER BY ts_utc ASC", (video_id,))
        data = cur.fetchall()
    if not data:
        flash("No data to export yet.", "warning")
        return redirect(url_for("index"))

    df = pd.DataFrame([{
        "Time (IST)": r["ts_utc"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S"),
        "Views": r["views"]
    } for r in data])

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Views")
    bio.seek(0)

    safe = "".join(c for c in name if c.isalnum() or c in " _-").rstrip()
    return send_file(
        bio,
        as_attachment=True,
        download_name=f"{safe or 'export'}_views.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Bootstrap
init_db()
start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
