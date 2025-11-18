# app.py — YouTube tracker + Google OAuth + manual signup review (copy-paste ready)
import os
import threading
import logging
import time
import math
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo
from typing import Optional

import pandas as pd
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, abort
)
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import psycopg
from psycopg.rows import dict_row

# Auth
from authlib.integrations.flask_client import OAuth
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user,
    current_user, login_required
)
from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, SubmitField
from wtforms.validators import DataRequired, Email

# -----------------------------
# App & logging
# -----------------------------
app = Flask(__name__, template_folder="templates")
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(24))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("yt-tracker")

IST = ZoneInfo("Asia/Kolkata")

# -----------------------------
# Env & OAuth
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

# Google OAuth config
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

oauth = OAuth(app)
if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'},
    )
else:
    log.warning("Google OAuth not configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET to enable sign-in.")

login_manager = LoginManager(app)
login_manager.login_view = "home"

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
          ts_utc    TIMESTAMPTZ NOT NULL,
          date_ist  DATE NOT NULL,
          views     BIGINT NOT NULL,
          likes     BIGINT,
          PRIMARY KEY (video_id, ts_utc),
          FOREIGN KEY (video_id) REFERENCES video_list(video_id) ON DELETE CASCADE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS targets (
          id           SERIAL PRIMARY KEY,
          video_id     TEXT NOT NULL REFERENCES video_list(video_id) ON DELETE CASCADE,
          target_views BIGINT NOT NULL,
          target_ts    TIMESTAMPTZ NOT NULL,
          note         TEXT
        );
        """)
        # Users & signup requests tables for auth & admin workflow
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY,
          email TEXT UNIQUE NOT NULL,
          name TEXT,
          google_id TEXT UNIQUE,
          is_approved BOOLEAN NOT NULL DEFAULT FALSE,
          is_admin BOOLEAN NOT NULL DEFAULT FALSE,
          signup_note TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          approved_at TIMESTAMPTZ
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS signup_requests (
          id SERIAL PRIMARY KEY,
          email TEXT NOT NULL,
          name TEXT,
          note TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          handled BOOLEAN NOT NULL DEFAULT FALSE,
          handled_by INT REFERENCES users(id),
          handled_at TIMESTAMPTZ
        );
        """)
    log.info("DB schema ready.")


# -----------------------------
# YouTube helpers (unchanged)
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
# Storage helpers (unchanged)
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


def interpolate_views_at(rows_asc: list[dict], target_ts: datetime) -> Optional[float]:
    """
    Return interpolated views at target_ts (tz-aware). If exact match exists return that value.
    If target_ts < first ts or > last ts return None.
    Interpolation between bracketing samples (linear in time).
    """
    if not rows_asc:
        return None
    # exact match
    for r in rows_asc:
        if r["ts_utc"] == target_ts:
            return float(r["views"])
    # find bracketing indices
    prev = None
    for r in rows_asc:
        if r["ts_utc"] < target_ts:
            prev = r
            continue
        # r.ts_utc >= target_ts
        if r["ts_utc"] > target_ts and prev is not None:
            t0 = prev["ts_utc"].timestamp()
            v0 = float(prev["views"])
            t1 = r["ts_utc"].timestamp()
            v1 = float(r["views"])
            if t1 == t0:
                return v1
            frac = (target_ts.timestamp() - t0) / (t1 - t0)
            return v0 + frac * (v1 - v0)
        # prev is None and r.ts_utc >= target_ts -> target before first sample
        if prev is None:
            return None
    # target after last sample -> None
    return None


def _time_to_seconds(time_str: str) -> int:
    h, m, s = [int(x) for x in time_str.split(":")]
    return h * 3600 + m * 60 + s


def find_closest_tpl(prev_map: dict, time_part: str, tolerance_seconds: int = 10):
    """
    prev_map: mapping time_str -> tpl
    time_part: 'HH:MM:SS' string to match
    returns tpl with minimal abs(second-diff) if within tolerance, else None
    """
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


def process_gains(rows_asc: list[dict]):
    """
    Input: rows ascending by ts_utc.
    Output: list of tuples:
      (ts_ist_str, views, gain_5min, hourly_gain, gain_24h)
    24h gain uses interpolation when possible, otherwise falls back to latest <= target.
    """
    out = []
    for i, r in enumerate(rows_asc):
        ts_utc = r["ts_utc"]
        ts_ist = ts_utc.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
        views = r["views"]
        gain = None if i == 0 else views - rows_asc[i - 1]["views"]

        # hourly: latest row <= ts - 1h
        target_h = ts_utc - timedelta(hours=1)
        ref_idx_h = None
        for j in range(i, -1, -1):
            if rows_asc[j]["ts_utc"] <= target_h:
                ref_idx_h = j
                break
        hourly = None if ref_idx_h is None else (views - rows_asc[ref_idx_h]["views"])

        # 24h: try interpolation first, fallback to latest <= target
        target_d = ts_utc - timedelta(days=1)
        interp = interpolate_views_at(rows_asc, target_d)
        if interp is None:
            ref_idx_d = None
            for j in range(i, -1, -1):
                if rows_asc[j]["ts_utc"] <= target_d:
                    ref_idx_d = j
                    break
            gain_24h = None if ref_idx_d is None else (views - rows_asc[ref_idx_d]["views"])
        else:
            gain_24h = views - int(round(interp))

        out.append((ts_ist, views, gain, hourly, gain_24h))
    return out


# -----------------------------
# Background sampler (unchanged)
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
# Auth: Flask-Login user loader & helper
# -----------------------------
class LocalUser(UserMixin):
    def __init__(self, id, email, name, is_admin=False, is_approved=False):
        self.id = str(id)
        self.email = email
        self.name = name
        self.is_admin = bool(is_admin)
        self.is_approved = bool(is_approved)


@login_manager.user_loader
def load_user(user_id):
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, email, name, is_admin, is_approved FROM users WHERE id=%s", (int(user_id),))
        r = cur.fetchone()
    if not r:
        return None
    return LocalUser(r["id"], r["email"], r["name"], r["is_admin"], r["is_approved"])


# -----------------------------
# Helper: build display data for one video (used by detail & export)
# same as earlier but now includes latest_ts_iso and latest_ts_ist in returned dict
# -----------------------------
def build_video_display(vid: str):
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT video_id, name, is_tracking FROM video_list WHERE video_id=%s", (vid,))
        vrow = cur.fetchone()
        if not vrow:
            return None

        cur.execute("SELECT ts_utc, views FROM views WHERE video_id=%s ORDER BY ts_utc ASC", (vid,))
        all_rows = cur.fetchall()

        if not all_rows:
            daily = {}
        else:
            processed_all = process_gains(all_rows)
            grouped = {}
            date_time_map = {}
            for tpl in processed_all:
                ts_ist = tpl[0]
                date_str, time_part = ts_ist.split(" ")
                grouped.setdefault(date_str, []).append(tpl)
                date_time_map.setdefault(date_str, {})[time_part] = tpl

            dates_sorted = sorted(grouped.keys(), reverse=True)
            daily = {}
            for date_str in dates_sorted:
                processed = grouped[date_str]
                prev_date_str = (datetime.fromisoformat(date_str).date() - timedelta(days=1)).isoformat()
                prev_map = date_time_map.get(prev_date_str, {})

                display_rows = []
                for tpl in processed:
                    ts_ist, views, gain_5min, hourly_gain, gain_24h = tpl
                    time_part = ts_ist.split(" ")[1]

                    prev_tpl_for_pct = prev_map.get(time_part)
                    if prev_tpl_for_pct is None:
                        prev_tpl_for_pct = find_closest_tpl(prev_map, time_part, tolerance_seconds=10)
                    prev_gain24_for_pct = prev_tpl_for_pct[4] if prev_tpl_for_pct else None

                    pct24 = None
                    if prev_gain24_for_pct not in (None, 0):
                        try:
                            pct24 = round(((gain_24h or 0) - prev_gain24_for_pct) / prev_gain24_for_pct * 100, 2)
                        except Exception:
                            pct24 = None

                    # exact yesterday 22:30 lookup for projection
                    projected = None
                    prev_2230_tpl = prev_map.get("22:30:00")
                    if prev_2230_tpl is not None and pct24 not in (None,):
                        prev_views_2230 = prev_2230_tpl[1]
                        prev_gain24_2230 = prev_2230_tpl[4]
                        if prev_views_2230 is not None and prev_gain24_2230 not in (None, 0):
                            try:
                                projected_val = prev_views_2230 + prev_gain24_2230 * (1 + (pct24 / 100.0))
                                projected = int(round(projected_val))
                            except Exception:
                                projected = None

                    display_rows.append((ts_ist, views, gain_5min, hourly_gain, gain_24h, pct24, projected))

                daily[date_str] = list(reversed(display_rows))

        latest_views = None
        latest_ts = None
        if all_rows:
            latest_views = all_rows[-1]["views"]
            latest_ts = all_rows[-1]["ts_utc"]

        # --- added: ISO + IST formatted strings for frontend badge ---
        latest_ts_iso = latest_ts.isoformat() if latest_ts is not None else None
        latest_ts_ist = latest_ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if latest_ts is not None else None

        # targets
        cur.execute("SELECT id, target_views, target_ts, note FROM targets WHERE video_id=%s ORDER BY target_ts ASC", (vid,))
        target_rows = cur.fetchall()
        nowu = now_utc()
        targets_display = []
        for t in target_rows:
            tid, t_views, t_ts, note = t["id"], t["target_views"], t["target_ts"], t["note"]
            remaining_views = (t_views - (latest_views or 0))
            remaining_seconds = (t_ts - nowu).total_seconds()
            if remaining_views <= 0:
                status = "reached"
                req_hr = req_5m = 0
            elif remaining_seconds <= 0:
                status = "overdue"
                req_hr = math.ceil(remaining_views)
                req_5m = math.ceil(req_hr / 12)
            else:
                status = "active"
                hrs = max(remaining_seconds / 3600.0, 1/3600)
                req_hr = math.ceil(remaining_views / hrs)
                req_5m = math.ceil(req_hr / 12)
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

    return {
        "video_id": vrow["video_id"],
        "name": vrow["name"],
        "is_tracking": bool(vrow["is_tracking"]),
        "daily": daily,
        "targets": targets_display,
        "latest_views": latest_views,
        "latest_ts": latest_ts,
        "latest_ts_iso": latest_ts_iso,
        "latest_ts_ist": latest_ts_ist
    }


# -----------------------------
# Routes: home + video detail + existing tracker features (unchanged)
# but templates slightly updated to show login UI
# -----------------------------
@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/")
def home():
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
        videos = cur.fetchall()
    vids = []
    for v in videos:
        vid = v["video_id"]
        thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
        short_title = v["name"] if len(v["name"]) <= 60 else v["name"][:57] + "..."
        vids.append({
            "video_id": vid,
            "name": v["name"],
            "short_title": short_title,
            "thumbnail": thumb,
            "is_tracking": bool(v["is_tracking"])
        })
    return render_template("home.html", videos=vids)


@app.get("/video/<video_id>")
def video_detail(video_id):
    info = build_video_display(video_id)
    if info is None:
        flash("Video not found.", "warning")
        return redirect(url_for("home"))
    info["thumbnail"] = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    return render_template("video_detail.html", v=info)


@app.post("/add_video")
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


@app.get("/remove_target/<int:target_id>")
@login_required
def remove_target(target_id):
    # only allow admin or the presence of an approved user? For now require admin
    if not current_user.is_admin:
        abort(403)
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


@app.get("/stop_tracking/<video_id>")
@login_required
def stop_tracking(video_id):
    # guard: require admin to pause/resume to avoid anonymous misuse
    if not current_user.is_admin:
        abort(403)
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT is_tracking, name FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "warning")
            return redirect(url_for("home"))
        new_state = not bool(row["is_tracking"])
        cur.execute("UPDATE video_list SET is_tracking=%s WHERE video_id=%s", (new_state, video_id))
        flash(("Resumed" if new_state else "Paused") + f" tracking: {row['name']}", "info")
    return redirect(url_for("video_detail", video_id=video_id))


@app.get("/remove_video/<video_id>")
@login_required
def remove_video(video_id):
    # require admin
    if not current_user.is_admin:
        abort(403)
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
def export_video(video_id):
    info = build_video_display(video_id)
    if info is None:
        flash("Video not found.", "warning")
        return redirect(url_for("home"))

    # Build DataFrame from info['daily'] (chronological ascending)
    rows_for_df = []
    dates = sorted(info["daily"].keys())
    for date in dates:
        # info['daily'][date] is newest-first, reverse to chronological
        day_rows = list(reversed(info["daily"][date]))
        for tpl in day_rows:
            ts, views, gain5, hourly, gain24, pct24, projected = tpl
            rows_for_df.append({
                "Time (IST)": ts,
                "Views": views,
                "Gain (5 min)": gain5 if gain5 is not None else "",
                "Hourly Growth": hourly if hourly is not None else "",
                "Gain (24 h)": gain24 if gain24 is not None else "",
                "Change 24h vs prev day (%)": pct24 if pct24 is not None else "",
                "Projected (min) views": projected if projected is not None else ""
            })

    df_views = pd.DataFrame(rows_for_df)

    # targets sheet
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
        latest_views = info["latest_views"]
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

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df_views.to_excel(writer, index=False, sheet_name="Views")
        if not df_targets.empty:
            df_targets.to_excel(writer, index=False, sheet_name="Targets")
    bio.seek(0)

    safe = "".join(c for c in info["name"] if c.isalnum() or c in " _-").rstrip()
    return send_file(
        bio,
        as_attachment=True,
        download_name=f"{safe or 'export'}_views.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# -----------------------------
# OAuth & Signup flows
# -----------------------------
@app.get("/login/google")
def login_google():
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        flash("Google login not configured.", "warning")
        return redirect(url_for("home"))
    redirect_uri = url_for('auth_google_callback', _external=True)
    return oauth.google.authorize_redirect(redirect_uri)


@app.get("/auth/google/callback")
def auth_google_callback():
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        flash("Google login not configured.", "warning")
        return redirect(url_for("home"))

    token = oauth.google.authorize_access_token()
    if not token:
        flash("Failed to get token from Google.", "danger")
        return redirect(url_for("home"))
    userinfo = oauth.google.parse_id_token(token)
    if not userinfo:
        flash("Failed to fetch Google userinfo.", "danger")
        return redirect(url_for("home"))

    google_id = userinfo.get("sub")
    email = userinfo.get("email")
    name = userinfo.get("name") or email.split("@")[0]

    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, is_approved, is_admin FROM users WHERE google_id=%s OR email=%s", (google_id, email))
        r = cur.fetchone()
        if r:
            uid, is_approved, is_admin = r["id"], r["is_approved"], r["is_admin"]
            cur.execute("""
              UPDATE users SET google_id = COALESCE(google_id, %s), name = COALESCE(name, %s), email = %s
              WHERE id=%s
            """, (google_id, name, email, uid))
        else:
            # create unapproved user by default (admin must approve)
            cur.execute("""
              INSERT INTO users (email, name, google_id, is_approved)
              VALUES (%s, %s, %s, FALSE) RETURNING id, is_approved, is_admin
            """, (email, name, google_id))
            r = cur.fetchone()
            uid, is_approved, is_admin = r["id"], r["is_approved"], r["is_admin"]

    if is_approved:
        user = LocalUser(uid, email, name, bool(is_admin), bool(is_approved))
        login_user(user)
        flash("Logged in.", "success")
        return redirect(url_for("home"))
    else:
        flash("Your account is pending approval. An admin will review your request.", "info")
        return redirect(url_for("home"))


@app.get("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("home"))


# Manual signup (request) form using Flask-WTF
class SignupRequestForm(FlaskForm):
    name = StringField("Full name", validators=[DataRequired()])
    email = StringField("Email", validators=[DataRequired(), Email()])
    note = TextAreaField("Reason for access (optional)")
    submit = SubmitField("Request access")


@app.route("/signup", methods=["GET", "POST"])
def signup_request():
    form = SignupRequestForm()
    if form.validate_on_submit():
        email = form.email.data.strip().lower()
        name = form.name.data.strip()
        note = form.note.data.strip()
        conn = db()
        with conn.cursor() as cur:
            # avoid duplicate pending requests
            cur.execute("SELECT id FROM signup_requests WHERE email=%s AND handled=FALSE", (email,))
            if cur.fetchone():
                flash("A pending request already exists for this email.", "warning")
                return redirect(url_for("home"))
            cur.execute("INSERT INTO signup_requests (email, name, note) VALUES (%s,%s,%s)", (email, name, note))
        flash("Signup request submitted — an admin will review it.", "success")
        return redirect(url_for("home"))
    return render_template("signup_request.html", form=form)


# Admin: view pending requests
@app.get("/admin/requests")
@login_required
def admin_requests():
    if not current_user.is_admin:
        abort(403)
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT id, email, name, note, created_at FROM signup_requests WHERE handled=FALSE ORDER BY created_at")
        pending = cur.fetchall()
    return render_template("admin_requests.html", pending=pending)


@app.post("/admin/approve_request/<int:req_id>")
@login_required
def approve_request(req_id):
    if not current_user.is_admin:
        abort(403)
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT email, name FROM signup_requests WHERE id=%s AND handled=FALSE", (req_id,))
        r = cur.fetchone()
        if not r:
            flash("Request not found or already handled.", "warning")
            return redirect(url_for("admin_requests"))
        email, name = r["email"], r["name"]
        # create user and approve
        cur.execute("INSERT INTO users (email, name, is_approved, approved_at) VALUES (%s,%s,TRUE,now()) RETURNING id", (email, name))
        new_id = cur.fetchone()["id"]
        cur.execute("UPDATE signup_requests SET handled=TRUE, handled_by=%s, handled_at=now() WHERE id=%s", (current_user.id, req_id))
    flash(f"Request approved — user created (id {new_id}).", "success")
    # TODO: send email to the user (optional)
    return redirect(url_for("admin_requests"))


@app.post("/admin/decline_request/<int:req_id>")
@login_required
def decline_request(req_id):
    if not current_user.is_admin:
        abort(403)
    conn = db()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM signup_requests WHERE id=%s AND handled=FALSE", (req_id,))
        if not cur.fetchone():
            flash("Request not found or already handled.", "warning")
            return redirect(url_for("admin_requests"))
        cur.execute("UPDATE signup_requests SET handled=TRUE, handled_by=%s, handled_at=now() WHERE id=%s", (current_user.id, req_id))
    flash("Request declined.", "info")
    return redirect(url_for("admin_requests"))


# -----------------------------
# Boot
# -----------------------------
init_db()
start_background()

if __name__ == "__main__":
    # helpful startup logging
    log.info("Starting app with Google OAuth %s", "enabled" if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET else "disabled")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
