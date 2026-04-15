#!/usr/bin/env python3
# APDCL Schedule Monitor Worker
#
# Monitors schedules from public.SCH_JOB_DEF for these app IDs:
# MDMS, MWM, SPM_BILLING, UHES
#
# Conditions:
# ✔ Only enabled schedules
# ✔ nextfiretime must be > now (future)
# ✔ If nextfiretime <= now - grace → MISSED schedule
# ✔ Email-only alerting
#
# Configuration from config.env

import os
import time
from datetime import datetime, timedelta

import pytz
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

# Local db loader
from db import get_engine


# ---------------------------------------------------
# Load Environment
# ---------------------------------------------------
load_dotenv("config.env")

LOCAL_TZ = pytz.timezone(os.getenv("TZ", "Asia/Kolkata"))

APPIDS_RAW = os.getenv("MASTERCONFIG_APPIDS", "MDMS,MWM,SPM_BILLING,UHES")
APPIDS = []
for s in APPIDS_RAW.split(","):
    s = s.strip()
    if not s:
        continue
    safe = "".join(ch for ch in s if ch.isalnum() or ch == "_")
    if safe:
        APPIDS.append(safe.upper())

GRACE_MINUTES = int(os.getenv("GRACE_MINUTES", "10"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# NEW: Repeat alert interval (minutes)
ALERT_REPEAT_MINUTES = int(os.getenv("ALERT_REPEAT_MINUTES", "10"))

# Email
ALERT_EMAIL_ENABLED = os.getenv("ALERT_EMAIL_ENABLED", "false").lower() == "true"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = [e.strip() for e in os.getenv("ALERT_EMAIL_TO", "").split(",") if e.strip()]
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "")
ALERT_EMAIL_SUBJECT_PREFIX = os.getenv("ALERT_EMAIL_SUBJECT_PREFIX", "[APDCL PROD] Schedule missed trigger")

# State filters
DISABLED_STATES = {s.strip().upper() for s in os.getenv("DISABLED_STATES", "DISABLED,PAUSED,INACTIVE").split(",")}
ENABLED_STATES  = {s.strip().upper() for s in os.getenv("ENABLED_STATES", "ENABLED,ACTIVE,SCHEDULED").split(",")}


# ---------------------------------------------------
# SQL
# ---------------------------------------------------
appid_list_sql = ", ".join(f"'{a}'" for a in APPIDS)

SCHEDULE_DEF_SQL = text(f"""
SELECT id, appid, name, nextfiretime, state, triggerexp,
       schstartdate, schenddate, crondesc, orgid
FROM public.SCH_JOB_DEF
WHERE appid IN ({appid_list_sql})
ORDER BY appid, name
""")


# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def tz_now():
    return datetime.now(LOCAL_TZ)


def parse_ts(ts):
    """Convert DB timestamp into tz-aware datetime."""
    if ts is None:
        return None
    try:
        dt = pd.to_datetime(ts)
    except Exception:
        return None

    if dt.tzinfo is None:
        return LOCAL_TZ.localize(dt.to_pydatetime())
    return dt.tz_convert(LOCAL_TZ).to_pydatetime()


def is_enabled_state(state):
    if not state:
        return False
    s = str(state).strip().upper()
    if s in DISABLED_STATES:
        return False
    if s in ENABLED_STATES:
        return True
    # default
    return s not in {"", "NONE", "UNKNOWN"}


def within_schedule_window(start, end, now):
    if start and now < start:
        return False
    if end and now > end:
        return False
    return True


def send_alert_console(row, next_run, repeated=False):
    tag = "REPEAT" if repeated else "ALERT"
    print(
        f"[{tag}] Missed trigger: [{row['appid']}] {row['name']} (ID={row['id']})  "
        f"Next Run: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )


def send_alert_email(row, next_run, repeated=False):
    if not (ALERT_EMAIL_ENABLED and SMTP_USER and SMTP_PASSWORD and ALERT_EMAIL_TO and ALERT_EMAIL_FROM):
        return

    try:
        import smtplib
        from email.message import EmailMessage

        repeat_tag = " (repeat)" if repeated else ""
        msg = EmailMessage()
        msg["Subject"] = f"{ALERT_EMAIL_SUBJECT_PREFIX}{repeat_tag}: {row['name']} ({row['id']})"
        msg["From"] = ALERT_EMAIL_FROM
        msg["To"] = ", ".join(ALERT_EMAIL_TO)

        body = f"""Schedule missed trigger in APDCL PROD{repeat_tag}
- App ID   : {row['appid']}
- Job ID   : {row['id']}
- Job Name : {row['name']}
- Next Run : {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}
- Checked  : {tz_now().strftime('%Y-%m-%d %H:%M:%S %Z')}
"""

        msg.set_content(body)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASSWORD)
            smtp.send_message(msg)

    except Exception as e:
        print(f"[ALERT ERROR] Email failed: {e}")


# ---------------------------------------------------
# Monitoring logic
# ---------------------------------------------------
def monitor_once(engine, alerts):
    """
    alerts: dict keyed by (job_id, next_run_iso) -> {"last_alert_at": datetime_tz}
    - Re-alert every ALERT_REPEAT_MINUTES while still missed.
    - Clear entries when schedule becomes healthy or nextfiretime changes (key changes).
    """
    now = tz_now()
    grace = timedelta(minutes=GRACE_MINUTES)
    repeat_delta = timedelta(minutes=ALERT_REPEAT_MINUTES)

    with engine.connect() as conn:
        df = pd.read_sql(SCHEDULE_DEF_SQL, conn)

    if df.empty:
        print("[INFO] No schedules found for:", ", ".join(APPIDS))
        return

    # Normalize timestamps
    for col in ["nextfiretime", "schstartdate", "schenddate"]:
        if col in df.columns:
            df[col] = df[col].map(parse_ts)

    # Track keys seen this iteration so we can garbage-collect old ones
    seen_keys = set()

    for _, r in df.iterrows():

        # Enabled state check
        if not is_enabled_state(r.get("state")):
            continue

        next_run = r.get("nextfiretime")
        start = r.get("schstartdate")
        end = r.get("schenddate")

        if not next_run:
            continue

        # Window check
        if not within_schedule_window(start, end, now):
            continue

        key = (str(r.get("id")), next_run.isoformat())
        seen_keys.add(key)

        # If future schedule → healthy → ensure any stale alerts for this job+old next_run are ignored automatically
        if next_run > now:
            # Healthy → nothing to do
            continue

        # If next_run <= now → schedule should have fired already
        # If exceeded grace period → MISSED
        if now >= next_run + grace:
            # Re-alert policy: if never alerted OR last alert older than repeat interval -> alert
            last = alerts.get(key, {}).get("last_alert_at")
            if (last is None) or (now - last >= repeat_delta):
                repeated = last is not None
                send_alert_console(r, next_run, repeated=repeated)
                send_alert_email(r, next_run, repeated=repeated)
                alerts[key] = {"last_alert_at": now}

    # GC: remove alert entries that weren't seen this round (e.g., nextfiretime changed or job deleted)
    # This ensures a fresh alert will fire for a new missed window.
    stale_keys = [k for k in alerts.keys() if k not in seen_keys]
    for k in stale_keys:
        alerts.pop(k, None)


# ---------------------------------------------------
# Main
# ---------------------------------------------------
def main():
    print(
        f"Starting Schedule Monitor | AppIDs={APPIDS} | Poll={POLL_INTERVAL_SECONDS}s | "
        f"Grace={GRACE_MINUTES}m | Repeat={ALERT_REPEAT_MINUTES}m | TZ={LOCAL_TZ.zone}"
    )

    engine = get_engine()
    # CHANGED: dict instead of set
    alerts = {}

    while True:
        try:
            monitor_once(engine, alerts)
        except Exception as e:
            print("[ERROR]", e)
            time.sleep(10)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()