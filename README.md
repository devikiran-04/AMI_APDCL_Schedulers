 Here's the complete README code ready to copy-paste:

```markdown
# APDCL Schedule Monitor

A production-grade Python worker that monitors job schedules from `public.SCH_JOB_DEF` table and alerts when scheduled jobs miss their trigger windows.

---

## Features

| Feature | Description |
|---------|-------------|
| **Multi-App Support** | Monitors MDMS, MWM, SPM_BILLING, UHES (configurable) |
| **Grace Period Detection** | Configurable grace period before marking schedule as missed |
| **Repeat Alerts** | Re-alerts every N minutes while schedule remains missed |
| **Email Notifications** | SMTP-enabled email alerts with TLS encryption |
| **Timezone Aware** | Full timezone support (default: Asia/Kolkata) |
| **State Filtering** | Respects ENABLED/DISABLED/PAUSED/INACTIVE states |
| **Self-Healing** | Auto-clears alerts when schedules recover or nextfiretime updates |

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  config.env     │────▶│ schedule_monitor│────▶│  PostgreSQL DB  │
│  (settings)     │     │    (worker)     │     │  SCH_JOB_DEF    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌─────────────────┐
                        │  SMTP Server    │
                        │ (Email Alerts)  │
                        └─────────────────┘
```

---

## Quick Start

### 1. Prerequisites

```bash
# Python 3.8+
pip install pandas sqlalchemy python-dotenv pytz psycopg2-binary
```

### 2. Configuration

Create `config.env` in the same directory:

```ini
# Database
DB_HOST=your-db-host
DB_PORT=5432
DB_NAME=apdcl_prod
DB_USER=monitor_user
DB_PASSWORD=your-password

# Timezone
TZ=Asia/Kolkata

# Monitoring Settings
MASTERCONFIG_APPIDS=MDMS,MWM,SPM_BILLING,UHES
GRACE_MINUTES=10
POLL_INTERVAL_SECONDS=60
ALERT_REPEAT_MINUTES=10

# State Filters
DISABLED_STATES=DISABLED,PAUSED,INACTIVE
ENABLED_STATES=ENABLED,ACTIVE,SCHEDULED

# Email Alerts (Optional)
ALERT_EMAIL_ENABLED=true
SMTP_HOST=smtp.office365.com
SMTP_PORT=587
SMTP_USER=alerts@yourdomain.com
SMTP_PASSWORD=your-smtp-password
ALERT_EMAIL_TO=ops-team@yourdomain.com,admin@yourdomain.com
ALERT_EMAIL_FROM=apdcl-monitor@yourdomain.com
ALERT_EMAIL_SUBJECT_PREFIX=[APDCL PROD] Schedule missed trigger
```

### 3. Run

```bash
python schedule_monitor.py
```

---

## How It Works

### Missed Schedule Detection Logic

```
NOW = current time
NEXT = schedule.nextfiretime
GRACE = GRACE_MINUTES (default 10m)

IF NEXT > NOW:
    → Schedule is healthy (future)
    
IF NEXT <= NOW AND (NOW - NEXT) < GRACE:
    → Within grace, wait...
    
IF NEXT <= NOW AND (NOW - NEXT) >= GRACE:
    → MISSED! Trigger alert
```

### Alert Lifecycle

| Scenario | Action |
|----------|--------|
| First miss | Send immediate alert |
| Still missed after 10m | Send repeat alert |
| Schedule recovers (next_run > now) | Clear alert state |
| `nextfiretime` changes | Reset alert for new window |
| Job disabled/deleted | Remove from tracking |

---

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `MASTERCONFIG_APPIDS` | MDMS,MWM,SPM_BILLING,UHES | Comma-separated app IDs to monitor |
| `GRACE_MINUTES` | 10 | Minutes to wait before declaring missed |
| `POLL_INTERVAL_SECONDS` | 60 | DB poll frequency |
| `ALERT_REPEAT_MINUTES` | 10 | Minutes between repeat alerts |
| `DISABLED_STATES` | DISABLED,PAUSED,INACTIVE | States to ignore |
| `ENABLED_STATES` | ENABLED,ACTIVE,SCHEDULED | States to monitor |

---

## Database Schema Expected

```sql
-- Target table: public.SCH_JOB_DEF
CREATE TABLE public.SCH_JOB_DEF (
    id              BIGINT PRIMARY KEY,
    appid           VARCHAR(50),
    name            VARCHAR(255),
    nextfiretime    TIMESTAMP,
    state           VARCHAR(50),
    triggerexp      TEXT,
    schstartdate    TIMESTAMP,
    schenddate      TIMESTAMP,
    crondesc        VARCHAR(255),
    orgid           VARCHAR(50)
);
```

---

## Running as Systemd Service

Create `/etc/systemd/system/apdcl-schedule-monitor.service`:

```ini
[Unit]
Description=APDCL Schedule Monitor Worker
After=network.target

[Service]
Type=simple
User=apdcl
WorkingDirectory=/opt/apdcl/monitor
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/apdcl/monitor/venv/bin/python schedule_monitor.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable apdcl-schedule-monitor
sudo systemctl start apdcl-schedule-monitor
sudo systemctl status apdcl-schedule-monitor
```

---

## Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY config.env .

CMD ["python", "schedule_monitor.py"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  monitor:
    build: .
    container_name: apdcl-schedule-monitor
    restart: unless-stopped
    env_file:
      - config.env
    volumes:
      - ./logs:/app/logs
```

---

## Log Output Example

```
Starting Schedule Monitor | AppIDs=['MDMS', 'MWM', 'SPM_BILLING', 'UHES'] | 
Poll=60s | Grace=10m | Repeat=10m | TZ=Asia/Kolkata

[ALERT] Missed trigger: [MDMS] BILL_GENERATION_JOB (ID=1521)  
        Next Run: 2025-04-15 14:30:00 IST+0530
        
[REPEAT] Missed trigger: [MDMS] BILL_GENERATION_JOB (ID=1521)  
         Next Run: 2025-04-15 14:30:00 IST+0530
         
[INFO] No schedules found for: MDMS, MWM, SPM_BILLING, UHES
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `DB_PORT is None` | Check `config.env` exists and is readable |
| No alerts firing | Verify `ALERT_EMAIL_ENABLED=true` and SMTP credentials |
| Wrong timezone | Set `TZ` variable (e.g., `America/New_York`) |
| Too many alerts | Increase `GRACE_MINUTES` or `ALERT_REPEAT_MINUTES` |
| Memory growth | Alert state auto-cleans; ensure `nextfiretime` updates in DB |

---

## File Structure

```
.
├── schedule_monitor.py   # Main worker logic
├── db.py                 # SQLAlchemy engine factory
├── config.env            # Environment configuration (not in git)
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

---

## License

Internal APDCL Use Only

```
