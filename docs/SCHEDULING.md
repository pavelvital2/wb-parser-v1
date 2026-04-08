# Scheduling Guide (V1)

This document describes production V1 scheduling for:
- Windows Task Scheduler (primary)
- Linux cron/systemd timer (portable adaptation)

## 1. Commands to Schedule
Daily pipeline:
```powershell
py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run daily
```

Monthly pipeline:
```powershell
py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run monthly
```

## 2. Required Runtime Context
Working directory:
- `C:\parser_new`

Required env variables for scheduled user session:
- `WB_COOKIE_FILE`
- `WEBUI_ADMIN_PASSWORD` (only needed if same account also runs Web UI)
- `WEBUI_SECRET_KEY` (only needed if same account also runs Web UI)

## 3. Windows Task Scheduler (GUI)
Create two tasks:

### Task A: Daily pipeline
- Name: `WB Parser Daily`
- Trigger: Daily, 00:00
- Action:
  - Program/script: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
  - Add arguments:
    ```powershell
    -NoProfile -ExecutionPolicy Bypass -Command "$env:WB_COOKIE_FILE='C:\parser_new\config\wb_cookie.txt'; Set-Location 'C:\parser_new'; & py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run daily *> C:\parser_new\data\logs\scheduler_daily.log"
    ```
- Start in: `C:\parser_new`

### Task B: Monthly pipeline
- Name: `WB Parser Monthly`
- Trigger: Monthly, day 1, 00:00
- Action:
  - Program/script: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
  - Add arguments:
    ```powershell
    -NoProfile -ExecutionPolicy Bypass -Command "$env:WB_COOKIE_FILE='C:\parser_new\config\wb_cookie.txt'; Set-Location 'C:\parser_new'; & py C:\parser_new\main.py --config C:\parser_new\config\config.yaml run monthly *> C:\parser_new\data\logs\scheduler_monthly.log"
    ```
- Start in: `C:\parser_new`

Recommended task options:
- "Run whether user is logged on or not"
- "Run with highest privileges"
- If task is already running: `Do not start a new instance`

Note:
- Pipeline-level singleton lock (`state/locks/pipeline.lock`) also prevents conflicting parallel starts.

## 4. Verification Checklist (Windows)
After first trigger run, verify:
1. Task Scheduler `Last Run Result` is success.
2. New row exists in `py C:\parser_new\main.py --config C:\parser_new\config\config.yaml runs --limit 5`.
3. `C:\parser_new\state\run_reports\latest.json` was updated.
4. Output files updated in `data/marts/*/latest`.
5. `scheduler_daily.log` or `scheduler_monthly.log` has no critical traceback.

## 5. Linux Adaptation (Short)
Use the same CLI commands.

### cron example
```bash
# daily 00:00
0 0 * * * cd /opt/parser_new && WB_COOKIE_FILE=/opt/parser_new/config/wb_cookie.txt /usr/bin/python3 main.py --config config/config.yaml run daily >> data/logs/cron_daily.log 2>&1

# monthly day 1 00:00
0 0 1 * * cd /opt/parser_new && WB_COOKIE_FILE=/opt/parser_new/config/wb_cookie.txt /usr/bin/python3 main.py --config config/config.yaml run monthly >> data/logs/cron_monthly.log 2>&1
```

### systemd timer note
- Create two services calling the same commands.
- Attach matching timers (`OnCalendar=daily` and `OnCalendar=*-*-01 00:00:00`).
- Keep the same working directory and env vars.

## 6. Collision Safety
Even with scheduler rules, keep lock enabled in config:
- `runtime.locking_enabled: true`

If a run crashes and lock remains stale, stale handling is controlled by:
- `runtime.lock_stale_seconds`
