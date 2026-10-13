# Deploy SeatRush on Neon + Render (no Docker on your PC)

You need free accounts on **GitHub**, **Neon** and **Render**. For the optional load test you also need **Python 3.10+** on your PC.
Nothing else is installed locally.

How it fits together: one Render web service runs the API **and** the background loops (`RUN_WORKER_IN_API=1`, because Render
background workers are not free). Render Key Value (free) is Redis. Neon is Postgres.

## 1. Neon (database)
1. Go to console.neon.tech and create a project. Pick a region close to where you will put Render.
2. Click **Connect**. Copy two connection strings:
   - **Pooled** (the host contains `-pooler`) -> will be `DATABASE_URL`
   - **Direct** (turn "Connection pooling" off) -> will be `DIRECT_DATABASE_URL`
   Keep them somewhere safe; they contain your password. A `channel_binding` parameter in them is fine, the app strips it.

## 2. GitHub (put the code online)
Create a new empty repository named `seatrush` on github.com. Then either:

**Option A: Git in PowerShell** (Git for Windows required)
```powershell
Expand-Archive $HOME\Downloads\seatrush.zip -DestinationPath $HOME\projects
cd $HOME\projects\seatrush
git init
git add .
git commit -m "SeatRush"
git branch -M main
git remote add origin https://github.com/<your-username>/seatrush.git
git push -u origin main
```
**Option B: no Git** - unzip, open the empty repo on github.com, choose **Add file -> Upload files**, drag in the *contents* of the
`seatrush` folder (`app`, `loadtest`, `tests`, `docs`, `Dockerfile`, `render.yaml`, ...), and commit.

## 3. Render (hosting)
1. dashboard.render.com -> **New -> Blueprint** -> connect GitHub -> select the `seatrush` repo.
2. Render reads `render.yaml` and proposes a web service plus a Key Value instance. When asked, paste:
   - `DATABASE_URL` = the Neon **pooled** string
   - `DIRECT_DATABASE_URL` = the Neon **direct** string
3. Click **Apply / Deploy**. The first build takes a few minutes. When the service shows **Live**, open
   `https://<service-name>.onrender.com/docs`.

If the Blueprint is rejected, create them by hand: **Key Value** (Free, same region, eviction policy `noeviction`), then a **Web Service**
(Runtime: Docker, Free, health check path `/health`) with environment variables `DATABASE_URL`, `DIRECT_DATABASE_URL`,
`REDIS_URL` (the Key Value *internal* URL), `RUN_WORKER_IN_API=1`, `AUTO_SEED=1`, `DB_STATEMENT_CACHE=0`, `DB_POOL_MAX=10`,
`REDIS_MAX_CONN=30`, `SEED_SEATS=2000`.

## 4. Check it works
Open `docs/RUNBOOK.pdf` and follow it from the top. Quick version in PowerShell:
```powershell
$u = "https://<your-service>.onrender.com"
Invoke-RestMethod "$u/health"
Invoke-RestMethod "$u/events"
Invoke-RestMethod "$u/events/1/availability"
```
Expected: `status ok`, two events, `available 2000`. The first request after idle takes up to a minute (free tier waking up).

Load test (needs Python):
```powershell
cd $HOME\projects\seatrush
python -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
pip install httpx pytest
python loadtest\flash_sale.py --base-url $u --event 2 --users 200 --concurrency 30
```
Expected last line: `RESULT: PASS - zero oversells, ledger reconciles`.

## 5. Updating the app later
Change a file, then `git add .`, `git commit -m "change"`, `git push`. Render redeploys automatically.

## 6. Reset the data
In the Neon **SQL Editor** run:
```sql
TRUNCATE notifications, outbox, orders, holds, seats, events RESTART IDENTITY CASCADE;
```
Then in Render open the service and choose **Manual Deploy -> Restart** so it reseeds the seats.
(Optional) clear Redis: Render -> Key Value -> Connect -> `FLUSHALL`.

## Free-tier gotchas
- The web service **sleeps after ~15 minutes idle** (about a minute to wake). Call `/health` before a demo. The background loops only run while it is awake.
- Free Key Value is **25 MB, no persistence**: queue state and the payment ledger reset on restart.
- The free web service has **0.1 CPU**: use 100-500 users in the load test, not 20,000. Latency is far higher than on a paid instance.
- Neon scales to zero, so the first query after idle is slower.
- `DB_STATEMENT_CACHE=0` is required behind Neon's pooler; seeding uses the direct URL because advisory locks are session-scoped.
- `/admin/stats/*` is public and read-only. Add a token before sharing the URL widely.

## Troubleshooting
| You see | Likely cause |
|---|---|
| `Not Found` at the site root or `/docs/health` | Wrong address; use `/docs` and `/health` |
| 500 on `/events` | Neon strings wrong: re-copy both into Render environment variables |
| 500 on seats/holds, logs say `Too many connections` or Redis errors | `REDIS_URL` missing or wrong |
| Never admitted to the waiting room | Service asleep or `RUN_WORKER_IN_API` not `1`; check Render Logs |
| Deploy fails at build | Open Render Logs; make sure `Dockerfile` and `requirements.txt` are in the repo root |
