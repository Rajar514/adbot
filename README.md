# Tecxo Ads Bot — Free Hosting Edition

Telegram ad broadcaster (multi-account, parallel send to all groups).

## 🆓 FREE Hosting Options

| Platform | Always-On | Persistent Storage | Setup |
|---|---|---|---|
| **Fly.io** ⭐ recommended | ✅ Yes | ✅ Yes (3GB free volume) | `flyctl deploy` |
| **Koyeb** | ✅ Yes | ❌ No (sessions lost on redeploy) | GitHub auto-deploy |
| **Replit** | ⚠️ sleeps | ✅ Yes | needs uptime pinger |

---

## Option 1: Fly.io (BEST — free + persistent)

1. Install flyctl: `curl -L https://fly.io/install.sh | sh`
2. `flyctl auth signup` (free account, requires card but no charge)
3. In project folder:
   ```
   flyctl launch --no-deploy --copy-config --name tecxo-ads-bot
   flyctl volumes create tecxo_data --size 1 --region sin
   flyctl secrets set API_ID=xxx API_HASH=xxx BOT_TOKEN=xxx LOGS_BOT_TOKEN=xxx
   flyctl deploy
   ```
4. Done — bot runs 24/7 free.

## Option 2: Koyeb (no card needed)

1. Push this repo to GitHub.
2. Go to https://app.koyeb.com → **Create Web Service** → GitHub → select repo.
3. Instance type: **Free** (Nano).
4. Add env vars: `API_ID`, `API_HASH`, `BOT_TOKEN`, `LOGS_BOT_TOKEN`.
5. Port: `8080`. Deploy.
6. ⚠️ Koyeb free has no persistent disk — users must re-login after redeploy.

## Option 3: Local / VPS

```
pip install -r requirements.txt
cp .env.example .env   # fill in values
python bot.py
```

---

## Env vars

- `API_ID`, `API_HASH` — from https://my.telegram.org
- `BOT_TOKEN` — main dashboard bot
- `LOGS_BOT_TOKEN` — logs bot
- `DATA_DIR` — where sessions + sqlite live (default `/data`)
- `PORT` — health server port (default 8080)

## Usage

1. `/start` the main bot in Telegram → dashboard appears.
2. **Login Account** → enter phone → OTP (send digits with spaces: `1 2 3 4 5`) → 2FA if any.
3. **Set Ad Message** → paste your ad text.
4. **Set Interval** → seconds between rounds (min 60).
5. **Start Ads ▶️** — sends to ALL groups in parallel.
6. Open the **logs bot** to watch per-group send results live.

⚠️ Sending to many groups simultaneously can trigger Telegram SpamBan. Use a throwaway account.
