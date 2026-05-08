# 🚀 Render Free 24/7 Deployment Guide

## Step-by-Step Setup

### 1. GitHub pe code push karo
```bash
git init
git add .
git commit -m "tecxo ads bot"
git remote add origin https://github.com/YOUR_USERNAME/tecxo-ads-bot.git
git push -u origin main
```

### 2. Render pe deploy karo
1. https://render.com pe sign up karo (free, no credit card)
2. **New +** → **Web Service** click karo
3. Apna GitHub repo connect karo
4. Settings:
   - **Name**: `tecxo-ads-bot`
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python bot.py`
   - **Plan**: **Free**
5. **Environment Variables** add karo:
   - `API_ID` = (my.telegram.org se)
   - `API_HASH` = (my.telegram.org se)
   - `BOT_TOKEN` = `8274609224:AAGFSiRwEeGdJ5OhLCD-JVEPQuxUSWRLMLQ`
   - `LOGS_BOT_TOKEN` = `8532660301:AAGtcytlBJmS11mOMlMK4VvXuMzU_YzmWkk`
   - `DATA_DIR` = `/tmp`
6. **Create Web Service** click karo

### 3. ⚠️ IMPORTANT: 24/7 Keep Alive Setup

Render Free 15 min idle ke baad sleep ho jata hai. Solution:

#### UptimeRobot Setup (Free):
1. https://uptimerobot.com pe sign up karo (free)
2. **+ Add New Monitor** click karo
3. Settings:
   - **Monitor Type**: `HTTP(s)`
   - **Friendly Name**: `Tecxo Bot`
   - **URL**: `https://tecxo-ads-bot.onrender.com` (apna Render URL)
   - **Monitoring Interval**: `5 minutes`
4. **Create Monitor**

Ab bot 24/7 jaga rahega! 🎉

---

## ⚠️ Important Limitations (Render Free)

### 1. **Sessions LOST honge restart pe**
Render Free me **persistent disk nahi** hai (paid feature).
- `DATA_DIR=/tmp` pe sessions save honge
- Service restart hone pe (deploy ya crash) **sab users ko re-login karna padega**
- SQLite database bhi reset ho jayegi

**Fix**: Sessions ko PostgreSQL me store karo (Render free PostgreSQL deta hai 90 days, ya use Supabase free forever).

### 2. **750 hours/month free** (1 service 24/7 chalane ke liye enough)

### 3. **512 MB RAM** — 50-100 users ke liye theek hai

---

## 🏆 Better Alternative: Persistent Storage Chahiye?

Agar sessions lose nahi karna chahte to:
- **Fly.io** — free me 3GB persistent volume (already configured `fly.toml` me)
- **Oracle Cloud Free** — full VPS, lifetime free, 24GB RAM

---

## Testing
1. Render dashboard me **Logs** tab open karo
2. `Bot started successfully` message dikhna chahiye
3. Telegram pe apne main bot ko `/start` bhejo
4. Dashboard milega → account add karo → ads start karo
