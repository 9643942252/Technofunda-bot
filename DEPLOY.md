# Deploying technofunda_bot to Railway

## Files in this bundle
- `technofunda_bot.py` — the bot
- `requirements.txt` — Python deps
- `Dockerfile` — build config
- `railway.json` — Railway service config (health check on `/health`)
- `.env.example` — variable names to set in Railway (values are yours to fill in)

## Steps

1. **Create a new GitHub repo** (or add a folder to your existing [[financial-app]]
   repo) and push these 5 files into it.

2. **In Railway**: New Project → Deploy from GitHub repo → select the repo.
   Railway will detect the `Dockerfile` and `railway.json` automatically.

3. **Set environment variables** (Railway dashboard → your service → Variables):
   copy the variable *names* from `.env.example` and paste in your actual
   broker credentials there — never in chat, never committed to the repo.
   Leave any broker's variables blank to run without that broker (the bot
   auto-disables it and falls back to yfinance-only data).

4. **Deploy.** Railway assigns a public URL. Check it's alive:
   ```
   curl https://<your-service>.up.railway.app/health
   curl https://<your-service>.up.railway.app/signals
   ```

5. **Stays PAPER until you flip it.** `ORDER_MODE=PAPER` is the default —
   the bot logs intended trades to its SQLite DB but never calls a broker's
   place_order. Change the Railway variable to `LIVE` only once you've
   watched a few scan cycles and trust the signals.

## Getting it onto your phone

Railway doesn't push to a phone by itself — nothing does that without you
taking one action somewhere. Two low-effort ways to close that gap:

- **PWA polling**: your existing [[financial-app]] PWA can poll
  `/signals` on this Railway URL and fire a browser notification when a
  new high-score signal appears — since the PWA is already installed to
  your home screen, this needs no new install step from you.
- **Telegram bot** (~10 min one-time setup): create a bot via @BotFather,
  drop the token in as a Railway variable, and the scan loop can push a
  message to your phone the moment a signal clears the score threshold.
  Say the word and I'll add that agent to the script.

Both require Railway to be live and your credentials to be set — those
two steps are the only manual actions I can't do for you.
