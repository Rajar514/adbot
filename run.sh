#!/usr/bin/env bash
# Auto-restart wrapper: restarts bot on crash with exponential backoff
set -u
ATTEMPT=0
while true; do
  ATTEMPT=$((ATTEMPT + 1))
  echo "[run.sh] starting bot (attempt #${ATTEMPT}) at $(date -u)"
  python bot.py
  code=$?
  echo "[run.sh] bot exited with code ${code}"
  if [ ${code} -eq 0 ]; then
    echo "[run.sh] clean exit — not restarting"
    break
  fi
  # Exponential backoff: 5s, 10s, 20s … capped at 60s
  wait=$(( 5 * (1 << (ATTEMPT < 4 ? ATTEMPT - 1 : 3)) ))
  wait=$(( wait > 60 ? 60 : wait ))
  echo "[run.sh] restarting in ${wait}s…"
  sleep "${wait}"
done
