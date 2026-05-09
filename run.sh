#!/usr/bin/env bash
# Auto-restart wrapper: agar bot crash ho jaye to 5s baad restart
set -u
while true; do
  echo "[run.sh] starting bot at $(date -u)"
  python bot.py
  code=$?
  echo "[run.sh] bot exited with code $code, restarting in 5s..."
  sleep 5
done
