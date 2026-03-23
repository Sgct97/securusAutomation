#!/usr/bin/env bash
# =========================================================================
# Droplet setup script — run once after cloning the repo
# Usage: ssh root@<ip> then:
#   git clone https://github.com/Sgct97/securusAutomation.git /opt/securusAutomation
#   cd /opt/securusAutomation
#   chmod +x setup.sh && ./setup.sh
# =========================================================================
set -euo pipefail

APP_DIR="/opt/securusAutomation"
VENV_DIR="$APP_DIR/venv"
LOG_DIR="$APP_DIR/logs"
DATA_DIR="$APP_DIR/data"
CRON_SCHEDULE="0 9 * * *"  # Daily at 9 AM UTC

echo "=== Securus Automation — Droplet Setup ==="

# System packages
echo "[1/6] Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    xvfb > /dev/null 2>&1

# Python venv
echo "[2/6] Creating Python virtual environment..."
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip -q

# Dependencies
echo "[3/6] Installing Python packages..."
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt" -q

# Playwright browser
echo "[4/6] Installing Playwright Chromium..."
"$VENV_DIR/bin/playwright" install chromium
"$VENV_DIR/bin/playwright" install-deps chromium > /dev/null 2>&1

# Directories
echo "[5/6] Creating data and log directories..."
mkdir -p "$LOG_DIR" "$DATA_DIR" "$DATA_DIR/securus_debug"

# .env file
if [ ! -f "$APP_DIR/.env" ]; then
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    echo ">>> IMPORTANT: Edit $APP_DIR/.env with the Securus password <<<"
else
    echo ".env already exists, skipping"
fi

# Cron job
echo "[6/6] Setting up daily cron job..."
CRON_CMD="$CRON_SCHEDULE cd $APP_DIR && $VENV_DIR/bin/python pipeline.py >> $LOG_DIR/pipeline.log 2>&1"
(crontab -l 2>/dev/null | grep -v "pipeline.py"; echo "$CRON_CMD") | crontab -
echo "Cron installed: $CRON_SCHEDULE"

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit .env:  nano $APP_DIR/.env"
echo "  2. Test run:    cd $APP_DIR && $VENV_DIR/bin/python pipeline.py"
echo "  3. Check logs:  tail -f $LOG_DIR/pipeline.log"
echo ""
echo "To adjust daily message count:"
echo "  Edit DAILY_MESSAGE_LIMIT in $APP_DIR/.env"
echo "  (takes effect on next cron run, no restart needed)"
echo ""
echo "To pause sending:  set DAILY_MESSAGE_LIMIT=0 in .env"
echo "To resume sending: set DAILY_MESSAGE_LIMIT=25 in .env"
