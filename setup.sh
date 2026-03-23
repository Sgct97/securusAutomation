#!/bin/bash
# Setup script for Securus Automation

set -e

echo "=================================================="
echo "Securus Automation - Setup Script"
echo "=================================================="

# Check Python version
echo ""
echo "[1/4] Checking Python version..."
python3 --version || { echo "Python 3 not found. Please install Python 3.11+"; exit 1; }

# Create virtual environment
echo ""
echo "[2/4] Creating virtual environment..."
if [ -d "venv" ]; then
    echo "    Virtual environment already exists"
else
    python3 -m venv venv
    echo "    ✓ Created virtual environment"
fi

# Activate and install dependencies
echo ""
echo "[3/4] Installing dependencies..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Install Playwright browsers
echo ""
echo "[4/4] Installing Playwright browsers..."
playwright install chromium

# Create data directory
mkdir -p data logs

echo ""
echo "=================================================="
echo "Setup complete!"
echo "=================================================="
echo ""
echo "To activate the environment:"
echo "    source venv/bin/activate"
echo ""
echo "To run Oklahoma reconnaissance:"
echo "    python -m scrapers.oklahoma_recon"
echo ""

