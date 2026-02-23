#!/bin/bash
# Daily sales scraping pipeline (Almaty only).
# Scrapes Almaty sales, then runs opportunity finder.
# Each run gets its own timestamped log file in logs/.

set -e

PYTHON=/root/orthanc/venv/bin/python
cd /root/orthanc

# Create per-run log file
mkdir -p logs
LOGFILE="logs/pipeline_$(date +%Y_%m_%d_%H%M%S).log"

echo "=== $(date) === Starting daily sales pipeline (Almaty) ===" | tee "$LOGFILE"

# Step 1: Scrape Almaty sales
echo "--- Step 1: Scraping Almaty sales ---" | tee -a "$LOGFILE"
$PYTHON -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales --city almaty --max-pages 20 2>&1 | tee -a "$LOGFILE"

echo "--- Almaty sales scraping complete ---" | tee -a "$LOGFILE"

# Step 2: Run opportunity finder for Almaty
echo "--- Step 2: Opportunity finder (Almaty) ---" | tee -a "$LOGFILE"
$PYTHON -m analytics.launch.launch_opportunity_finder 2>&1 | tee -a "$LOGFILE"

echo "--- Opportunity finder complete ---" | tee -a "$LOGFILE"

echo "=== $(date) === Daily sales pipeline complete ===" | tee -a "$LOGFILE"
