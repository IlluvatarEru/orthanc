#!/bin/bash
# Daily sales scraping pipeline (Almaty only).
# Scrapes Almaty sales, then runs opportunity finder.
# Runs as a single cron job at 10:00 CET daily.

set -e

PYTHON=/root/orthanc/venv/bin/python
cd /root/orthanc

echo "=== $(date) === Starting daily sales pipeline (Almaty) ==="

# Step 1: Scrape Almaty sales
echo "--- Step 1: Scraping Almaty sales ---"
$PYTHON -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales --city almaty --max-pages 10

echo "--- Almaty sales scraping complete ---"

# Step 2: Run opportunity finder for Almaty + Astana
echo "--- Step 2: Opportunity finder (Almaty + Astana) ---"
$PYTHON -m analytics.launch.launch_opportunity_finder --city almaty astana

echo "--- Opportunity finder complete ---"

echo "=== $(date) === Daily sales pipeline complete ==="
