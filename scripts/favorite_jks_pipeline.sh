#!/bin/bash
set -e

PYTHON=/root/orthanc/venv/bin/python
cd /root/orthanc

mkdir -p logs
LOGFILE="logs/favorite_jks_pipeline_$(date +%Y_%m_%d_%H%M%S).log"

echo "=== $(date) === Starting favorite JKs pipeline ===" | tee "$LOGFILE"

# Get favorite JK names from DB
FAVORITE_JKS=$($PYTHON -c "
from db.src.write_read_database import OrthancDB
with OrthancDB() as db:
    for jk in db.get_favorite_jks():
        print(jk['name'])
")

if [ -z "$FAVORITE_JKS" ]; then
    echo "No favorite JKs found, skipping" | tee -a "$LOGFILE"
    exit 0
fi

# Scrape each favorite JK
while IFS= read -r jk_name; do
    echo "--- Scraping: $jk_name ---" | tee -a "$LOGFILE"
    $PYTHON -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "$jk_name" --sales 2>&1 | tee -a "$LOGFILE"
done <<< "$FAVORITE_JKS"

# Run opportunity finder after scraping
echo "--- Running opportunity finder ---" | tee -a "$LOGFILE"
$PYTHON -m analytics.launch.launch_opportunity_finder 2>&1 | tee -a "$LOGFILE"

echo "=== $(date) === Favorite JKs pipeline complete ===" | tee -a "$LOGFILE"
