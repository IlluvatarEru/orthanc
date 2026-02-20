# Orthanc Capital

## Daily Pipeline

Sales scraping and opportunity finding run automatically via a single cron job at 14:50 CET daily:

```
50 8 * * * /root/orthanc/scripts/daily_sales_pipeline.sh >> /root/orthanc/daily_pipeline.out 2>&1
```

(Server is US/Eastern, so 14:50 CET = 08:50 EST.)

The pipeline (`scripts/daily_sales_pipeline.sh`) prioritizes Almaty:
1. **Scrape Almaty sales** -- scrapes Almaty JKs first (638 JKs, ~fastest)
2. **In parallel**: runs the **opportunity finder** for Almaty while scraping **remaining cities** in the background
3. Waits for remaining cities to finish

This means Almaty opportunities are available on the dashboard as soon as Almaty scraping completes, without waiting for all other cities.

Logs go to `/root/orthanc/daily_pipeline.out`. To check the latest run:
```bash
tail -100 /root/orthanc/daily_pipeline.out
```

## Services

These run continuously via systemd:

| Service | Command | Purpose |
|---------|---------|---------|
| API Server | `systemctl start orthanc-api` | REST API on port 8000 |
| Frontend | `systemctl start orthanc-web` | Dashboard on port 5000 |
| Status | `systemctl start orthanc-status` | Health check on port 8002 |
| Market Data | `nohup python -m price.launch.launch_market_data > market_data.out 2>&1 &` | Fetches exchange rates daily |

To check status:
```bash
curl localhost:8002
systemctl status orthanc-api orthanc-web orthanc-status
```

### Restarting after code updates

```bash
git pull
systemctl restart orthanc-api orthanc-web orthanc-status
```

---

### How do I...
1. run sales scraping as a one-off?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales --city almaty
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales --exclude-city almaty
```
2. run rental scraping as a one-off?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --rentals
```
3. scrape a specific JK?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Meridian" --sales
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Jazz" --rentals
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Istanbul" --sales --max-pages 5
```
4. fetch all residential complexes from Krisha?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode fetch-jks
```
5. update JKs with unknown cities?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode update-jks-cities
```
6. run the opportunity finder manually?
```commandline
python -m analytics.launch.launch_opportunity_finder --city almaty
```
7. manage blacklisted JKs?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action list
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action add --jk-name "Complex Name"
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action remove --jk-name "Complex Name"
```