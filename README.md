# Orthanc Capital

## Realtime Processes

These processes should be running continuously to keep the database populated and provide API access:

| Process | Command | Purpose |
|---------|---------|---------|
| Daily Sales Scraper | `nohup python -m scrapers.launch.launch_scraping_all_jks --mode daily-sales > daily_sales.out 2>&1 &` | Continuously scrapes sales listings |
| Daily Rentals Scraper | `nohup python -m scrapers.launch.launch_scraping_all_jks --mode daily-rentals > daily_rentals.out 2>&1 &` | Continuously scrapes rental listings |
| Market Data | `nohup python -m price.launch.launch_market_data > market_data.out 2>&1 &` | Fetches MIG exchange rates daily at midday UTC |
| API Server | `systemctl start orthanc-api` | Serves the explorer/API |
| Frontend | `systemctl start orthanc-web` | Serves the frontend |
| Status Service | `systemctl start orthanc-status` | Health check endpoint on port 8002 |

To check if processes are running:
```bash
ps aux | grep launch_scraping_all_jks
ps aux | grep launch_market_data
systemctl status orthanc-api
systemctl status orthanc-web
systemctl status orthanc-status
```

Or use the status endpoint:
```bash
curl localhost:8002
```

### Restarting after code updates

After pulling new code changes, restart the services:
```bash
git pull
systemctl restart orthanc-api
systemctl restart orthanc-web
systemctl restart orthanc-status
```

---

### How do I...
1. scrap sales on a regular basis?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode daily-sales
```
   Run in background with nohup:
```commandline
nohup python -m scrapers.launch.launch_scraping_all_jks --mode daily-sales > daily_sales.out 2>&1 &
```
2. scrap sales the current sales as a one off?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales
```
3. scrap rentals on a regular daily basis?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode daily-rentals
```
   Run in background with nohup:
```commandline
nohup python -m scrapers.launch.launch_scraping_all_jks --mode daily-rentals > daily_rentals.out 2>&1 &
```
4. scrap rentals just as a one off?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --rentals
```
5. scrap all the JKs?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode immediate
```
6. fetch all residential complexes from Krisha?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode fetch-jks
```
7. update JKs with unknown cities?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode update-jks-cities
```
8. scrap a specific JK (only if it doesn't already have data)?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Meridian" --rentals --sales
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Jazz" --rentals
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Istanbul" --sales --max-pages 5
```
9. manage blacklisted JKs?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action list
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action add --jk-name "Complex Name"
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action remove --jk-name "Complex Name"
```