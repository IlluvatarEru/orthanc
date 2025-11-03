# Orthanc Capital


### How do I...
1. scrap sales on a regular basis?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode daily-sales
```
2. scrap sales the current sales as a one off?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales
```
3. scrap rentals on a regular daily basis?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode daily-rentals
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
7. scrap a specific JK (only if it doesn't already have data)?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Meridian" --rentals --sales
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Jazz" --rentals
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Istanbul" --sales --max-pages 5
```
8. manage blacklisted JKs?
```commandline
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action list
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action add --jk-name "Complex Name"
python -m scrapers.launch.launch_scraping_all_jks --mode blacklist --blacklist-action remove --jk-name "Complex Name"
```