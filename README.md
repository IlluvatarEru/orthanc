# Orthanc Capital


### How do I...
1. scrap sales on a regular basis?
```commandline
python -m scrapers.launch.launch_scrapping_all_jks --mode daily-sales
```
2. scrap sales the current sales as a one off?
```commandline
python -m scrapers.launch.launch_scrapping_all_jks --mode immediate --sales
```
3. scrap rentals on a regular daily basis?
```commandline
python -m scrapers.launch.launch_scrapping_all_jks --mode daily-rentals
```
4. scrap rentals just as a one off?
```commandline
python -m scrapers.launch.launch_scrapping_all_jks --mode immediate --rentals
```
5. scrap all the JKs?
```commandline
python -m scrapers.launch.launch_scrapping_all_jks --mode immediate
```
6. fetch all residential complexes from Krisha?
```commandline
python -m scrapers.launch.launch_scrapping_all_jks --mode fetch-jks
```
7. manage blacklisted JKs?
```commandline
python -m scrapers.launch.launch_scrapping_all_jks --mode blacklist --blacklist-action list
python -m scrapers.launch.launch_scrapping_all_jks --mode blacklist --blacklist-action add --jk-name "Complex Name"
python -m scrapers.launch.launch_scrapping_all_jks --mode blacklist --blacklist-action remove --jk-name "Complex Name"
```