# Frontend

The frontend package contains the Flask web application for the Orthanc real estate analytics system.

## Structure

```
frontend/
├── src/                    # Source code
│   ├── webapp.py          # Main Flask application
│   └── webapp_api_client.py # API client for backend communication
├── templates/             # HTML templates
├── test/                  # Test files
│   └── test_webapp.py     # Webapp tests
├── launch/                # Launch scripts
│   └── launch_webapp.py   # Webapp launcher
└── README.md              # This file
```

## Quick Start

### 1. Start the API Server (Required)
```bash
cd /home/arthur/dev/orthanc
python -m api.launch.launch_api --host 127.0.0.1 --port 8000
```

### 2. Start the Webapp
```bash
cd /home/arthur/dev/orthanc
python -m frontend.launch.launch_webapp --host 0.0.0.0 --port 5000
```

### 3. Access the Webapp
- **Webapp**: http://localhost:5000
- **API Docs**: http://localhost:8000/docs

## API Calls Made

When visiting `http://localhost:5000/analyze_jk/Meridian%20Apartments`, the webapp makes these API calls:

1. `GET /api/complexes/Meridian%20Apartments` - Get complex info
2. `GET /api/jks/sales/Meridian%20Apartments/analysis` - Sales analysis (20% discount)
3. `GET /api/jks/rentals/Meridian%20Apartments/analysis` - Rental analysis (5% yield)
4. `POST /api/complexes/Meridian%20Apartments/scrape` - Auto-scraping if no data
5. `GET /api/database/stats` - Database statistics

## Features

- **Global Statistics**: Min/Max/Mean/Median prices for rentals and sales
- **Bucket Analysis**: Statistics by flat type with rental yields
- **Best Opportunities**: Properties with 20% discount vs median price
- **Collapsible Sections**: Sales and rental flats sections are collapsed by default

## Testing

```bash
cd /home/arthur/dev/orthanc
python frontend/test/test_webapp.py
```

## Troubleshooting

### "No data available" Error
This happens when:
1. API server is not running
2. No data in database for the complex
3. API endpoints are not working

### Solution
1. Start the API server first
2. Use "Refresh Analysis" button to scrape data
3. Check API server logs for errors
