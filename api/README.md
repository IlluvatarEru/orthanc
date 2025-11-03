# Orthanc Real Estate Analytics API

FastAPI-based REST API for analyzing real estate data including flats, sales, and rentals.

## Features

- **Flat Analysis**: Search and analyze individual flats
- **JK Sales Analysis**: Comprehensive sales analytics for residential complexes
- **JK Rentals Analysis**: Rental market analysis for residential complexes
- **Interactive Documentation**: Auto-generated API docs with Swagger UI

## Quick Start

### Installation

```bash
# Install API dependencies
pip install -r api/requirements.txt
```

### Running the API

```bash
# Development mode with auto-reload
python -m api.launch.launch_api --reload

# Production mode
python -m api.launch.launch_api --host 0.0.0.0 --port 8000
```

### Accessing the API

- **API Base URL**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc Documentation**: http://localhost:8000/redoc

## API Endpoints

### Flat Analysis (`/api/flats/`)

- `GET /api/flats/search` - Search flats with filters
- `GET /api/flats/{flat_id}` - Get specific flat details
- `GET /api/flats/stats/summary` - Get flats summary statistics

### JK Sales Analysis (`/api/jks/sales/`)

- `GET /api/jks/sales/` - List all residential complexes
- `GET /api/jks/sales/{jk_name}/summary` - Get sales summary for a JK
- `GET /api/jks/sales/{jk_name}/analysis` - Get comprehensive sales analysis
- `GET /api/jks/sales/{jk_name}/opportunities` - Get sales opportunities

### JK Rentals Analysis (`/api/jks/rentals/`)

- `GET /api/jks/rentals/` - List JKs with rental data
- `GET /api/jks/rentals/{jk_name}/summary` - Get rental summary for a JK
- `GET /api/jks/rentals/{jk_name}/rentals` - Get rental listings for a JK
- `GET /api/jks/rentals/{jk_name}/price-trends` - Get rental price trends
- `GET /api/jks/rentals/stats/overview` - Get overall rental market overview

## Example Usage

### Search for flats

```bash
curl "http://localhost:8000/api/flats/search?flat_type=2BR&min_price=10000000&limit=10"
```

### Get JK sales analysis

```bash
curl "http://localhost:8000/api/jks/sales/Meridian%20Apartments/analysis?discount_percentage=0.15"
```

### Get rental opportunities

```bash
curl "http://localhost:8000/api/jks/rentals/Meridian%20Apartments/rentals?flat_type=2BR&limit=20"
```

## Testing

```bash
# Run API tests
python -m pytest api/test/ -v

# Run specific test
python -m pytest api/test/test_api.py::TestAPI::test_health_check -v
```

## Development

The API is organized into separate modules:

- `api/src/app.py` - Main FastAPI application
- `api/src/flat_analysis.py` - Flat-specific endpoints
- `api/src/jk_analysis_sales.py` - JK sales analysis endpoints
- `api/src/jk_analysis_rentals.py` - JK rentals analysis endpoints
- `api/launch/launch_api.py` - API launcher script

## Configuration

The API uses the same database (`flats.db`) as the main Orthanc application. Make sure the database is properly initialized before running the API.

