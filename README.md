# Orthanc Capital - Krisha.kz Scraper

This repository contains a comprehensive web scraping system for [Krisha.kz](https://krisha.kz), a popular real estate website in Kazakhstan. The system extracts flat information for both rental and sales listings with historical tracking.

## 🏗️ System Architecture

### Core Components

1. **Individual Flat Scraper** (`krisha_scraper.py`) - Scrapes individual flat details using Krisha.kz API
2. **Search Scraper** (`search_scraper.py`) - Extracts flat URLs from search pages and scrapes each flat
3. **Enhanced Database** (`enhanced_database.py`) - SQLite database with separate tables for rentals and sales
4. **Residential Complex Scraper** (`complex_scraper.py`) - Fetches and maps residential complex IDs to names
5. **Scheduler** (`scheduler.py`) - Runs scraping jobs based on configuration
6. **Configuration** (`config.toml`) - TOML-based configuration for search queries

## 🗄️ Database Schema

### Tables Overview

The system uses SQLite with three main tables for data storage and analysis:

#### 1. `rental_flats` - Rental Property Data
Stores rental flat information with historical tracking by query date.

**Schema:**
```sql
CREATE TABLE rental_flats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flat_id TEXT NOT NULL,                    -- Unique flat identifier
    price INTEGER NOT NULL,                   -- Monthly rent in tenge
    area REAL NOT NULL,                       -- Area in square meters
    residential_complex TEXT,                 -- Complex name (e.g., "Meridian Apartments")
    floor INTEGER,                            -- Floor number
    total_floors INTEGER,                     -- Total floors in building
    construction_year INTEGER,                -- Year of construction
    parking TEXT,                             -- Parking information
    description TEXT NOT NULL,                -- Full description
    url TEXT NOT NULL,                        -- Original listing URL
    query_date DATE NOT NULL,                 -- Date when scraped (YYYY-MM-DD)
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(flat_id, query_date)              -- Prevents duplicates per day
);
```

**Example Queries:**
```sql
-- Get all rental flats for a specific date
SELECT * FROM rental_flats WHERE query_date = '2025-08-02';

-- Get average rental price by complex
SELECT residential_complex, AVG(price) as avg_price, COUNT(*) as count
FROM rental_flats 
WHERE query_date = '2025-08-02'
GROUP BY residential_complex
ORDER BY avg_price DESC;

-- Get rental price trends over time
SELECT query_date, AVG(price) as avg_price, COUNT(*) as listings
FROM rental_flats 
WHERE residential_complex = 'Meridian Apartments'
GROUP BY query_date
ORDER BY query_date;
```

#### 2. `sales_flats` - Sales Property Data
Stores sales flat information with historical tracking by query date.

**Schema:**
```sql
CREATE TABLE sales_flats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flat_id TEXT NOT NULL,                    -- Unique flat identifier
    price INTEGER NOT NULL,                   -- Sale price in tenge
    area REAL NOT NULL,                       -- Area in square meters
    residential_complex TEXT,                 -- Complex name
    floor INTEGER,                            -- Floor number
    total_floors INTEGER,                     -- Total floors in building
    construction_year INTEGER,                -- Year of construction
    parking TEXT,                             -- Parking information
    description TEXT NOT NULL,                -- Full description
    url TEXT NOT NULL,                        -- Original listing URL
    query_date DATE NOT NULL,                 -- Date when scraped (YYYY-MM-DD)
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(flat_id, query_date)              -- Prevents duplicates per day
);
```

**Example Queries:**
```sql
-- Get all sales flats for a specific date
SELECT * FROM sales_flats WHERE query_date = '2025-08-02';

-- Get average sale price by complex
SELECT residential_complex, AVG(price) as avg_price, COUNT(*) as count
FROM sales_flats 
WHERE query_date = '2025-08-02'
GROUP BY residential_complex
ORDER BY avg_price DESC;

-- Get price per square meter
SELECT residential_complex, AVG(price/area) as price_per_sqm
FROM sales_flats 
WHERE query_date = '2025-08-02'
GROUP BY residential_complex
ORDER BY price_per_sqm DESC;
```

#### 3. `residential_complexes` - Complex Mapping
Maps complex IDs to readable names and locations.

**Schema:**
```sql
CREATE TABLE residential_complexes (
    id INTEGER PRIMARY KEY,
    complex_id TEXT UNIQUE NOT NULL,          -- Krisha.kz complex ID
    name TEXT NOT NULL,                       -- Complex name
    city TEXT,                                -- City name
    district TEXT,                            -- District name
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Example Queries:**
```sql
-- Get all complexes in Almaty
SELECT * FROM residential_complexes WHERE city = 'Алматы';

-- Search complexes by name
SELECT * FROM residential_complexes WHERE name LIKE '%Meridian%';

-- Get complex by ID
SELECT * FROM residential_complexes WHERE complex_id = '2758';
```

### Indexes for Performance

```sql
-- Rental flats indexes
CREATE INDEX idx_rental_flat_id ON rental_flats(flat_id);
CREATE INDEX idx_rental_query_date ON rental_flats(query_date);
CREATE INDEX idx_rental_price ON rental_flats(price);
CREATE INDEX idx_rental_area ON rental_flats(area);

-- Sales flats indexes
CREATE INDEX idx_sales_flat_id ON sales_flats(flat_id);
CREATE INDEX idx_sales_query_date ON sales_flats(query_date);
CREATE INDEX idx_sales_price ON sales_flats(price);
CREATE INDEX idx_sales_area ON sales_flats(area);

-- Complexes index
CREATE INDEX idx_complex_id ON residential_complexes(complex_id);
```

### Advanced Queries

#### Rental Yield Analysis
```sql
-- Calculate rental yield (annual rent / sale price)
SELECT 
    r.residential_complex,
    AVG(r.price * 12.0 / s.price) as rental_yield,
    COUNT(*) as sample_size
FROM rental_flats r
JOIN sales_flats s ON r.residential_complex = s.residential_complex 
    AND r.query_date = s.query_date
    AND ABS(r.area - s.area) < 5  -- Similar area
WHERE r.query_date = '2025-08-02'
GROUP BY r.residential_complex
HAVING sample_size > 1
ORDER BY rental_yield DESC;
```

#### Price Trends Over Time
```sql
-- Rental price trends
SELECT 
    query_date,
    residential_complex,
    AVG(price) as avg_rental_price,
    COUNT(*) as listings
FROM rental_flats 
WHERE residential_complex = 'Meridian Apartments'
GROUP BY query_date, residential_complex
ORDER BY query_date;

-- Sales price trends
SELECT 
    query_date,
    residential_complex,
    AVG(price) as avg_sale_price,
    COUNT(*) as listings
FROM sales_flats 
WHERE residential_complex = 'Meridian Apartments'
GROUP BY query_date, residential_complex
ORDER BY query_date;
```

#### Market Analysis
```sql
-- Compare rental vs sales prices by area
SELECT 
    CASE 
        WHEN area < 35 THEN 'Small (<35m²)'
        WHEN area < 60 THEN 'Medium (35-60m²)'
        ELSE 'Large (>60m²)'
    END as size_category,
    AVG(rental_price) as avg_rental,
    AVG(sale_price) as avg_sale,
    AVG(rental_price * 12.0 / sale_price) as yield
FROM (
    SELECT 
        r.area,
        r.price as rental_price,
        s.price as sale_price
    FROM rental_flats r
    JOIN sales_flats s ON r.residential_complex = s.residential_complex 
        AND r.query_date = s.query_date
        AND ABS(r.area - s.area) < 5
    WHERE r.query_date = '2025-08-02'
        AND r.residential_complex = 'Meridian Apartments'
) combined
GROUP BY size_category;
```

## 🚀 Quick Start

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database and fetch residential complexes
python complex_scraper.py

# Test the system
python scheduler.py --query-type rental --query-name "Meridian Apartments - 1 room rentals"
```

### Configuration

Edit `config.toml` to define your search queries:

```toml
# Example rental query
[[rental_queries]]
name = "Meridian Apartments - 1 room rentals"
description = "1-room apartments for rent in Meridian Apartments, Almaty"
url = "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&das[map.complex]=2758"
parameters = { rooms = 1, complex_id = "2758", area_max = 35 }

# Example sales query
[[sales_queries]]
name = "Meridian Apartments - 1 room sales"
description = "1-room apartments for sale in Meridian Apartments, Almaty"
url = "https://krisha.kz/prodazha/kvartiry/almaty/?das[live.rooms]=1&das[map.complex]=2758"
parameters = { rooms = 1, complex_id = "2758", area_max = 35 }
```

## 📊 Features

### Data Extraction

The scraper extracts the following information from flat listings:

1. **Price** - Price in tenge
2. **Area** - Area in square meters (m²)
3. **Residential Complex (JK)** - Name of the residential complex
4. **Floor** - Floor number where the flat is located
5. **Total Floors** - Total number of floors in the building
6. **Construction Year** - Year of construction
7. **Parking** - Parking information
8. **Description** - Full description text
9. **Flat ID** - Unique identifier extracted from URL
10. **Query Date** - Date when the search was performed (for historical tracking)

### Historical Tracking

- Each flat is stored with the query date
- Allows tracking price changes over time
- Supports multiple queries per day
- Prevents duplicate entries for the same flat on the same date

### Residential Complex Mapping

- Fetches complex data from `https://krisha.kz/complex/ajaxMapComplexGetAll?isSearch=1`
- Maps complex IDs to readable names
- Supports 3,254+ residential complexes in Kazakhstan

### Investment Analysis

- **JK Analytics**: Comprehensive analysis of residential complexes including rental yields
- **Flat Estimation**: Individual flat investment potential analysis
- **Price Comparison**: Compare flat prices against market medians and averages
- **Yield Calculation**: Calculate expected rental yields based on market data
- **Investment Recommendations**: Buy/sell recommendations based on yield and price analysis

## 🛠️ Usage

### Main Entry Point

The system now provides a unified command-line interface through `main.py`:

```bash
# Show all available commands
python main.py --help

# Run all configured scraping jobs
python main.py schedule --all

# Run specific rental query
python main.py schedule --query-type rental --query-name "Meridian Apartments - 1 room rentals"

# Analyze a residential complex
python main.py analyze --complex "Meridian" --area-max 35

# Update residential complex database
python main.py update-complexes

# Show database statistics
python main.py stats

# Search database
python main.py search --min-price 300000 --max-price 600000

# Estimate investment potential for a flat
python main.py estimate --flat-id 1003924251
```

### Command Line Tools

#### Scheduler
```bash
# Run all configured queries
python cli/scheduler.py --all

# Run specific query
python cli/scheduler.py --query-type rental --query-name "Meridian Apartments - 1 room rentals"

# List available queries
python cli/scheduler.py
```

#### Database Management
```bash
# Show database statistics
python cli/cli_tool.py stats

# Search flats with filters
python cli/cli_tool.py search --min-price 300000 --max-price 600000

# Export data to CSV
python cli/cli_tool.py export flats_data.csv
```

#### Search Scraper
```bash
# Analyze search page
python cli/search_cli.py analyze "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&das[map.complex]=2758"

# Scrape and save to database
python cli/search_cli.py scrape-save "https://krisha.kz/arenda/kvartiry/almaty/?das[live.rooms]=1&das[map.complex]=2758" --max-flats 10
```

#### JK Analytics
```bash
# Analyze residential complex
python analytics/jk_analytics.py

# Or use the main entry point
python main.py analyze --complex "Meridian" --area-max 35
```

### Scheduled Execution

Set up a cron job to run every 24 hours:

```bash
# Add to crontab (run at 9 AM daily)
0 9 * * * /usr/bin/python3 /path/to/orthanc/launcher.py

# Or run manually
python launcher.py
```

## 📈 Data Analysis

### Historical Statistics

```python
from db.enhanced_database import EnhancedFlatDatabase

db = EnhancedFlatDatabase()
stats = db.get_historical_statistics('2025-07-01', '2025-08-02')

print(f"Rental price range: {stats['rental_stats']['min_rental_price']:,} - {stats['rental_stats']['max_rental_price']:,} tenge")
print(f"Sales price range: {stats['sales_stats']['min_sales_price']:,} - {stats['sales_stats']['max_sales_price']:,} tenge")
```

### JK (Residential Complex) Analytics

The system includes comprehensive analytics for residential complexes, including rental yield calculations:

```python
from analytics.jk_analytics import JKAnalytics

# Analyze a specific complex
analytics = JKAnalytics()
analysis = analytics.get_jk_comprehensive_analysis("Meridian", area_max=35.0)

# Get rental yield analysis
yield_data = analytics.calculate_rental_yield("Meridian", area_max=35.0)
print(f"Median rental yield: {yield_data['yield_analysis']['median_yield_percent']:.2f}%")
```

#### JK Analytics Features

- **Rental Statistics**: Average, median, min/max prices for rental flats
- **Sales Statistics**: Average, median, min/max prices for sales flats  
- **Rental Yield Calculation**: Annual rental yield based on median prices
- **Market Insights**: Investment potential, data reliability assessment
- **Price per Square Meter**: Both rental and sales price per m² analysis

#### Example Output

```
🏢 Analysis for Meridian
   Date: 2025-08-02
   Area limit: ≤35.0 m²
============================================================
📊 Rental Statistics (11 flats):
   Price range: 330,000 - 500,000 tenge
   Average price: 380,455 tenge
   Median price: 350,000 tenge
   Area range: 31.0 - 35.0 m²
   Average area: 33.7 m²

💰 Sales Statistics (12 flats):
   Price range: 33,000,000 - 48,000,000 tenge
   Average price: 40,991,667 tenge
   Median price: 41,700,000 tenge
   Area range: 30.0 - 35.0 m²
   Average area: 33.5 m²

📈 Rental Yield Analysis:
   Median annual rent: 4,200,000 tenge
   Median sale price: 41,700,000 tenge
   Median yield: 10.07%
   Average yield: 11.14%

💡 Market Insights:
   Rental price per m²: 11,280 tenge
   Sales price per m²: 1,221,808 tenge
   Investment potential: High
   Data reliability: High
```

### Complex Search

```python
from scrapers.complex_scraper import search_complex_by_name

# Find complex by name
meridian = search_complex_by_name("Meridian")
if meridian:
    print(f"Found: {meridian['name']} (ID: {meridian['complex_id']})")
```

### Flat Investment Estimation

The system can analyze individual flats for investment potential:

```bash
# Estimate investment potential for a specific flat
python main.py estimate --flat-id 1003924251 --area-tolerance 25
```

#### Estimation Features

- **Flat Analysis**: Scrapes and saves flat information to database
- **Similar Property Search**: Finds rentals and sales with similar characteristics
- **Rental Yield Calculation**: Calculates expected annual rental income and yield
- **Price Comparison**: Compares flat price against median/average similar sales
- **Investment Recommendation**: Provides buy/sell recommendations based on yield and price
- **Discount Analysis**: Calculates returns for 10% and 20% purchase discounts
- **Yield Thresholds**: STRONG BUY (>20%), BUY (>6% + good price), CONSIDER (>5%)

#### Example Output

```
🏠 Analyzing flat 1003924251 for investment potential...
📥 Scraping flat info from: https://krisha.kz/a/show/1003924251
✅ Flat info saved to database
📊 Flat Details:
   Price: 38,000,000 tenge
   Area: 33.0 m²
   Residential Complex: Meridian Apartments
   Floor: 10
   Construction Year: 2024

🔍 Finding similar rental flats...
   Found 19 similar rental flats
   Rental price range: 330,000 - 500,000 tenge
   Average rental price: 392,368 tenge
   Median rental price: 360,000 tenge

💰 Finding similar sales flats...
   Found 19 similar sales flats
   Sales price range: 33,000,000 - 48,000,000 tenge
   Average sales price: 41,100,000 tenge
   Median sales price: 41,400,000 tenge

📈 Investment Analysis:
==================================================
💡 Investment Potential:
   Expected annual rental income: 4,320,000 tenge
   Rental yield: 11.37%

💰 Price Analysis:
   Your price: 38,000,000 tenge
   Median similar sales: 41,400,000 tenge
   Average similar sales: 41,100,000 tenge
   Price vs median: -8.2% (✅ Good deal (below median))
   Yield rating: 🔥 Excellent yield (>8%)

🎯 Overall Recommendation:
   ✅ BUY - Good yield + good price

💰 Discount Analysis:
==============================

📉 10% Discount Scenario:
   Discounted price: 34,200,000 tenge
   Savings: 3,800,000 tenge
   Rental yield: 12.63%
   Yield rating: ✅ Good yield (10-15%)
   Price vs median: -17.4% (🔥 Excellent deal (significantly below median))
   💡 Recommendation: ✅ BUY with 10% discount

📉 20% Discount Scenario:
   Discounted price: 30,400,000 tenge
   Savings: 7,600,000 tenge
   Rental yield: 14.21%
   Yield rating: ✅ Good yield (10-15%)
   Price vs median: -26.6% (🔥 Excellent deal (significantly below median))
   💡 Recommendation: ✅ BUY with 20% discount
```

## 🔧 Configuration Options

### Scraping Settings
- `delay_between_requests` - Delay between requests (default: 2.0 seconds)
- `max_flats_per_query` - Maximum flats to scrape per query (default: 50)
- `retry_attempts` - Number of retry attempts for failed requests (default: 3)

### Logging Settings
- `level` - Log level (DEBUG, INFO, WARNING, ERROR)
- `file` - Log file path (default: scraper.log)
- `max_size_mb` - Maximum log file size
- `backup_count` - Number of backup log files

### Scheduling Settings
- `enabled` - Enable/disable scheduling
- `interval_hours` - Interval between runs (default: 24)
- `start_time` - Start time for daily runs (default: 09:00)
- `timezone` - Timezone for scheduling (default: Asia/Almaty)

## 📁 File Structure

```
orthanc/
├── main.py                           # Main entry point with CLI
├── requirements.txt                   # Python dependencies
├── flats.db                          # SQLite database
├── scraper.log                       # Log file
├── README.md                         # This file
│
├── common/                           # Shared utilities
│   ├── __init__.py
│   └── krisha_scraper.py            # Individual flat scraper
│
├── db/                              # Database management
│   ├── __init__.py
│   └── enhanced_database.py         # Database operations
│
├── scrapers/                        # Web scraping components
│   ├── __init__.py
│   ├── search_scraper.py            # Search page scraper
│   └── complex_scraper.py           # Residential complex scraper
│
├── analytics/                       # Data analysis
│   ├── __init__.py
│   └── jk_analytics.py             # JK analytics and rental yield
│
├── cli/                            # Command line interfaces
│   ├── __init__.py
│   ├── scheduler.py                 # Job scheduler
│   ├── launcher.py                  # Cron launcher
│   ├── cli_tool.py                 # Database management CLI
│   └── search_cli.py               # Search scraper CLI
│
└── config/                         # Configuration
    ├── __init__.py
    └── config.toml                 # TOML configuration file
```

## 🧪 Testing

### Test Individual Components

```bash
# Test flat scraper
python common/krisha_scraper.py

# Test complex scraper
python scrapers/complex_scraper.py

# Test database
python db/enhanced_database.py

# Test scheduler
python cli/scheduler.py --query-type rental --query-name "Meridian Apartments - 1 room rentals"

# Test JK analytics
python analytics/jk_analytics.py
```

### Test Complete Workflow

```bash
# 1. Update complex database
python main.py update-complexes

# 2. Run a test query
python main.py schedule --query-type rental --query-name "Meridian Apartments - 1 room rentals"

# 3. Check results
python main.py stats

# 4. Analyze the data
python main.py analyze --complex "Meridian" --area-max 35
```

### Test Main Entry Point

```bash
# Show help
python main.py --help

# Test all commands
python main.py stats
python main.py analyze --complex "Meridian" --area-max 35
```

## 📊 Example Results

### Rental Data (Meridian Apartments - 1 room)
- **Price Range**: 330,000 - 600,000 tenge
- **Area Range**: 33.0 - 35.0 m²
- **Average Price**: 465,000 tenge
- **Total Flats**: 20

### Sales Data (Meridian Apartments - 1 room)
- **Price Range**: 33,000,000 - 62,500,000 tenge
- **Area Range**: 33.0 - 35.0 m²
- **Average Price**: 47,750,000 tenge
- **Total Flats**: 20

## 🔍 API Discovery

The system uses the following Krisha.kz APIs:

1. **Individual Flat API**: `https://m.krisha.kz/analytics/aPriceAnalysis/?id={flat_id}`
2. **Residential Complex API**: `https://krisha.kz/complex/ajaxMapComplexGetAll?isSearch=1`
3. **Search Pages**: Server-side rendered HTML with embedded flat URLs

## 📝 Logging

The system provides comprehensive logging:

- **File Logging**: All operations logged to `scraper.log`
- **Console Output**: Real-time progress updates
- **Error Tracking**: Detailed error messages with stack traces
- **Performance Metrics**: Timing and statistics for each operation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is part of Orthanc Capital's quantitative real estate analysis toolkit.

## 🆘 Support

For issues or questions:
1. Check the logs in `scraper.log`
2. Review the configuration in `config.toml`
3. Test individual components
4. Check database integrity with `python enhanced_database.py`
