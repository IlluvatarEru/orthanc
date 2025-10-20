# Database Documentation

This document describes the complete database schema for the Orthanc Capital Krisha.kz scraper system.

## Overview

The system uses SQLite as the database engine with multiple tables to store:
- Rental property data with historical tracking
- Sales property data with historical tracking  
- Residential complex information
- User favorites
- Exchange rate data (FX rates)
- JK performance snapshots

## Database Tables

### 1. `rental_flats` - Rental Property Data

Stores rental flat information with historical tracking by query date. This table captures rental listings from Krisha.kz with full property details.

**Schema:**
```sql
CREATE TABLE rental_flats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flat_id TEXT NOT NULL,                    -- Unique flat identifier from Krisha.kz
    price INTEGER NOT NULL,                   -- Monthly rent in tenge (KZT)
    area REAL NOT NULL,                       -- Area in square meters
    residential_complex TEXT,                 -- Complex name (e.g., "Meridian Apartments")
    floor INTEGER,                            -- Floor number (NULL if not specified)
    total_floors INTEGER,                     -- Total floors in building (NULL if not specified)
    construction_year INTEGER,                -- Year of construction (NULL if not specified)
    parking TEXT,                             -- Parking information (NULL if not specified)
    description TEXT NOT NULL,                -- Full property description
    url TEXT NOT NULL,                        -- Original listing URL on Krisha.kz
    query_date DATE NOT NULL,                 -- Date when scraped (YYYY-MM-DD format)
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When record was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When record was last updated
    UNIQUE(flat_id, query_date)              -- Prevents duplicate entries per day
);
```

**Column Details:**
- `id`: Auto-incrementing primary key
- `flat_id`: Unique identifier from Krisha.kz (e.g., "12345678")
- `price`: Monthly rental price in tenge (KZT)
- `area`: Property area in square meters
- `residential_complex`: Name of the residential complex
- `floor`: Floor number where the flat is located
- `total_floors`: Total number of floors in the building
- `construction_year`: Year the building was constructed
- `parking`: Parking availability and type
- `description`: Full property description from the listing
- `url`: Complete URL to the original listing
- `query_date`: Date when the data was scraped (for historical tracking)
- `scraped_at`: Timestamp when the record was created
- `updated_at`: Timestamp when the record was last modified

**Indexes:**
```sql
CREATE INDEX idx_rental_flat_id ON rental_flats(flat_id);
CREATE INDEX idx_rental_query_date ON rental_flats(query_date);
CREATE INDEX idx_rental_price ON rental_flats(price);
CREATE INDEX idx_rental_area ON rental_flats(area);
```

### 2. `sales_flats` - Sales Property Data

Stores sales flat information with historical tracking by query date. This table captures property sales listings from Krisha.kz.

**Schema:**
```sql
CREATE TABLE sales_flats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flat_id TEXT NOT NULL,                    -- Unique flat identifier from Krisha.kz
    price INTEGER NOT NULL,                   -- Sale price in tenge (KZT)
    area REAL NOT NULL,                       -- Area in square meters
    residential_complex TEXT,                 -- Complex name
    floor INTEGER,                            -- Floor number (NULL if not specified)
    total_floors INTEGER,                     -- Total floors in building (NULL if not specified)
    construction_year INTEGER,                -- Year of construction (NULL if not specified)
    parking TEXT,                             -- Parking information (NULL if not specified)
    description TEXT NOT NULL,                -- Full property description
    url TEXT NOT NULL,                        -- Original listing URL on Krisha.kz
    query_date DATE NOT NULL,                 -- Date when scraped (YYYY-MM-DD format)
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When record was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When record was last updated
    UNIQUE(flat_id, query_date)              -- Prevents duplicate entries per day
);
```

**Column Details:**
- Same structure as `rental_flats` but `price` represents the sale price instead of monthly rent

**Indexes:**
```sql
CREATE INDEX idx_sales_flat_id ON sales_flats(flat_id);
CREATE INDEX idx_sales_query_date ON sales_flats(query_date);
CREATE INDEX idx_sales_price ON sales_flats(price);
CREATE INDEX idx_sales_area ON sales_flats(area);
```

### 3. `residential_complexes` - Complex Mapping

Maps Krisha.kz complex IDs to readable names and location information. This table provides a reference for residential complex data.

**Schema:**
```sql
CREATE TABLE residential_complexes (
    id INTEGER PRIMARY KEY,
    complex_id TEXT UNIQUE NOT NULL,          -- Krisha.kz complex ID
    name TEXT NOT NULL,                       -- Complex name
    city TEXT,                                -- City name (e.g., "Алматы")
    district TEXT,                            -- District name (e.g., "Алмалинский")
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Column Details:**
- `id`: Auto-incrementing primary key
- `complex_id`: Unique identifier from Krisha.kz (e.g., "2758")
- `name`: Human-readable complex name
- `city`: City where the complex is located
- `district`: District within the city
- `created_at`: When the complex record was created

**Indexes:**
```sql
CREATE INDEX idx_complex_id ON residential_complexes(complex_id);
```

### 4. `favorites` - User Favorites

Stores user-selected favorite properties for quick access and comparison.

**Schema:**
```sql
CREATE TABLE favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flat_id TEXT NOT NULL,                    -- Unique flat identifier
    flat_type TEXT NOT NULL CHECK (flat_type IN ('rental', 'sale')),  -- Type of listing
    price INTEGER NOT NULL,                   -- Price in tenge
    area REAL NOT NULL,                       -- Area in square meters
    residential_complex TEXT,                 -- Complex name
    floor INTEGER,                            -- Floor number
    total_floors INTEGER,                     -- Total floors in building
    construction_year INTEGER,                -- Year of construction
    parking TEXT,                             -- Parking information
    description TEXT NOT NULL,                -- Property description
    url TEXT NOT NULL,                        -- Original listing URL
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When added to favorites
    notes TEXT,                               -- User notes about the property
    UNIQUE(flat_id, flat_type)               -- Prevents duplicate favorites
);
```

**Column Details:**
- `id`: Auto-incrementing primary key
- `flat_id`: Unique identifier from Krisha.kz
- `flat_type`: Either 'rental' or 'sale'
- `price`: Price in tenge (monthly rent for rentals, sale price for sales)
- `area`: Property area in square meters
- `residential_complex`: Name of the residential complex
- `floor`: Floor number
- `total_floors`: Total floors in building
- `construction_year`: Year of construction
- `parking`: Parking information
- `description`: Property description
- `url`: Original listing URL
- `added_at`: When the property was added to favorites
- `notes`: Optional user notes about the property

### 5. `mid_prices` - Exchange Rate Data

Stores currency exchange rates for financial analysis and reporting. This table is automatically created and managed by the integrated `OrthancDB` class.

**Schema:**
```sql
CREATE TABLE mid_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    currency TEXT NOT NULL,                   -- Currency code (e.g., "USD", "EUR")
    rate REAL NOT NULL,                       -- Exchange rate to KZT
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- When rate was fetched
);
```

**Column Details:**
- `id`: Auto-incrementing primary key
- `currency`: Three-letter currency code (USD, EUR, GBP, etc.)
- `rate`: Exchange rate (how many KZT per unit of currency)
- `fetched_at`: When the exchange rate was retrieved

**Indexes:**
```sql
CREATE INDEX idx_currency_fetched ON mid_prices(currency, fetched_at);
```

**Operations:**
- Insert rates: `db.insert_exchange_rate(currency, rate, timestamp)`
- Get latest rate: `db.get_latest_rate(currency)`
- Get rates by date range: `db.get_rates_by_date_range(currency, start_date, end_date)`
- Delete rates at timestamp: `db.delete_rate_at_timestamp(timestamp)`
- Get all currencies: `db.get_all_currencies()`

### 6. `jk_performance_snapshots` - Residential Complex Performance Data

Stores comprehensive performance snapshots for residential complexes, including rental yields, price analysis, and statistics by flat type.

**Schema:**
```sql
CREATE TABLE jk_performance_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    residential_complex TEXT NOT NULL,         -- Complex name
    snapshot_date DATE NOT NULL,               -- Date of snapshot (YYYY-MM-DD)
    
    -- Overall statistics
    total_rental_flats INTEGER DEFAULT 0,
    total_sales_flats INTEGER DEFAULT 0,
    
    -- Rental yield statistics
    median_rental_yield REAL,
    mean_rental_yield REAL,
    min_rental_yield REAL,
    max_rental_yield REAL,
    
    -- Price per m2 statistics
    min_rent_price_per_m2 REAL,
    max_rent_price_per_m2 REAL,
    mean_rent_price_per_m2 REAL,
    median_rent_price_per_m2 REAL,
    
    min_sales_price_per_m2 REAL,
    max_sales_price_per_m2 REAL,
    mean_sales_price_per_m2 REAL,
    median_sales_price_per_m2 REAL,
    
    -- Statistics by flat type (Studio, 1BR, 2BR, 3BR+)
    studio_rental_count INTEGER DEFAULT 0,
    studio_sales_count INTEGER DEFAULT 0,
    studio_median_rent_yield REAL,
    studio_median_rent_price_per_m2 REAL,
    studio_median_sales_price_per_m2 REAL,
    
    -- ... (similar fields for 1BR, 2BR, 3BR+)
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(residential_complex, snapshot_date)
);
```

**Operations:**
- Create snapshot: `db.create_jk_performance_snapshot(complex_name, date)`
- Get snapshots: `db.get_jk_performance_snapshots(complex_name, start_date, end_date)`

## Common Queries and Examples

### Basic Data Retrieval

**Get all rental flats for a specific date:**
```sql
SELECT * FROM rental_flats WHERE query_date = '2025-01-15';
```

**Get all sales flats for a specific date:**
```sql
SELECT * FROM sales_flats WHERE query_date = '2025-01-15';
```

**Get all complexes in Almaty:**
```sql
SELECT * FROM residential_complexes WHERE city = 'Алматы';
```

### Statistical Analysis

**Average rental price by complex:**
```sql
SELECT 
    residential_complex, 
    AVG(price) as avg_price, 
    COUNT(*) as count,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM rental_flats 
WHERE query_date = '2025-01-15'
GROUP BY residential_complex
ORDER BY avg_price DESC;
```

**Average sale price by complex:**
```sql
SELECT 
    residential_complex, 
    AVG(price) as avg_price, 
    COUNT(*) as count,
    AVG(price/area) as price_per_sqm
FROM sales_flats 
WHERE query_date = '2025-01-15'
GROUP BY residential_complex
ORDER BY avg_price DESC;
```

**Price per square meter analysis:**
```sql
SELECT 
    residential_complex,
    AVG(price/area) as avg_price_per_sqm,
    COUNT(*) as listings
FROM sales_flats 
WHERE query_date = '2025-01-15'
GROUP BY residential_complex
HAVING listings >= 3
ORDER BY avg_price_per_sqm DESC;
```

### Historical Analysis

**Rental price trends over time:**
```sql
SELECT 
    query_date,
    residential_complex,
    AVG(price) as avg_rental_price,
    COUNT(*) as listings
FROM rental_flats 
WHERE residential_complex = 'Meridian Apartments'
    AND query_date >= '2024-01-01'
GROUP BY query_date, residential_complex
ORDER BY query_date;
```

**Sales price trends over time:**
```sql
SELECT 
    query_date,
    residential_complex,
    AVG(price) as avg_sale_price,
    COUNT(*) as listings
FROM sales_flats 
WHERE residential_complex = 'Meridian Apartments'
    AND query_date >= '2024-01-01'
GROUP BY query_date, residential_complex
ORDER BY query_date;
```

### Investment Analysis

**Rental yield calculation (annual rent / sale price):**
```sql
SELECT 
    r.residential_complex,
    AVG(r.price * 12.0 / s.price) as rental_yield,
    COUNT(*) as sample_size,
    AVG(r.price) as avg_rental,
    AVG(s.price) as avg_sale_price
FROM rental_flats r
JOIN sales_flats s ON r.residential_complex = s.residential_complex 
    AND r.query_date = s.query_date
    AND ABS(r.area - s.area) < 5  -- Similar area (±5 sqm)
WHERE r.query_date = '2025-01-15'
GROUP BY r.residential_complex
HAVING sample_size > 1
ORDER BY rental_yield DESC;
```

**Property comparison by area:**
```sql
SELECT 
    residential_complex,
    area,
    AVG(price) as avg_price,
    COUNT(*) as listings
FROM rental_flats 
WHERE query_date = '2025-01-15'
    AND area BETWEEN 50 AND 70
GROUP BY residential_complex, area
ORDER BY avg_price DESC;
```

### Data Quality and Maintenance

**Find duplicate entries:**
```sql
SELECT flat_id, query_date, COUNT(*) as count
FROM rental_flats 
GROUP BY flat_id, query_date
HAVING count > 1;
```

**Get latest data for each flat:**
```sql
SELECT r1.*
FROM rental_flats r1
INNER JOIN (
    SELECT flat_id, MAX(query_date) as max_date
    FROM rental_flats
    GROUP BY flat_id
) r2 ON r1.flat_id = r2.flat_id AND r1.query_date = r2.max_date;
```

**Clean up old data (older than 6 months):**
```sql
DELETE FROM rental_flats 
WHERE query_date < date('now', '-6 months');

DELETE FROM sales_flats 
WHERE query_date < date('now', '-6 months');
```

### Exchange Rate Analysis

**Get latest exchange rates:**
```sql
SELECT currency, rate, fetched_at 
FROM mid_prices 
WHERE fetched_at = (
    SELECT MAX(fetched_at) 
    FROM mid_prices 
    WHERE currency = mid_prices.currency
)
ORDER BY currency;
```

**Exchange rate trends over time:**
```sql
SELECT 
    currency,
    DATE(fetched_at) as date,
    AVG(rate) as avg_rate,
    MIN(rate) as min_rate,
    MAX(rate) as max_rate
FROM mid_prices 
WHERE fetched_at >= date('now', '-30 days')
GROUP BY currency, DATE(fetched_at)
ORDER BY currency, date;
```

**Convert property prices to EUR:**
```sql
SELECT 
    r.residential_complex,
    r.price as price_kzt,
    r.price / e.rate as price_eur,
    e.rate as eur_rate,
    r.query_date
FROM rental_flats r
JOIN (
    SELECT currency, rate, 
           ROW_NUMBER() OVER (ORDER BY fetched_at DESC) as rn
    FROM mid_prices 
    WHERE currency = 'EUR'
) e ON e.rn = 1
WHERE r.query_date = '2025-01-15';
```

**Delete exchange rates at specific timestamp:**
```sql
DELETE FROM mid_prices 
WHERE fetched_at = '2025-01-15 10:30:00';
```

## Database Operations

### Integrated Database Class

All database operations are handled through the unified `OrthancDB` class in `db/src/write_read_database.py`. This class provides:

**Flat Operations:**
- `insert_rental_flat(flat_info, url, query_date, flat_type)`
- `insert_sales_flat(flat_info, url, query_date, flat_type)`
- `get_rental_flats_by_date(query_date, limit)`
- `get_sales_flats_by_date(query_date, limit)`
- `get_historical_statistics(start_date, end_date, jk)`

**FX Operations:**
- `insert_exchange_rate(currency, rate, fetched_at)`
- `get_latest_rate(currency)`
- `get_rates_by_date_range(currency, start_date, end_date)`
- `delete_rate_at_timestamp(timestamp)`
- `get_all_currencies()`

**Complex Operations:**
- `insert_residential_complex(complex_id, name, city, district)`
- `get_residential_complex_by_id(complex_id)`
- `get_all_residential_complexes()`
- `get_flats_by_complex(complex_name, flat_type)`

**Favorites Operations:**
- `add_to_favorites(flat_id, flat_type, notes)`
- `remove_from_favorites(flat_id, flat_type)`
- `get_favorites()`
- `is_favorite(flat_id, flat_type)`

**Performance Snapshots:**
- `create_jk_performance_snapshot(residential_complex, snapshot_date)`
- `get_jk_performance_snapshots(residential_complex, start_date, end_date)`

### Convenience Functions

The module also provides convenience functions for common operations:
- `save_rental_flat_to_db(flat_info, url, query_date, flat_type, db_path)`
- `save_sales_flat_to_db(flat_info, url, query_date, flat_type, db_path)`
- `save_exchange_rate_to_db(currency, rate, fetched_at, db_path)`
- `get_latest_exchange_rate(currency, db_path)`

## Data Relationships

1. **rental_flats** ↔ **residential_complexes**: Linked by `residential_complex` name
2. **sales_flats** ↔ **residential_complexes**: Linked by `residential_complex` name  
3. **favorites** ↔ **rental_flats/sales_flats**: Linked by `flat_id` and `flat_type`
4. **jk_performance_snapshots** ↔ **rental_flats/sales_flats**: Linked by `residential_complex` name
5. **mid_prices**: Independent table for exchange rate data

## Performance Considerations

- All tables have appropriate indexes on frequently queried columns
- The `UNIQUE(flat_id, query_date)` constraint prevents duplicate daily entries
- Historical data is preserved by date, allowing trend analysis
- Large datasets should be periodically cleaned up to maintain performance

## Backup and Maintenance

**Create database backup:**
```bash
sqlite3 flats.db ".backup backup_$(date +%Y%m%d_%H%M%S).db"
```

**Vacuum database to reclaim space:**
```sql
VACUUM;
```

**Analyze table statistics:**
```sql
ANALYZE;
```

## Notes

- All prices are stored in tenge (KZT)
- Areas are stored in square meters
- Dates use ISO format (YYYY-MM-DD)
- Timestamps use SQLite's CURRENT_TIMESTAMP
- The system automatically handles duplicate prevention and data integrity
- Exchange rates are stored as KZT per unit of foreign currency
- All database operations are handled through the unified `OrthancDB` class
- FX operations are integrated into the main database class (no separate `DatabaseFX` class)
- The `mid_prices` table is automatically created when initializing the database
- Performance snapshots provide comprehensive analytics for residential complexes 