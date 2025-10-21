# JK Blacklist Management System

This document describes the JK (Residential Complex) blacklist management system that allows you to exclude specific JKs from scraping operations.

## Overview

The blacklist system consists of:
- A database table to store blacklisted JKs
- Database functions for managing blacklisted JKs
- A command-line tool for easy management
- Automatic exclusion from scraping operations

## Database Schema

The `blacklisted_jks` table has the following structure:
```sql
CREATE TABLE blacklisted_jks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    krisha_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    blacklisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);
```

## Command Line Usage

### Basic Commands

```bash
# List all blacklisted JKs
python blacklist_management.py --action list

# Add a JK to blacklist (traditional method - requires both name and Krisha ID)
python blacklist_management.py --action add --name "Problematic JK" --krisha-id "jk_123" --notes "Causes scraping errors"

# Add a JK to blacklist by name only (auto-finds Krisha ID from residential_complexes table)
python blacklist_management.py --action add-by-name --name "Problematic JK" --notes "Causes scraping errors"

# Add a JK to blacklist by ID only (auto-finds name from residential_complexes table)
python blacklist_management.py --action add-by-id --jk-id "jk_123" --notes "Causes scraping errors"

# Remove JK by name (traditional method)
python blacklist_management.py --action remove --name "Problematic JK"

# Remove JK by Krisha ID (traditional method)
python blacklist_management.py --action remove --krisha-id "jk_123"

# Remove JK by name only (auto-finds Krisha ID)
python blacklist_management.py --action remove-by-name --name "Problematic JK"

# Remove JK by ID only (auto-finds name)
python blacklist_management.py --action remove-by-id --jk-id "jk_123"
```

### Advanced Usage

```bash
# Use custom database path
python blacklist_management.py --action list --db-path "custom_flats.db"

# Add JK without notes
python blacklist_management.py --action add --name "Another JK" --krisha-id "jk_456"
```

## Programmatic Usage

```python
from db.src.write_read_database import OrthancDB

# Initialize database
db = OrthancDB("flats.db")

# Add JK to blacklist (traditional method)
db.blacklist_jk("jk_123", "Problematic JK", "Causes errors")

# Add JK to blacklist by name only (auto-finds Krisha ID from residential_complexes table)
db.blacklist_jk_by_name("Problematic JK", "Causes errors")

# Add JK to blacklist by ID only (auto-finds name from residential_complexes table)
db.blacklist_jk_by_id("jk_123", "Causes errors")

# Check if JK is blacklisted
is_blacklisted = db.is_jk_blacklisted(name="Problematic JK")

# Remove from blacklist (traditional method)
db.whitelist_jk(name="Problematic JK")

# Remove from blacklist by name only (auto-finds Krisha ID)
db.whitelist_jk_by_name("Problematic JK")

# Remove from blacklist by ID only (auto-finds name)
db.whitelist_jk_by_id("jk_123")

# Get all blacklisted JKs
blacklisted_jks = db.get_blacklisted_jks()

# Bulk operations
jks_to_blacklist = [
    {"krisha_id": "jk_001", "name": "JK One", "notes": "Low quality"},
    {"krisha_id": "jk_002", "name": "JK Two", "notes": "Too many errors"}
]
db.blacklist_jks(jks_to_blacklist)
```

## Integration with Scraping

The blacklist system is automatically integrated with the scraping operations:

```bash
# Run scraping (automatically excludes blacklisted JKs)
python -m scrapers.launch.launch_scrapping_all_jks --mode immediate --rentals --sales
```

The scraping system will:
1. Query all JKs from the database
2. Automatically exclude blacklisted JKs
3. Log which JKs are excluded for transparency

## Database Functions

### Core Functions

- `blacklist_jk(krisha_id, name, notes=None)`: Add single JK to blacklist (traditional method)
- `blacklist_jk_by_name(name, notes="")`: Add JK to blacklist by name only (auto-finds Krisha ID from residential_complexes table)
- `blacklist_jk_by_id(jk_id, notes="")`: Add JK to blacklist by ID only (auto-finds name from residential_complexes table)
- `blacklist_jks(jk_list)`: Bulk blacklist multiple JKs
- `whitelist_jk(krisha_id=None, name=None)`: Remove single JK from blacklist (traditional method)
- `whitelist_jk_by_name(name)`: Remove JK from blacklist by name only (auto-finds Krisha ID from residential_complexes table)
- `whitelist_jk_by_id(jk_id)`: Remove JK from blacklist by ID only (auto-finds name from residential_complexes table)
- `whitelist_jks(jk_list)`: Bulk whitelist multiple JKs
- `get_blacklisted_jks()`: Get all blacklisted JKs
- `is_jk_blacklisted(krisha_id=None, name=None)`: Check if JK is blacklisted

### Function Parameters

- `krisha_id`: Unique Krisha ID for the JK
- `name`: Human-readable name of the JK
- `notes`: Optional notes about why the JK is blacklisted
- `jk_list`: List of dictionaries with JK information

## Examples

### Example 1: Basic Blacklist Management

```bash
# List current blacklisted JKs
python blacklist_management.py --action list

# Add a problematic JK
python blacklist_management.py --action add --name "Faulty JK" --krisha-id "faulty_123" --notes "Frequently causes timeouts"

# Verify it was added
python blacklist_management.py --action list

# Remove it later
python blacklist_management.py --action remove --name "Faulty JK"
```

### Example 2: Bulk Operations

```python
from db.src.write_read_database import OrthancDB

db = OrthancDB("flats.db")

# Blacklist multiple JKs
problematic_jks = [
    {"krisha_id": "jk_001", "name": "Old JK", "notes": "Outdated information"},
    {"krisha_id": "jk_002", "name": "Error JK", "notes": "Frequent API errors"},
    {"krisha_id": "jk_003", "name": "Slow JK", "notes": "Very slow response times"}
]

count = db.blacklist_jks(problematic_jks)
print(f"Blacklisted {count} JKs")
```

### Example 3: Integration with Scraping

```python
from scrapers.launch.launch_scrapping_all_jks import get_all_jks_from_db

# Get JKs for scraping (automatically excludes blacklisted)
jks = get_all_jks_from_db("flats.db")
print(f"Found {len(jks)} JKs available for scraping")
```

## Error Handling

The system includes comprehensive error handling:

- **Duplicate entries**: Prevents adding the same JK twice
- **Missing parameters**: Validates required parameters
- **Database errors**: Handles database connection issues
- **Invalid actions**: Provides clear error messages

## Logging

The system provides detailed logging:

- **Info level**: Successful operations
- **Warning level**: Non-critical issues (e.g., JK already blacklisted)
- **Error level**: Critical errors that prevent operation

## Best Practices

1. **Use descriptive notes**: Always include notes explaining why a JK is blacklisted
2. **Regular cleanup**: Periodically review and remove JKs that are no longer problematic
3. **Monitor logs**: Check scraping logs to see which JKs are being excluded
4. **Test before production**: Use the test script to verify functionality

## Troubleshooting

### Common Issues

1. **JK not found in blacklist**: Ensure you're using the correct name or Krisha ID
2. **Database connection errors**: Check database file path and permissions
3. **Permission errors**: Ensure the script has write access to the database

### Debug Mode

Enable debug logging by modifying the logging level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Testing

Run the test script to verify functionality:

```bash
python test_blacklist_management.py
```

This will test all major operations and verify the system works correctly.
