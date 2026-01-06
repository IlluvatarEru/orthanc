# Claude Documentation - Orthanc Capital

## Project Overview

Orthanc Capital is a real estate data scraping and analytics platform focused on the Kazakhstan real estate market. The system scrapes data from Krisha.kz (primary real estate platform), analyzes residential complexes (referred to as "JKs" - residential complexes), and provides comprehensive analytics for sales and rental opportunities.

### Core Purpose
- **Data Collection**: Automated scraping of real estate listings from Krisha.kz
- **Data Analysis**: Analytics for flats, sales, and rental markets
- **API Access**: REST API for accessing analyzed data
- **Decision Support**: Tools for identifying investment opportunities

## Project Structure

```
orthanc/
├── api/                    # FastAPI REST API for data access
│   ├── src/               # API route handlers and logic
│   ├── launch/            # API launcher scripts
│   └── test/              # API tests
├── scrapers/              # Web scrapers for data collection
│   ├── src/               # Scraper implementations
│   ├── launch/            # Scraper launcher scripts
│   └── test/              # Scraper tests
├── db/                    # Database operations and models
│   ├── src/               # Database read/write operations
│   ├── launch/            # Database utilities
│   └── test/              # Database tests
├── analytics/             # Data analytics and insights
│   ├── src/               # Analytics implementations
│   └── test/              # Analytics tests
├── price/                 # Price-related functionality
│   ├── src/               # Price analysis and calculations
│   └── test/              # Price tests
├── frontend/              # Frontend components
│   ├── src/               # Frontend source
│   └── test/              # Frontend tests
├── common/                # Shared utilities and helpers
│   └── src/               # Common utilities
├── config/                # Configuration management
├── doc/                   # Additional documentation
└── .github/workflows/     # CI/CD pipelines
```

## Technology Stack

### Core Technologies
- **Python**: 3.11+ (primary language)
- **Database**: SQLite (`flats.db`)
- **Web Scraping**: beautifulsoup4, requests
- **API Framework**: FastAPI with uvicorn
- **Legacy API**: Flask (some endpoints)
- **Testing**: pytest

### Development Tools
- **Code Quality**: Ruff (linting and formatting)
- **Version Control**: Git
- **CI/CD**: GitHub Actions
- **Documentation**: Markdown

### Key Dependencies
```
beautifulsoup4==4.14.2
fastapi==0.120.0
Flask==2.2.5
pytest==7.4.0
Requests==2.32.5
toml==0.10.2
uvicorn==0.38.0
```

## Development Standards

### Code Style
- **Formatting**: Use Ruff for code formatting
  - Command: `ruff format .`
  - Auto-formats on save (recommended)
- **Linting**: Use Ruff for linting
  - Command: `ruff check .`
  - Fix automatically: `ruff check --fix .`
- **Module Structure**: Follow Python package conventions
  - Each module has `__init__.py`
  - Organized into `src/`, `launch/`, `test/` subdirectories

### Testing Standards
- **Test Framework**: pytest
- **Test Location**: Tests in `test/` directory within each module
- **Running Tests**:
  - All tests: `pytest`
  - Specific module: `pytest api/test/ -v`
  - Specific test: `pytest path/to/test.py::TestClass::test_method -v`
- **CI/CD**: Tests run automatically on push to branches matching `main`, `master`, `develop`, `feat/**`

### Git Workflow
- **Branch Naming**:
  - Features: `feat/feature-name`
  - Fixes: `fix/issue-description`
  - Claude branches: `claude/task-description-{sessionId}`
- **Commits**: Clear, descriptive commit messages
- **CI/CD**: GitHub Actions runs tests on all PRs

### Documentation Standards
- **Module Documentation**: Each major module has a README.md
- **Code Comments**: Document complex logic and business rules
- **API Documentation**: Auto-generated via FastAPI/Swagger at `/docs`

## Key Concepts

### Residential Complexes (JKs)
- **JK**: Residential complex (Russian: "ЖК" - жилой комплекс)
- **Krisha ID**: Unique identifier from Krisha.kz platform
- **Blacklist**: System to exclude problematic JKs from scraping

### Data Types
- **Flats**: Individual apartments/units
- **Sales**: Purchase listings
- **Rentals**: Rental listings
- **Flat Types**: Categorized (e.g., 1BR, 2BR, 3BR, Studio)

### Database Schema
- **Main Database**: `flats.db` (SQLite)
- **Key Tables**:
  - `residential_complexes`: JK metadata
  - `flats`: Individual flat data
  - `sales`: Sale listings
  - `rentals`: Rental listings
  - `blacklisted_jks`: Excluded JKs

## Common Operations

### Running Scrapers
```bash
# Daily sales scraping (background)
nohup python -m scrapers.launch.launch_scraping_all_jks --mode daily-sales > daily_sales.out 2>&1 &

# Daily rentals scraping (background)
nohup python -m scrapers.launch.launch_scraping_all_jks --mode daily-rentals > daily_rentals.out 2>&1 &

# Immediate scraping (one-off)
python -m scrapers.launch.launch_scraping_all_jks --mode immediate --sales --rentals

# Fetch all residential complexes
python -m scrapers.launch.launch_scraping_all_jks --mode fetch-jks

# Scrape specific JK
python -m scrapers.launch.launch_scraping_all_jks --mode scrape-jk --jk-name "Meridian" --rentals --sales
```

### Running API
```bash
# Development mode with auto-reload
python -m api.launch.launch_api --reload

# Production mode
python -m api.launch.launch_api --host 0.0.0.0 --port 8000

# Access documentation
# http://localhost:8000/docs (Swagger UI)
# http://localhost:8000/redoc (ReDoc)
```

### Blacklist Management
```bash
# List blacklisted JKs
python blacklist_management.py --action list

# Add JK to blacklist (by name - auto-finds ID)
python blacklist_management.py --action add-by-name --name "Problematic JK" --notes "Reason"

# Remove from blacklist
python blacklist_management.py --action remove-by-name --name "Problematic JK"
```

### Testing
```bash
# Run all tests
pytest

# Run specific module tests
pytest api/test/ -v
pytest scrapers/test/ -v

# Run with coverage
pytest --cov=. --cov-report=html
```

### Code Quality
```bash
# Format code
ruff format .

# Lint code
ruff check .

# Auto-fix linting issues
ruff check --fix .
```

## API Endpoints

### Flat Analysis
- `GET /api/flats/search` - Search flats with filters
- `GET /api/flats/{flat_id}` - Get specific flat details
- `GET /api/flats/stats/summary` - Get summary statistics

### JK Sales Analysis
- `GET /api/jks/sales/` - List all residential complexes
- `GET /api/jks/sales/{jk_name}/summary` - Get sales summary
- `GET /api/jks/sales/{jk_name}/analysis` - Get comprehensive analysis
- `GET /api/jks/sales/{jk_name}/opportunities` - Get investment opportunities

### JK Rentals Analysis
- `GET /api/jks/rentals/` - List JKs with rental data
- `GET /api/jks/rentals/{jk_name}/summary` - Get rental summary
- `GET /api/jks/rentals/{jk_name}/rentals` - Get rental listings
- `GET /api/jks/rentals/{jk_name}/price-trends` - Get price trends
- `GET /api/jks/rentals/stats/overview` - Get market overview

## Development Guidelines

### When Adding Features
1. **Follow Module Structure**: Add code to appropriate module (`api/`, `scrapers/`, `db/`, etc.)
2. **Write Tests**: Add pytest tests in the module's `test/` directory
3. **Update Documentation**: Update relevant README.md files
4. **Format Code**: Run `ruff format .` before committing
5. **Lint Code**: Run `ruff check --fix .` to catch issues
6. **Test Locally**: Run `pytest` to ensure all tests pass
7. **Commit**: Use clear, descriptive commit messages

### When Fixing Bugs
1. **Reproduce**: Understand the issue and reproduce it
2. **Write Test**: Add test that captures the bug (if applicable)
3. **Fix**: Implement the fix
4. **Verify**: Run tests to ensure fix works
5. **Format & Lint**: Run Ruff before committing

### When Adding Scrapers
1. **Location**: Add to `scrapers/src/`
2. **Follow Patterns**: Look at existing scrapers for patterns
3. **Error Handling**: Implement robust error handling for network issues
4. **Respect Blacklist**: Check blacklist before scraping
5. **Logging**: Add comprehensive logging for debugging
6. **Rate Limiting**: Be respectful to source websites

### When Modifying Database
1. **Location**: Database operations go in `db/src/write_read_database.py`
2. **Use OrthancDB Class**: Central database class for all operations
3. **Transactions**: Use appropriate transaction handling
4. **Testing**: Add tests in `db/test/`
5. **Migrations**: Document schema changes

## Important Patterns

### Database Access
```python
from db.src.write_read_database import OrthancDB

db = OrthancDB("flats.db")
# Use db methods for all database operations
```

### Command-Line Scripts
- Use `argparse` for CLI arguments
- Follow pattern: `python -m module.launch.script_name --args`
- Add `--help` documentation

### Module Imports
- Use absolute imports from project root
- Example: `from db.src.write_read_database import OrthancDB`
- Set `PYTHONPATH` to project root when needed

### Error Handling
- Comprehensive error handling in scrapers (network issues common)
- Logging at appropriate levels (INFO, WARNING, ERROR)
- Graceful degradation when possible

## Additional Documentation

- **API Documentation**: See `api/README.md`
- **Blacklist System**: See `BLACKLIST_MANAGEMENT.md`
- **Database Schema**: See `doc/db.md`
- **Main Usage**: See `README.md`

## Notes for AI Assistants (Claude)

### Project Context
- This is a production system for real estate investment analysis
- Data accuracy is critical
- Scraping must be respectful (rate limiting, error handling)
- Database integrity is paramount

### Code Modification Guidelines
1. **Always run tests** after making changes
2. **Format with Ruff** before committing
3. **Follow existing patterns** in the codebase
4. **Add documentation** for new features
5. **Respect module boundaries** - don't mix concerns
6. **Consider blacklist system** when working with scrapers
7. **Update READMEs** when adding significant features

### Common Tasks
- Adding new scraper? → `scrapers/src/`
- Adding API endpoint? → `api/src/`
- Database operation? → `db/src/write_read_database.py`
- Analytics feature? → `analytics/src/`
- Bug fix? → Add test first, then fix
- Configuration? → `config/`

### Quality Checklist
- [ ] Code formatted with Ruff
- [ ] Code linted with Ruff
- [ ] Tests added/updated
- [ ] Tests passing locally
- [ ] Documentation updated
- [ ] Commit message is clear
- [ ] No hardcoded credentials or secrets
- [ ] Error handling is robust
- [ ] Logging is appropriate

## Contact & Resources

- **GitHub**: Check issues and PRs for current work
- **CI/CD**: GitHub Actions runs tests automatically
- **Testing**: `pytest` is configured and ready to use
- **Documentation**: Inline code documentation + module READMEs
