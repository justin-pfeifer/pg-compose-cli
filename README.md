# pg-compose-core

A core library for comparing PostgreSQL schemas from SQL files, Git repositories, or live databases. Provides both a command-line interface and a FastAPI web service for database migration reviews, schema versioning, and detecting changes between environments.

## Features

- **Multiple Source Types**: Compare schemas from SQL files, Git repositories, or live PostgreSQL connections
- **AST-based Diffing**: Uses PostgreSQL AST parsing for accurate schema comparison
- **Normalized Output**: Ignores whitespace and comment differences for cleaner diffs
- **Comprehensive Analysis**: Detects table, view, function, and other schema object changes
- **Web API**: FastAPI service for programmatic access
- **CLI Interface**: Command-line tool for direct usage
- **Schema Operations**: Sort, deploy, and merge schema definitions

## Installation

```bash
pip install pg-compose-core
```

Or install from source:

```bash
git clone <repository-url>
cd pg-compose-cli
pip install -e .
```

## Usage

### Command Line Interface

#### Basic Command Structure

```bash
pg-compose <source1> <source2> [options]
```

#### Source Types

1. **SQL Files**: Direct path to a `.sql` file
2. **Directories**: Path to a directory containing SQL files
3. **Git Repositories**: Git repository URL
4. **Live Database**: PostgreSQL connection string

#### Examples

```bash
# Compare two SQL files
pg-compose schema_v1.sql schema_v2.sql

# Compare SQL file with live database
pg-compose schema.sql "postgresql://user:pass@localhost:5432/dbname"

# Compare two Git repositories
pg-compose "git://github.com/user/repo1.git" "git://github.com/user/repo2.git"
```

### Web API

Start the API server:

```bash
uvicorn pg_compose_core.api:app --reload --host 0.0.0.0 --port 8000
```

#### Available Endpoints

- **`GET /`** - Project documentation (README)
- **`GET /docs`** - Interactive API documentation
- **`GET /health`** - Health check
- **`POST /compare`** - Compare two schema sources
- **`POST /sort`** - Sort SQL statements by dependencies
- **`POST /deploy`** - Deploy schema changes to database
- **`POST /merge`** - Merge two schemas

#### API Examples

```bash
# Compare schemas
curl -X POST "http://localhost:8000/compare" \
  -F "source_a=CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);" \
  -F "source_b=CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);" \
  -F "output_format=sql"

# Sort SQL statements
curl -X POST "http://localhost:8000/sort" \
  -F "sql_content=CREATE INDEX idx_users_name ON users(name); CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);" \
  -F "use_object_names=true" \
  -F "grant_handling=after"
```

## API Documentation

Visit `http://localhost:8000/docs` for interactive API documentation with examples and request/response schemas.

## Use Cases

### Database Migration Reviews

Compare your migration files against the current database state:

```bash
# CLI
pg-compose migrations/001_create_users.sql "postgresql://dev:pass@localhost:5432/dev_db"

# API
curl -X POST "http://localhost:8000/compare" \
  -F "source_a=@migrations/001_create_users.sql" \
  -F "source_b=postgresql://dev:pass@localhost:5432/dev_db"
```

### Feature Branch Comparison

Compare schema changes between feature branches:

```bash
# CLI
pg-compose ./feature/add_user_roles/ ./main/

# API
curl -X POST "http://localhost:8000/compare" \
  -F "source_a=git@github.com/user/repo.git#feature/add_user_roles" \
  -F "source_b=git@github.com/user/repo.git#main"
```

### Environment Validation

Ensure staging and production schemas match:

```bash
# CLI
pg-compose "postgresql://staging:pass@staging:5432/app" "postgresql://prod:pass@prod:5432/app"

# API
curl -X POST "http://localhost:8000/compare" \
  -F "source_a=postgresql://staging:pass@staging:5432/app" \
  -F "source_b=postgresql://prod:pass@prod:5432/app"
```

## Development

### Running Tests

```bash
# Install test dependencies
pip install -e ".[test]"

# Run tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=pg_compose_core --cov-report=html
```

### Project Structure

```
pg-compose-cli/
├── pg_compose_core/        # Main package
│   ├── cli/               # Command-line interface
│   │   └── cli.py        # CLI entry point
│   ├── api/              # FastAPI web service
│   │   ├── api.py        # Main FastAPI app
│   │   ├── health.py     # Health check endpoint
│   │   ├── compare.py    # Schema comparison endpoint
│   │   ├── sort.py       # SQL sorting endpoint
│   │   ├── deploy.py     # Deployment endpoint
│   │   ├── merge.py      # Schema merge endpoint
│   │   ├── home.py       # Home page (README)
│   │   ├── models.py     # Pydantic models
│   │   └── templates/    # Jinja2 templates
│   └── lib/              # Core library functionality
│       ├── compare.py    # Schema comparison logic
│       ├── extract.py    # SQL parsing and extraction
│       ├── sorter.py     # SQL dependency sorting
│       └── ast_objects.py # AST object definitions
├── tests/                # Test suite
│   ├── test_api.py       # API tests
│   ├── test_compare.py   # Comparison tests
│   ├── test_deploy.py    # Deployment tests
│   └── test_sorter.py    # Sorting tests
└── pyproject.toml        # Package configuration
```

## Requirements

- Python 3.8+
- PostgreSQL (for live database connections)
- Git (for repository comparisons)

## Dependencies

- `pglast`: PostgreSQL AST parsing (built on libpg_query)
- `psycopg[binary]`: PostgreSQL database connectivity
- `fastapi`: Web API framework
- `uvicorn`: ASGI server
- `jinja2`: Template engine
- `markdown`: Markdown rendering

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the GNU General Public License v3.0 (GPL-3.0).
See the LICENSE file for details.

## Future Plans

We intend to transition to our own direct Python wrapper for [libpg_query](https://github.com/pganalyze/libpg_query) in the future. This will allow for more control, performance, and flexibility in parsing PostgreSQL SQL, and will reduce our reliance on third-party AST libraries.
