# pg-compose-cli

A command-line tool for comparing PostgreSQL schemas from SQL files, Git repositories, or live databases. Perfect for database migration reviews, schema versioning, and detecting changes between environments.

## Features

- **Multiple Source Types**: Compare schemas from SQL files, Git repositories, or live PostgreSQL connections
- **Directory Comparison**: Compare entire directories of SQL files (useful for feature branches)
- **AST-based Diffing**: Uses PostgreSQL AST parsing for accurate schema comparison
- **Normalized Output**: Ignores whitespace and comment differences for cleaner diffs
- **Comprehensive Analysis**: Detects table, view, function, and other schema object changes
- **Schema Deployment**: Generate and apply schema changes with safety options

## Installation

```bash
pip install pg-compose-cli
```

Or install from source:

```bash
git clone <repository-url>
cd pg-compose-cli
pip install -e .
```

## Usage

### Basic Command Structure

```bash
pg-compose <source1> <source2> [options]
```

### Source Types

1. **SQL Files**: Direct path to a `.sql` file
2. **Directories**: Path to a directory containing SQL files
3. **Git Repositories**: Git repository URL
4. **Live Database**: PostgreSQL connection string

### Examples

#### Compare Two SQL Files

```bash
pg-compose schema_v1.sql schema_v2.sql
```

#### Compare Directories (Feature Branch Style)

```bash
pg-compose ./feature_branch/schema/ ./main_branch/schema/
```

#### Compare SQL File with Live Database

```bash
pg-compose schema.sql "postgresql://user:pass@localhost:5432/dbname"
```

#### Compare Two Git Repositories

```bash
pg-compose "git://github.com/user/repo1.git" "git://github.com/user/repo2.git"
```

#### Deploy Schema Changes

Generate deployment commands and save to a file:

```bash
pg-compose schema_v1.sql schema_v2.sql --deploy migration.sql
```

Preview what would be deployed without applying changes:

```bash
pg-compose schema_v1.sql schema_v2.sql --deploy migration.sql --dry-run
```

### Output Format

The tool outputs a structured diff showing:

- **Added Objects**: New tables, views, functions, etc.
- **Removed Objects**: Dropped tables, views, functions, etc.
- **Modified Objects**: Changed definitions with detailed diffs
- **Dependencies**: Related objects that may be affected

When using `--deploy`, the output file contains SQL commands like:

```sql
ALTER TABLE users ADD COLUMN email VARCHAR(255);
ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(100);
DROP TABLE old_table;
CREATE TABLE new_table (...);
```

## Use Cases

### Database Migration Reviews

Compare your migration files against the current database state:

```bash
pg-compose migrations/001_create_users.sql "postgresql://dev:pass@localhost:5432/dev_db"
```

### Feature Branch Comparison

Compare schema changes between feature branches:

```bash
pg-compose ./feature/add_user_roles/ ./main/
```

### Environment Validation

Ensure staging and production schemas match:

```bash
pg-compose "postgresql://staging:pass@staging:5432/app" "postgresql://prod:pass@prod:5432/app"
```

### Schema Deployment

Generate deployment scripts for applying schema changes:

```bash
# Preview changes
pg-compose production_schema.sql feature_schema.sql --deploy migration.sql --dry-run

# Generate deployment script
pg-compose production_schema.sql feature_schema.sql --deploy migration.sql
```

## Development

### Running Tests

```bash
python -m pytest tests/ -v
```

### Project Structure

```
pg-compose-cli/
├── pg_compose_cli/          # Main package
│   ├── cli.py              # Command-line interface
│   ├── compare.py          # Core comparison logic
│   ├── extract.py          # SQL parsing and extraction
│   ├── diff.py             # Schema diffing algorithms
│   ├── alter_generator.py  # Deployment command generation
│   ├── merge.py            # SQL file merging
│   ├── pgdump.py           # Database connection handling
│   ├── git.py              # Git repository handling
│   ├── catalog.py          # Schema catalog management
│   └── sorter.py           # SQL dependency sorting
├── tests/                  # Test suite
│   ├── users/              # User table tests
│   ├── feature_change/     # Feature branch tests
│   └── table_removal/      # Schema removal tests
└── pyproject.toml          # Package configuration
```

## Requirements

- Python 3.8+
- PostgreSQL (for live database connections)
- Git (for repository comparisons)

## Dependencies

- `pglast`: PostgreSQL AST parsing (built on libpg_query) - Copyright (c) 2018-2021, Lele Gaifax and contributors. Licensed under the GNU General Public License v3.0.
- `psycopg[binary]`: PostgreSQL database connectivity

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

- This project is licensed under the GNU General Public License v3.0 (GPL-3.0).
- See the LICENSE file for details.

## Future Plans

We intend to transition to our own direct Python wrapper for [libpg_query](https://github.com/pganalyze/libpg_query) in the future. This will allow for more control, performance, and flexibility in parsing PostgreSQL SQL, and will reduce our reliance on third-party AST libraries.

## Third-Party Libraries

This project uses the following libraries:

- [pglast](https://github.com/lelit/pglast) (GPL-3.0): PostgreSQL AST parsing, built on libpg_query.
- [libpg_query](https://github.com/pganalyze/libpg_query) (PostgreSQL License): C library for parsing PostgreSQL SQL.

Please refer to each project's repository for their full license and attribution.

## Author

Justin Pfeifer - justin.pfeifer@protonmail.com
