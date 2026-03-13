# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

TuningManager is a Perl-based system that generates and maintains denormalized "performance tuned" tables in PostgreSQL databases. It uses a dependency tree defined in XML to specify which regular and tuning tables each denormalized table depends on. The system tracks table staleness and creates versioned tables with suffixes, using views (aliases) to point to the latest version. This allows nightly refreshes without disrupting database use.

## Build and Installation

- **Build**: `bld TuningManager` (uses Ant build system defined in build.xml)
- **Dependencies**: Requires FgpUtil project (imported in build.xml)

## Core Architecture

### Main Entry Point

`bin/tuningManager` - Main Perl script that orchestrates the entire tuning table management process

Key command-line options:
- `-configFile`: XML file describing tables to create (required)
- `-propFile`: XML file with credentials and configuration (required)
- `-instance`: Database instance (required)
- `-schema`: Database schema (default: apidbtuning)
- `-tables`: Comma-separated list of specific tables to check/update
- `-doUpdate`: Actually perform updates (not just check)
- `-forceUpdate`: Force update even if locked (implies -doUpdate)
- `-debug`: Enable debug logging
- `-prefix`: Substitute prefix macro in table names
- `-filterValue`: Substitute filterValue macro in XML
- `-cleanupAge <days>`: How long before dropping obsolete tables (default: 0)
- `-purgeOrphanTables`: Delete ALL orphan tuning tables
- `-skipDownstreamTables`: Don't automatically include dependent tables

### Object Model

**InternalTable** (`lib/perl/TuningManager/InternalTable.pm`):
- Represents tuning tables managed by TuningManager
- Tracks dependencies (internal, external, external tuning tables)
- Contains SQL, Perl code, programs, or unionizations to build the table
- Manages versioned table creation with suffixes
- Checks staleness by comparing timestamps with dependencies
- Handles intermediate and ancillary tables
- Supports macros: `&1` (suffix), `&prefix`, `&filterValue`

**ExternalTable** (`lib/perl/TuningManager/ExternalTable.pm`):
- Represents regular database tables that tuning tables depend on
- Tracks last modification time via database triggers
- Can reference tables via dblink for cross-database dependencies

**ExternalTuningTable** (`lib/perl/TuningManager/ExternalTuningTable.pm`):
- Represents tuning tables from external databases (via dblink)
- Used when a tuning table depends on another system's tuning tables

**TableSuffix** (`lib/perl/TuningManager/TableSuffix.pm`):
- Manages versioning suffix for tuning table instances
- Uses PostgreSQL sequence (`TuningManager_sq`) to generate unique suffixes
- Suffix is shared across all tables in a single run

### Configuration System

**XML Schema** (`lib/rng/tuningManager.rng`):
- Defines structure using Relax NG schema
- Top-level: `<tuningConfig>` with multiple `<tuningTable>` elements
- Each `<tuningTable>` can have:
  - `name` attribute (required)
  - `alwaysUpdate` attribute (optional)
  - `prefixEnabled` attribute (optional)
  - `<internalDependency>`: Other tuning tables
  - `<externalDependency>`: Regular database tables (with optional dblink, noTrigger)
  - `<externalTuningTableDependency>`: External tuning tables (with optional dblink)
  - `<ancillaryTable>`: Additional tables created alongside main table
  - `<intermediateTable>`: Temporary tables created during build
  - `<unionization>`: Union data from multiple sources
  - `<sql>`: SQL statements to build the table (most common)
  - `<perl>`: Embedded Perl code to execute
  - `<program>`: External command to run
  - `<import>`: Include another XML file

**Properties File** (propFile):
Required XML properties: `password`, `username`, `fromEmail`, `housekeepingSchema`, `logTable`
Optional properties: `maxRebuildMinutes`, `dbaEmail`, `dblink`, `schema`, `apolloUserId`, `apolloPassword`, `ncbiApiKey`

### Dependency Management

The system builds a dependency graph where:
1. Internal dependencies point to other tuning tables
2. External dependencies point to regular database tables
3. External tuning table dependencies point to tuning tables in other databases
4. System validates the graph is acyclic
5. When `-tables` is specified, automatically includes downstream dependent tables (unless `-skipDownstreamTables` is used)

### Workflow

1. **Validation**: Parse XML config, validate schema, check all external tables exist, verify no dependency cycles
2. **Staleness Check**: Compare timestamps of tuning tables with their dependencies
3. **Lock Management**: Set lock in database to prevent concurrent updates (unless `-prefix` mode)
4. **Update**: For stale tables, execute SQL/Perl/programs to rebuild with new suffix
5. **View Update**: Point alias view to new table version
6. **Cleanup**: Drop old table versions based on `-cleanupAge`
7. **Logging**: Write detailed logs and optionally email results via `-notifyEmail`

### Housekeeping Schema

The `housekeepingSchema` (specified in propFile) contains metadata tables:
- `TuningTable`: Tracks each tuning table's timestamp, status, definition
- `TuningManager_sq`: Sequence for generating table suffixes
- `logTable`: Records build duration, row count, table size for each rebuild

### Utilities

**Log** (`lib/perl/TuningManager/Log.pm`):
- Centralized logging with timestamps and indentation
- Tracks flags: updateNeeded, updatePerformed, errorsEncountered, partialUpdate
- Email notification support
- Writes to `/tmp/tuningManager.<pid>.<time>.log`

**Utils** (`lib/perl/TuningManager/Utils.pm`):
- Database connection handling (`getDbHandle`)
- SQL retry logic for handling transient errors (ORA-03135)
- Uses PostgreSQL (DBI:Pg)

## Testing

Test configuration: `lib/xml/testTuningManager.xml` - Simple example with HelloWorld table

## Database

Target RDBMS: PostgreSQL (code references `dbi:Pg:` and PostgreSQL-specific syntax like `SET search_path`)

Legacy note: Some error handling still references Oracle error codes (ORA-03135, ORA-01652) suggesting Oracle heritage

## Common Patterns

- Table versioning: Tables created as `TableName<suffix>`, view points to latest
- Macros in SQL: `&1` replaced with suffix, `&prefix` and `&filterValue` replaced if specified
- Comparison speed: Uses `information_schema` queries and table size checks to skip comparison when dependency is much larger than dependent
