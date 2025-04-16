# Testing MCP Alchemy

This guide explains how to test MCP Alchemy with multiple databases using Docker and Claude Desktop.

## Setup Test Databases

1. Start the test databases using docker-compose:

```bash
cd tests
docker-compose up -d
```

This will create:

- MySQL database on port 3307
- PostgreSQL database on port 5433
- The Chinook sample database will be loaded into both

2. Verify the databases are running:

```bash
# Check MySQL
mysql -h 127.0.0.1 -P 3307 -u chinook -pchinook Chinook -e "SELECT COUNT(*) FROM Album;"

# Check PostgreSQL
PGPASSWORD=chinook psql -h localhost -p 5433 -U chinook chinook_db -c "SELECT COUNT(*) FROM \"Album\";"
```

## Configure Claude Desktop

The provided `tests/claude_desktop_config.json` contains configurations for:

- SQLite Chinook database
- MySQL Chinook database
- PostgreSQL Chinook database

Copy it to your Claude Desktop config location:

```bash
cp tests/claude_desktop_config.json ~/.config/claude-desktop/config.json
```

## Sample Test Prompt

Here's a comprehensive prompt to test all three databases:

```
I'd like to explore the Chinook database across different database engines. Let's:

1. First, list all tables in each database (SQLite, MySQL, and PostgreSQL) to verify they're identical
2. Get the schema for the Album and Artist tables from each database
3. Run this query on each database:
   SELECT ar.Name as ArtistName, COUNT(al.AlbumId) as AlbumCount 
   FROM Artist ar 
   LEFT JOIN Album al ON ar.ArtistId = al.ArtistId 
   GROUP BY ar.ArtistId, ar.Name 
   HAVING COUNT(al.AlbumId) > 5 
   ORDER BY AlbumCount DESC;
4. Compare the results - they should be identical across all three databases

Can you help me with this analysis?
```

This will test:

- Database connectivity to all three databases
- Table listing functionality
- Schema inspection
- Complex query execution
- Result formatting
- Cross-database consistency

## Expected Results

The results should show:

- 11 tables in each database
- Identical schema definitions
- Same query results across all databases
- Proper handling of NULL values and formatting

If any discrepancies are found, check:

1. Docker container status
2. Database connection strings
3. Database initialization scripts
