# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2025-04-02T00:15:59

### Added
- Added python-dotenv integration for environment variables management
- Created .env.example template file
- Added documentation structure 

## [2025-04-02T20:59:29]
### Added
- Logging configuration to track server operations and database connections
- Graceful exit handling with signal handlers for clean shutdown 

## [2025-04-02T21:01:27]
### Fixed
- Improved graceful shutdown handling to properly stop FastMCP server on first Ctrl+C 

## [2025-04-02T21:02:16]
### Fixed
- Simplified signal handling for more reliable server shutdown 