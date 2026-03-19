-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Optional: install extensions if needed in your environment
INSTALL httpfs;
LOAD httpfs;
