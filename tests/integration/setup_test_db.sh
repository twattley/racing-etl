#!/bin/bash

# Set database credentials
export PGPASSWORD='test_password'
TEST_USER='test_user'
TEST_DB='test_db'
TEST_HOST='localhost'
TEST_PORT='5433'
SCHEMA_DIR='./src/schema/backup_files/racehorse-database-schema.sql'

# Terminate all connections to test_db
psql -h $TEST_HOST -p $TEST_PORT -U $TEST_USER -d postgres -c "
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = '$TEST_DB' AND pid <> pg_backend_pid();
"

# Drop test_db if it exists
psql -h $TEST_HOST -p $TEST_PORT -U $TEST_USER -d postgres -c "DROP DATABASE IF EXISTS $TEST_DB;"

# Ensure doadmin role exists
psql -h $TEST_HOST -p $TEST_PORT -U $TEST_USER -d postgres -c "
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='doadmin') THEN
      CREATE ROLE doadmin;
   END IF;
END
\$\$;
"

# Create test_db with doadmin as the owner
psql -h $TEST_HOST -p $TEST_PORT -U $TEST_USER -d postgres -c "CREATE DATABASE $TEST_DB OWNER doadmin;"

# Grant all privileges on test_db to test_user
psql -h $TEST_HOST -p $TEST_PORT -U $TEST_USER -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE $TEST_DB TO $TEST_USER;"

# Load the schema into the new test database
psql -h $TEST_HOST -p $TEST_PORT -U $TEST_USER -d $TEST_DB -f $SCHEMA_DIR
