#!/bin/bash
set -e

# Start rsyslog, required by ColumnStore
rsyslogd

# Initialize database if not already initialized
if [ ! -d "/var/lib/mysql/mysql" ]; then
    echo "Initializing MariaDB data directory..."
    mariadb-install-db --user=mysql --datadir=/var/lib/mysql > /dev/null

    echo "Starting temporary MariaDB server..."
    mysqld_safe --datadir=/var/lib/mysql --nowatch &
    
    # Wait for server to start
    sleep 5

    echo "Setting up root password and initial database..."
    mariadb -uroot -e "CREATE USER 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD}';"
    mariadb -uroot -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;"
    mariadb -uroot -e "CREATE DATABASE IF NOT EXISTS ${MARIADB_DATABASE};"
    mariadb -uroot -e "FLUSH PRIVILEGES;"
    
    echo "Stopping temporary MariaDB server..."
    mariadb-admin -uroot -p"${MARIADB_ROOT_PASSWORD}" shutdown
    sleep 2
fi

# Make sure socket and ColumnStore directories exist
mkdir -p /var/run/mysqld /var/lib/columnstore/storagemanager
chown mysql:mysql /var/run/mysqld
chown -R mysql:mysql /var/lib/columnstore /var/log/mariadb/columnstore 2>/dev/null || true

echo "Starting MariaDB server..."
"$@" &
mysqld_pid=$!

# Wait for MariaDB to accept connections
for i in $(seq 1 30); do
    if mariadb -uroot -p"${MARIADB_ROOT_PASSWORD}" -e "SELECT 1" > /dev/null 2>&1; then
        break
    fi
    sleep 2
done

# Allow remote connections from other Docker containers (e.g. Airflow)
mariadb -uroot -p"${MARIADB_ROOT_PASSWORD}" -e "CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD}'; GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;" \
  || mariadb -uroot -e "CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD}'; GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;"

/usr/local/bin/start-columnstore.sh

# Create ColumnStore tables after services are running
if [ -d /docker-entrypoint-initdb.d ]; then
    for f in /docker-entrypoint-initdb.d/*.sql; do
        echo "Ensuring schema from $f..."
        mariadb -uroot -p"${MARIADB_ROOT_PASSWORD}" < "$f" \
          || mariadb -uroot < "$f"
    done
fi

wait "$mysqld_pid"
