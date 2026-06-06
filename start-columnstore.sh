#!/bin/bash
set -e

# Start ColumnStore services (Docker has no systemd)
if pgrep -f PrimProc > /dev/null 2>&1 && dbrmctl status 2>/dev/null | grep -q "OK"; then
    echo "ColumnStore already running."
    exit 0
fi

echo "Starting ColumnStore services..."
JEM=$(ldconfig -p | grep -m1 libjemalloc | awk '{print $1}')
mkdir -p /var/lib/columnstore/storagemanager
chown -R mysql:mysql /var/lib/columnstore /var/log/mariadb/columnstore 2>/dev/null || true

run_as_mysql() {
    su -s /bin/bash mysql -c "$1"
}

if ! pgrep -f StorageManager > /dev/null 2>&1; then
    run_as_mysql "LD_PRELOAD=$JEM /usr/bin/StorageManager" &
    sleep 2
fi

# Skip loadbrm on first boot when no BRM save file exists yet
BRM_FILE="/var/lib/columnstore/data1/systemFiles/dbrm/BRM_saves_current"
if [ -f "$BRM_FILE" ]; then
    /usr/bin/mcs-loadbrm.py no || true
    sleep 2
fi

if ! pgrep -f "workernode DBRM_Worker1" > /dev/null 2>&1; then
    run_as_mysql "/usr/bin/workernode DBRM_Worker1" &
    sleep 3
fi

if ! pgrep -f controllernode > /dev/null 2>&1; then
    run_as_mysql "/usr/bin/controllernode" &
    sleep 3
fi

if ! pgrep -f PrimProc > /dev/null 2>&1; then
    run_as_mysql "LD_PRELOAD=$JEM /usr/bin/PrimProc" &
    sleep 3
fi

if ! pgrep -f ExeMgr > /dev/null 2>&1; then
    run_as_mysql "LD_PRELOAD=$JEM /usr/bin/ExeMgr" &
    sleep 2
fi

if ! pgrep -f WriteEngineServer > /dev/null 2>&1; then
    run_as_mysql "LD_PRELOAD=$JEM /usr/bin/WriteEngineServer" &
    sleep 2
fi

if ! pgrep -f DMLProc > /dev/null 2>&1; then
    run_as_mysql "LD_PRELOAD=$JEM /usr/bin/DMLProc" &
fi

if ! pgrep -f DDLProc > /dev/null 2>&1; then
    run_as_mysql "LD_PRELOAD=$JEM /usr/bin/DDLProc" &
    sleep 3
fi

if [ ! -f "$BRM_FILE" ]; then
    run_as_mysql "/usr/bin/dbbuilder 7"
    sleep 2
fi

dbrmctl status
echo "ColumnStore services started."
