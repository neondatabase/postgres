WAL_PATH=$1
SYSID=`od -A n -j 24 -N 8 -t d8 $WAL_PATH/000000010000000000000002* | cut -c 3-`
rm -fr pgsql.0 /tmp/pg_wals
mkdir /tmp/pg_wals
INITDB=`type -p initdb`
env -i $INITDB -E utf8 -U zenith_admin -D pgsql.0 --sysid=$SYSID
REDO_POS=0x`pg_controldata -D pgsql.0 | fgrep "REDO location"| cut -c 42-`
declare -i WAL_SIZE=$REDO_POS+114
cp $WAL_PATH/* /tmp/pg_wals
(cd /tmp/pg_wals ; for partial in *.partial ; do mv $partial `basename $partial .partial`; done)
dd if=pgsql.0/pg_wal/000000010000000000000001 of=/tmp/pg_wals/000000010000000000000001 bs=$WAL_SIZE count=1 conv=notrunc
echo > pgsql.0/recovery.signal
rm -f pgsql.0/pg_wal/*
echo "restore_command = 'cp /tmp/pg_wals/%f %p'" >> pgsql.0/postgresql.conf
rm -f logfile
pg_ctl -D pgsql.0 -l logfile start
