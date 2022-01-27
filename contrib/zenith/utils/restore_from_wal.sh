WAL_PATH=$1
SYSID=`od -A n -j 24 -N 8 -t d8 $WAL_PATH/000000010000000000000002* | cut -c 3-`
rm -fr pgsql.0
env -i /home/knizhnik/zenith.main/tmp_install/bin/initdb -E utf8 -U zenith_admin -D pgsql.0 --sysid=$SYSID
pg_ctl -D pgsql.0 -l logfile start
pg_ctl -D pgsql.0 -l logfile stop -m immediate
cp pgsql.0/pg_wal/000000010000000000000001 .
cp $WAL_PATH/* pgsql.0/pg_wal/
(cd pgsql.0/pg_wal ; for partial in *.partial ; do mv $partial `basename $partial .partial`; done)
dd if=000000010000000000000001 of=pgsql.0/pg_wal/000000010000000000000001 bs=6924704 count=1 conv=notrunc
rm 000000010000000000000001
rm -f logfile 000000010000000000000001
pg_ctl -D pgsql.0 -l logfile start
