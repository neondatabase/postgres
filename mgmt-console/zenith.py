#zenith.py
import click


@click.group()
def main():
    """Run the Zenith CLI."""

@click.group()
def pg():
    """Db operations"""

@click.command(name='create')
@click.argument('name')
@click.option('-s', '--storage-name', help='Name of the storage',
                                 default='zenith-local',
                                 show_default=True)
@click.option('--snapshot', help='init from the snapshot. Snap is a name or URL')
def pg_create(name, storage_name, snapshot):
    """Initialize the database"""

@click.command(name='destroy')
def pg_destroy():
    """Drop the database"""

@click.command(name='start')
@click.option('--read-only', is_flag=True, help='Start read-only node', show_default=True)
def pg_start(snapshot):
    """Start the database"""
    click.echo('snapshot %s' % snapshot)

@click.command(name='stop')
def pg_stop():
    """Stop the database"""

@click.command(name='list')
def pg_list():
    """List existing databases"""

pg.add_command(pg_create)
pg.add_command(pg_destroy)
pg.add_command(pg_start)   
pg.add_command(pg_stop)   
pg.add_command(pg_list)   


@click.group()
def storage():
    """Storage operations"""

@click.command(name='attach')
@click.argument('name')
def storage_attach():
    """Attach the storage"""

@click.command(name='detach')
@click.argument('name')
@click.option('--force', is_flag=True, show_default=True)
def storage_detach():
    """Detach the storage"""

@click.command(name='list')
def storage_list():
    """List existing storages"""

storage.add_command(storage_attach)
storage.add_command(storage_detach)
storage.add_command(storage_list)

@click.group()
def snapshot():
    """Snapshot operations"""


@click.command(name='create')
def snapshot_create():
    """Create new snapshot"""

@click.command(name='destroy')
def snapshot_destroy():
    """Destroy the snapshot"""

@click.command(name='pull')
def snapshot_pull():
    """Pull remote snapshot"""

@click.command(name='push')
def snapshot_push():
    """Push snapshot to remote"""

@click.command(name='import')
def snapshot_import():
    """Convert given format to zenith snapshot"""

@click.command(name='export')
def snapshot_export():
    """Convert zenith snapshot to PostgreSQL compatible format"""

snapshot.add_command(snapshot_create)
snapshot.add_command(snapshot_destroy)
snapshot.add_command(snapshot_pull)
snapshot.add_command(snapshot_push)
snapshot.add_command(snapshot_import)
snapshot.add_command(snapshot_export)

@click.group()
def wal():
    """WAL operations"""

@click.command()
def wallist(name="list"):
    """List WAL files"""

wal.add_command(wallist)


@click.command()
def console():
    """Open web console"""

main.add_command(pg)
main.add_command(storage)
main.add_command(snapshot)
main.add_command(wal)
main.add_command(console)


if __name__ == '__main__':
    main()
