# PostgreSQL

Here are kept metadata about files.

## Operation
On nodes is system service called `postgres`. To check status - `systemctl status postgres`. Supports `start`, `stop`, `restart`.

## Deployment details

Two independent units for online and offline sides respectively. Every side has enabled streaming master-stadby replication. Online and offline sides are synchronized asynchronously using Bucardo.

## Instalation and configuration

Details can be found here: https://gitlab.cern.ch/rmucha/ccdb_postgres_ansible

Configuration files for ansible receipes are here: https://gitlab.cern.ch/rmucha/ccdb_postgres_conf.git


### Important configuration changes

Main change from default configuration is turning off `synchronous_commit`. After that, database can handle much higher insert load.

Connected option is `wal_writer_delay`. It is set to `10ms`. It is time since first change in WAHL to dumping data from WAHL buffer on disk.

Others changes are mainly changes in buffer sizes.

[back](readme.md)