# Bucardo
It is tool to synchronize two master instances of database. It is connected directly to alicdb1 and alicdb3 (in default deployment). It is deployed on alicdb3.

## Operation
Bucardo has its own system service, so you can manage it using `systemctl` commands (`start`, `stop`, `restart`). It is running as `postgres` user. In `postgres` home directory there is also a folder with its files.

## Monitored parameters

- `inserts`, `deletes`, `updates` - number of inserted (...) rows, handled by last reconcile
- time of last good/bad - time when occur last good/bad reconcile process
- Status - status of a service

## Configuration

- `.bucardorc` - you can define here some default settings, like user or address of control database.
- `.pgpass` - from there are taken (and saved) passwords to connect with configured databases.

## Instalation and first configuration

During configuration, there is one step, which must be done manually. Instalation is in ansible receipes: https://gitlab.cern.ch/rmucha/ccdb_postgres_ansible .
Step which needs to be done manually is instalation Bucardo in databases. Why it is needed? To create all Bucardo stuffs (user, tables, and some other magic). So you need to execute on all instances, which are masters, even though, Bucardo is effectively running only on one. 
```bash
./bucardo install \
    --dbhost localhost \
    --dbuser postgres \
    --dbname postgres \
    --dbport 5433 \
    --piddir /home/postgres/bucardo 
```
It is interactive command, so you need to confim, that provided configuration is correct.
After that, rest you can follow ansible receipe.

## Trics, problems etc.

Bucardo, when facing high load, can go into loop. You can observe it in `PostgreSQL` parameters - number of `returned` and `fetched` tuples - they are increasing linearly to database limits. In such situation - stop bucardo and kill manually all backends (processes) in database, which are created by Bucardo.

## Reconcile scripts

Are in repository: 
https://github.com/mucharafal/ccdb_bucardo_scripts 

[back](readme.md)