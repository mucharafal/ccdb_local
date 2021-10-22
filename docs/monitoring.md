# Monitoring

Other components, so - Bucardo, Pgpool-II and PostgreSQL has dedicated monitoring.

Page on AliMonitor: http://alimonitor.cern.ch/stats?page=ccdb/service .

Sources are on repo: https://gitlab.cern.ch/rmucha/ccdb_postgres_monitoring .

## Instalation
In repo there is a file `ans`. It can be used to install monitoring. 

Important! Offline version has its own branch. During updates, please `git rebase master`.
To deploy on offline, please use this branch - it contains different configuration to connect with PostgreSQL.

[back](readme.md)