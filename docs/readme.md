# CCDB deployment documentation

## Deployment

CCDB servers are deployed on 4 instances:
- offline (Meyrin):
    - alicdb1.cern.ch
    - alicdb2.cern.ch
- online (point 2):
    - alicdb3.cern.ch
    - alicdb4.cern.ch

Consist from 5 parts: http load ballancer <-> CCDB <-> Pgpool <-> PostgreSQL <-> Bucardo -> Other side.

Bucardo is deployed on alicdb3 instance.

## Details about deployment parts:

- [Pgpool-II](pgpool.md)
- [Bucardo](bucardo.md)
- [PostgreSQL](postgresql.md)
- [Monitoring](monitoring.md)
## Repositories

- configurations: https://gitlab.cern.ch/rmucha/ccdb_postgres_conf.git
- ansible receipes: https://gitlab.cern.ch/rmucha/ccdb_postgres_ansible
- bucardo reconciling scripts: https://github.com/mucharafal/ccdb_bucardo_scripts 