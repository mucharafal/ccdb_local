psql -h $HOST -p $PORT -c "update ccdb_paths SET pathid = 0 where pathid=1516094193;" ccdb $DEFAULT_USER
psql -h $HOST -p $PORT -c "update ccdb_contenttype SET contenttypeid = 0 where contenttypeid=1178484638;" ccdb $DEFAULT_USER
psql -h $HOST -p $PORT -c "update ccdb_metadata SET metadataid = 0 where metadataid=1188045087;" ccdb $DEFAULT_USER