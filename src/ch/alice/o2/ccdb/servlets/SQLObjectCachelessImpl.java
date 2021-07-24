package ch.alice.o2.ccdb.servlets;

import lazyj.DBFunctions;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SQLObjectCachelessImpl extends SQLObject {

    /**
     * String representation of the path
     */
    private String path;

    private final Map<String, String> metadataValues = new HashMap<>();


    public static final String selectAllFromCCDB =
            "SELECT *,extract(epoch from lower(validity))*1000 as validfrom,extract(epoch from upper(validity))*1000 as validuntil FROM ccdb_view";

    /**
     * @return the pathId of this object
     */
    @Override
    public Integer getPathId() {
        return getPathID(path, true);
    }

    /**
     * @param pathId the pathId which should be set
     */
    @Override
    public void setPathId(Integer pathId) { //todo - test it in Cacheless impl
        this.path = getPath(pathId);
    }

    /**
     * @return the full path of this object
     */
    @Override
    public String getPath() {
        return path;
    }

    /**
     * @param path the path which should be set
     */
    @Override
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * @return Metadata fields set for this object
     */
    @Override
    public Map<String, String> getMetadataKeyValue() {
        return metadataValues;
    }

    /**
     * @param key
     * @return old value
     */
    @Override
    public String removeFromMetadata(Integer key) {
        return metadataValues.remove(getMetadataString(key));
    }

    /**
     * @param key   in store
     * @param value new value for key
     * @return old value
     */
    @Override
    public String addToMetadata(Integer key, String value) {
        return metadataValues.put(getMetadataString(key), value);
    }

    public SQLObjectCachelessImpl(final String path) {
        super(path);
    }

    public SQLObjectCachelessImpl(final HttpServletRequest request, final String path, final UUID uuid) {
        super(request, path, uuid);
    }

    /**
     * @param db database row to load the fields from
     */

    public SQLObjectCachelessImpl(final DBFunctions db) {
        super(db);

        setContentType(db.gets("contenttype_value"));

        path = db.gets("path"); // should convert back to the path

        final Map<?, ?> md = (Map<?, ?>) db.getObject("metadata_key_value");

        if (md != null && md.size() > 0)
            for (final Map.Entry<?, ?> entry : md.entrySet())
                metadataValues.put(entry.getKey().toString(), entry.getValue().toString());
    }

    @Override
    protected boolean updateObjectInDB(DBFunctions db, String replicaArray) {
        return db.query("UPDATE ccdb SET "
							+ "validity=tsrange(to_timestamp(?) AT TIME ZONE 'UTC', to_timestamp(?) AT TIME ZONE 'UTC'),"
							+ "replicas=?::int[], " + "contenttype=ccdb_contenttype_latest(?), "
							+ "metadata=ccdb_metadata_latest(?), " + "lastmodified=?" + "WHERE id=?;",
							false, validFrom / 1000., validUntil / 1000., replicaArray,
							getContentType(), getMetadataKeyValue(), lastModified, id);
    }

    @Override
    protected boolean insertObjectIntoDB(DBFunctions db, String replicaArray) {
        return db.query("INSERT INTO ccdb (id, pathid, validity, createTime, replicas, size, \n"
                        + "md5, initialvalidity, filename, contenttype, uploadedfrom, metadata, \n"
                        + "lastmodified) VALUES (?, ccdb_paths_latest(?), \n"
                        + "tsrange(to_timestamp(?) AT TIME ZONE 'UTC', to_timestamp(?) AT TIME ZONE 'UTC'), \n"
                        + "?, ?::int[], ?, ?::uuid, ?, ?, ccdb_contenttype_latest(?), \n"
                        + "	?::inet, ccdb_metadata_latest(?), ? );", false, id, getPath(), validFrom / 1000.,
                validUntil / 1000., createTime, replicaArray,
                size, md5, initialValidity, fileName, getContentType(),
                uploadedFrom, getMetadataKeyValue(), lastModified);
    }
}
