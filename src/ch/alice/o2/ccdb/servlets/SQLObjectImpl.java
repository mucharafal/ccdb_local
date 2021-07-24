package ch.alice.o2.ccdb.servlets;

import lazyj.DBFunctions;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SQLObjectImpl extends SQLObject {

    /**
     * Path identifier
     */
    private Integer pathId = null;

    /**
     * String representation of the path
     */
    private String path;

    private final Map<Integer, String> metadata = new HashMap<>();


    public static final String selectAllFromCCDB =
            "SELECT *,extract(epoch from lower(validity))*1000 as validfrom,extract(epoch from upper(validity))*1000 as validuntil FROM ccdb";

    /**
     * @return the pathId of this object
     */
    @Override
    public Integer getPathId() {
        return pathId;
    }

    /**
     * @param pathId the pathId which should be set
     */
    @Override
    public void setPathId(Integer pathId) {
        this.pathId = pathId;
        this.path = getPath(pathId);
    }

    /**
     * @return the full path of this object
     */
    @Override
    public String getPath() {
        return path == null ? getPath(pathId) : path;
    }

    /**
     * @param path the path which should be set
     */
    @Override
    public void setPath(String path) {
        this.path = path;
        this.pathId = getPathID(path, true);
    }

    /**
     * @return Metadata fields set for this object
     */
    @Override
    public Map<String, String> getMetadataKeyValue() {
        Map<String, String> keyValue = new HashMap<>();
        for(Map.Entry<Integer, String> entry: metadata.entrySet()) {
            keyValue.put(getMetadataString(entry.getKey()), entry.getValue());
        }
        return keyValue;
    }

    /**
     * @param key
     * @return old value
     */
    @Override
    public String removeFromMetadata(Integer key) {
        return metadata.remove(key);
    }

    /**
     * @param key   in store
     * @param value new value for key
     * @return old value
     */
    @Override
    public String addToMetadata(Integer key, String value) {
        return metadata.put(key, value);
    }

    public SQLObjectImpl(final String path) {
        super(path);
    }

    public SQLObjectImpl(final HttpServletRequest request, final String path, final UUID uuid) {
        super(request, path, uuid);
    }

    public SQLObjectImpl(final DBFunctions db) {
        super(db);

        setContentType(getContentType(db.geti("contenttype")));

        pathId = db.geti("pathId"); // should convert back to the path

        final Map<?, ?> md = (Map<?, ?>) db.getObject("metadata");
        if (md != null && md.size() > 0)
            for (final Map.Entry<?, ?> entry : md.entrySet())
                metadata.put(Integer.valueOf(entry.getKey().toString()), entry.getValue().toString());
    }

    @Override
    protected boolean updateObjectInDB(DBFunctions db, String replicaArray) {
        return db.query(
                "UPDATE ccdb SET validity=tsrange(to_timestamp(?) AT TIME ZONE 'UTC', to_timestamp(?) AT TIME ZONE 'UTC'), replicas=?::int[], contenttype=?, metadata=?::hstore, lastmodified=? WHERE id=?;",
                false, validFrom / 1000., validUntil / 1000., replicaArray, getContentTypeID(getContentType(), true), metadata, lastModified, id);

    }

    @Override
    protected boolean insertObjectIntoDB(DBFunctions db, String replicaArray) {
        return db.query(
                "INSERT INTO ccdb (id, pathid, validity, createTime, replicas, size, md5, initialvalidity, filename, contenttype, uploadedfrom, metadata, lastmodified) VALUES (?, ?, tsrange(to_timestamp(?) AT TIME ZONE 'UTC', to_timestamp(?) AT TIME ZONE 'UTC'), ?, ?::int[], ?, ?::uuid, ?, ?, ?, ?::inet, ?, ?);",
                false, id, pathId, validFrom / 1000., validUntil / 1000., createTime, replicaArray, size, md5,
                initialValidity, fileName, getContentTypeID(getContentType(), true), uploadedFrom, metadata, lastModified);

    }
}
