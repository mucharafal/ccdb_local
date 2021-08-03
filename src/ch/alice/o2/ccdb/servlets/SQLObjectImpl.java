package ch.alice.o2.ccdb.servlets;

import alien.monitoring.Timing;
import ch.alice.o2.ccdb.RequestParser;
import lazyj.DBFunctions;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

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

    /**
     * @param parser
     * @return all path IDs that match the request
     */
    static List<Integer> getPathIDsWithPatternFallback(final RequestParser parser) {
        final Integer exactPathId = parser.wildcardMatching ? null : getPathID(parser.path, false);

        final List<Integer> pathIDs;

        if (exactPathId != null)
            pathIDs = Arrays.asList(exactPathId);
        else
            // wildcard expression ?
            if (parser.path != null && (parser.path.contains("*") || parser.path.contains("%"))) {
                pathIDs = getPathIDs(parser.path);

                parser.wildcardMatching = true;

                if (pathIDs == null || pathIDs.size() == 0)
                    return null;
            }
            else
                return null;

        return pathIDs;
    }

    public static final Collection<SQLObject> getAllMatchingObjects(final RequestParser parser) {
        try (Timing t = new Timing(monitor, "getAllMatchingObjects_ms")) {
            final List<Integer> pathIDs = getPathIDsWithPatternFallback(parser);

            if (pathIDs == null || pathIDs.isEmpty())
                return null;

            final List<SQLObject> ret = Collections
                    .synchronizedList(new ArrayList<>(pathIDs.size() * (parser.latestFlag ? 1 : 2)));

            pathIDs.parallelStream().forEach((id) -> getMatchingObjects(parser, id, ret));

            if (parser.browseLimit > 0 && ret.size() > parser.browseLimit) {
                Collections.sort(ret);

                return ret.subList(0, parser.browseLimit);
            }

            return ret;
        }
    }

    private static final void getMatchingObjects(final RequestParser parser, final Integer pathId,
                                                 final Collection<SQLObject> ret) {
        final StringBuilder q = new StringBuilder(selectAllFromCCDB());
        q.append( " WHERE pathId=?");

        final List<Object> arguments = new ArrayList<>();

        arguments.add(pathId);

        SQLObject.parseOptionsToQuery(parser, q, arguments, false);

        q.append(" ORDER BY createTime DESC");

        if (parser.latestFlag)
            q.append(" LIMIT 1");
        else if (parser.browseLimit > 0)
            q.append(" LIMIT " + parser.browseLimit);

        try (DBFunctions db = getDB()) {
            db.query(q.toString(), false, arguments.toArray(new Object[0]));

            while (db.moveNext()) {
                try {
                    ret.add(SQLObject.fromDb(db));
                }
                catch (final Exception e) {
                    System.err.println("Got exception loading object " + db.geti("id") + " from DB: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<Integer> getPathIDs(final String pathPattern) {
        final List<Integer> ret = new ArrayList<>();

        try (DBFunctions db = getDB()) {
            selectFromCcdbPaths("pathid", pathPattern, db);
            while (db.moveNext())
                ret.add(db.geti(1));
        }

        return ret;
    }

    public static SQLObject getMatchingObject(final RequestParser parser) {
        final Integer pathId = getPathID(parser.path, false);		// todo

        if (pathId == null)
            return null;

        final List<Object> arguments = new ArrayList<>();

        try (DBFunctions db = getDB()) {
            final StringBuilder q = new StringBuilder(selectAllFromCCDB());
            q.append( " WHERE pathId=?");

            arguments.add(pathId);

            if(!SQLObject.parseOptionsToQuery(parser, q, arguments, true)) {
                return null;
            }

            q.append(" ORDER BY createTime DESC LIMIT 1;");

            db.query(q.toString(), false, arguments.toArray(new Object[0]));

            if (db.moveNext())
                return SQLObject.fromDb(db);

            return null;
        }
    }
}
