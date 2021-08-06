package ch.alice.o2.ccdb.servlets;

import alien.monitoring.Timing;
import ch.alice.o2.ccdb.RequestParser;
import lazyj.DBFunctions;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

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
    public Integer getPathId(boolean getFromDatabase) {
        return getPathID(path, true);
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
     * @param key MetadataKey in model
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
							+ "metadata=ccdb_metadata_latest_keyid_value(?), " + "lastmodified=?" + "WHERE id=?;",
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
                        + "	?::inet, ccdb_metadata_latest_keyid_value(?), ? );", false, id, getPath(), validFrom / 1000.,
                validUntil / 1000., createTime, replicaArray,
                size, md5, initialValidity, fileName, getContentType(),
                uploadedFrom, getMetadataKeyValue(), lastModified);
    }

    public static Collection<SQLObject> getAllMatchingObjects(final RequestParser parser) {
        try (Timing ignored = new Timing(monitor, "getAllMatchingObjects_ms")) {
            final List<String> paths = getPathsWithPatternFallback(parser);

            if (paths == null || paths.isEmpty())
                return null;

            final List<SQLObject> ret = Collections
                    .synchronizedList(new ArrayList<>(paths.size() * (parser.latestFlag ? 1 : 2)));

            paths.parallelStream().forEach((id) -> getMatchingObjects(parser, id, ret));

            if (parser.browseLimit > 0 && ret.size() > parser.browseLimit) {
                Collections.sort(ret);

                return ret.subList(0, parser.browseLimit);
            }

            return ret;
        }
    }

    /**
     * @param parser HTTP request parser
     * @return all path IDs that match the request
     */
    private static List<String> getPathsWithPatternFallback(final RequestParser parser) {
        final Integer exactPathID = parser.wildcardMatching ? null : getPathID(parser.path, false);

        final List<String> paths;

        if (exactPathID != null)
            paths = Collections.singletonList(parser.path);
        else
            // wildcard expression ?
            if (parser.path != null && (parser.path.contains("*") || parser.path.contains("%"))) {
                paths = getPaths(parser.path);

                parser.wildcardMatching = true;

                if (paths.size() == 0)
                    return null;
            }
            else
                return null;

        return paths;
    }


    private static void getMatchingObjects(final RequestParser parser, final String path,
                                                 final Collection<SQLObject> ret) {
        final StringBuilder q = new StringBuilder(selectAllFromCCDB());
        q.append( " WHERE path=?");

        final List<Object> arguments = new ArrayList<>();

        arguments.add(path);

        SQLObject.parseOptionsToQuery(parser, q, arguments, false);

        q.append(" ORDER BY createTime DESC");

        if (parser.latestFlag)
            q.append(" LIMIT 1");
        else if (parser.browseLimit > 0)
            q.append(" LIMIT ").append(parser.browseLimit);

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

    private static List<String> getPaths(final String pathPattern) {
        final List<String> ret = new ArrayList<>();

        try (DBFunctions db = getDB()) {
            selectFromCcdbPaths("path", pathPattern, db);
            while (db.moveNext())
                ret.add(db.gets(1));
        }

        return ret;
    }

    public static SQLObject getMatchingObject(final RequestParser parser) {
        final List<Object> arguments = new ArrayList<>();

        try (DBFunctions db = getDB()) {
            final StringBuilder q = new StringBuilder(selectAllFromCCDB());
            q.append( " WHERE path=?");

            arguments.add(parser.path);

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
