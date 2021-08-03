package ch.alice.o2.ccdb.servlets;

import java.io.File;
import java.net.InetAddress;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.XrootDEnvelope;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import ch.alice.o2.ccdb.Options;
import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.UUIDTools;
import lazyj.DBFunctions;
import lazyj.ExtProperties;
import lazyj.Format;
import lazyj.StringFactory;
import lazyj.Utils;

/**
 * SQL backing for a CCDB/QC object
 *
 * @author costing
 * @since 2017-10-13
 */
public abstract class SQLObject implements Comparable<SQLObject> {
	private static ExtProperties config = new ExtProperties(Options.getOption("config.dir", "."),
			Options.getOption("config.file", "config"));

	protected static final Monitor monitor = MonitorFactory.getMonitor(SQLObject.class.getCanonicalName());

	protected static final Logger logger = Logger.getLogger(SQLObject.class.getCanonicalName());

	public static final boolean multiMasterVersion = Options.getOption("multimaster", "false").equals("true");

	public static String selectAllFromCCDB() {
		return multiMasterVersion ? SQLObjectCachelessImpl.selectAllFromCCDB : SQLObjectImpl.selectAllFromCCDB;
	}

	/**
	 * @return the database connection
	 */
	public static final DBFunctions getDB() {
		if (config.gets("driver", null) != null)
			return new DBFunctions(config);

		return null;
	}

	/**
	 * Unique identifier of an object
	 */
	public UUID id;

	/**
	 * @return the pathId of this object
	 */
	public abstract Integer getPathId();
	/**
	 * @param pathId the pathId which should be set
	 */
	public abstract void setPathId(Integer pathId);

	/**
	 * @return the full path of this object
	 */
	public abstract String getPath();
	/**
	 * @param path the path which should be set
	 */
	public abstract void setPath(String path);

	/**
	 * Creation time, in epoch milliseconds
	 */
	public long createTime;

	/**
	 * Starting time of the validity of this object, in epoch milliseconds.
	 * Inclusive value.
	 */
	public long validFrom = System.currentTimeMillis();

	/**
	 * Ending time of the validity of this object, in epoch milliseconds. Exclusive
	 * value.
	 */
	public long validUntil = validFrom + 1;

	/**
	 * Metadata fields set for this object
	 */
	public Map<Integer, String> metadata = new HashMap<>();

	/**
	 * @return Metadata fields set for this object
	 */
	public abstract Map<String, String> getMetadataKeyValue();

	/**
	 *
	 * @param key
	 * @return old value
	 */
	public abstract String removeFromMetadata(Integer key);

	/**
	 *
	 * @param key in store
	 * @param value new value for key
	 * @return old value
	 */
	public abstract String addToMetadata(Integer key, String value);

	/**
	 * Servers holding a replica of this object
	 */
	public Set<Integer> replicas = new TreeSet<>();

	/**
	 * Size of the object
	 */
	public long size = -1;

	/**
	 * MD5 checksum
	 */
	public String md5 = null;

	/**
	 * Initial validity of this object (might be updated but this is to account for
	 * how long it was at the beginning)
	 */
	public long initialValidity = validUntil;

	/**
	 * Original file name that was uploaded (will be presented with the same name to
	 * the client)
	 */
	public String fileName = null;

	/**
	 * Content type
	 */
	private String contentType = null;
	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	/**
	 * IP address of the client that has uploaded this object
	 */
	public String uploadedFrom = null;

	/**
	 * Timestamp of the last update
	 */
	public long lastModified = System.currentTimeMillis();

	protected transient boolean existing = false;
	protected transient boolean tainted = false;

	/**
	 * Create an empty object
	 *
	 * @param path object path
	 */
	public SQLObject(final String path) {
		createTime = System.currentTimeMillis();
		id = UUIDTools.generateTimeUUID(createTime, null);

		assert path != null && path.length() > 0;

		this.setPath(path);
	}

	public static SQLObject fromPath(final String path) {
		return multiMasterVersion ? new SQLObjectCachelessImpl(path) : new SQLObjectImpl(path);
	}

	/**
	 * Create a new object from a request
	 *
	 * @param request
	 * @param path object path
	 * @param uuid unique identifier to force on the new object. Can be
	 *            <code>null</code> to automatically generate one.
	 */
	protected SQLObject(final HttpServletRequest request, final String path, final UUID uuid) {
		createTime = System.currentTimeMillis();

		if (uuid != null) {
			id = uuid;
		}
		else {
			byte[] remoteAddress = null;

			if (request != null)
				try {
					final InetAddress ia = InetAddress.getByName(request.getRemoteAddr());

					remoteAddress = ia.getAddress();
				}
				catch (@SuppressWarnings("unused") final Throwable t) {
					// ignore
				}

			id = UUIDTools.generateTimeUUID(createTime, remoteAddress);
		}

		assert path != null && path.length() > 0;

		this.setPath(path);
	}

	public static SQLObject fromRequest(final HttpServletRequest request, final String path, final UUID uuid) {
		return multiMasterVersion ? new SQLObjectCachelessImpl(request, path, uuid) : new SQLObjectImpl(request, path, uuid);
	}

	protected SQLObject(final DBFunctions db) {
		id = (UUID) db.getObject("id");

		createTime = db.getl("createtime");
		validFrom = db.getl("validfrom");
		validUntil = db.getl("validuntil"); // read from the tsrange structure
		size = db.getl("size");
		md5 = Format.replace(db.gets("md5"), "-", "");
		initialValidity = db.getl("initialvalidity");
		fileName = db.gets("filename");
		uploadedFrom = db.gets("uploadedfrom");

		final Array replicasObject = (Array) db.getObject("replicas");

		if (replicasObject != null)
			try {
				final Integer[] r = (Integer[]) replicasObject.getArray();

				Collections.addAll(replicas, r);
			} catch (@SuppressWarnings("unused") final SQLException e) {
				// ignore
			}

		existing = true;
	}

	public static SQLObject fromDb(final DBFunctions db) {
		return multiMasterVersion ? new SQLObjectCachelessImpl(db) : new SQLObjectImpl(db);
	}

	protected abstract boolean updateObjectInDB(DBFunctions db, String replicaArray);

	protected abstract boolean insertObjectIntoDB(DBFunctions db, String replicaArray);

	/**
	 * @param request request details, to decorate the metadata with
	 * @return <code>true</code> if the object was successfully saved
	 */
	public boolean save(final HttpServletRequest request) {
		if (!existing || tainted) {
			if (request != null) {
				if (existing)
					setProperty("UpdatedFrom", request.getRemoteHost());

				final AliEnPrincipal account = UserFactory.get(request);

				if (account != null)
					setProperty(existing ? "UpdatedBy" : "UploadedBy", account.getDefaultUser());
			}

			if (this.getPathId() == null)
				 this.setPathId(getPathID(getPath(), true));

			try (DBFunctions db = getDB()) {
				final StringBuilder sb = new StringBuilder();

				String replicaArray = null;

				if (replicas.size() > 0) {
					sb.setLength(0);
					sb.append("{");

					for (final Integer replica : replicas) {
						if (sb.length() > 2)
							sb.append(',');
						sb.append(replica);
					}

					sb.append('}');

					replicaArray = sb.toString();
				}

				lastModified = System.currentTimeMillis();

				Map<String, String> metadataWithPK = getMetadataKeyValue();

				getContentTypeID(getContentType(), true);

				if (existing) {
					final boolean ok = updateObjectInDB(db, replicaArray);

					if (ok) {
						existing = true;
						tainted = false;
						return true;
					}

					System.err.println("Update query failed for id=" + id);
				}
				else {
					initialValidity = validUntil;

					for (int attempt = 0; attempt < 2; attempt++) {
						if (attempt > 0) {
							// if another instance has cleaned up this path
							removePathID(getPathId());
							setPathId(getPathID(getPath(), true));
						}
						
						if (insertObjectIntoDB(db, replicaArray)) {
							existing = true;
							tainted = false;
							return true;
						}
					}

					System.err.println("Insert query failed for id=" + id);
				}
			}
		}

		return false;

	}

	/**
	 * @return last modification timestamp
	 */
	public long getLastModified() {
		try {
			final String md = getMetadataKeyValue().get("LastModified");

			if (md != null)
				return Long.parseLong(md);
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore, the default below will
		}

		return createTime;
	}

	/**
	 * @return the folder path to this object.
	 */
	public String getFolder() {
		final int hash = Math.abs(id.hashCode() % 1000000);

		return hash % 100 + "/" + hash / 100;
	}

	/**
	 * Return the full URL(s) to the physical representation on this replica ID
	 *
	 * @param replica
	 * @return full URL
	 */
	public List<String> getAddress(final Integer replica) {
		return getAddress(replica, null, true);
	}

	/**
	 * Return the full URL(s) to the physical representation on this replica ID
	 *
	 * @param replica
	 * @param ipAddress client's IP address, when known, to better sort the
	 *            replicas
	 * @param resolveAliEn whether or not to look up the PFNs for AliEn LFNs
	 * @return full URL
	 */
	public List<String> getAddress(final Integer replica, final String ipAddress, final boolean resolveAliEn) {
		final String configKey = "server." + replica + ".urlPattern";

		String pattern = Options.getOption(configKey, config.gets(configKey, null));

		if (pattern == null) {
			if (replica.intValue() == 0) {
				final String relativeURLKey = "server.0.relativeURL";

				if (Utils.stringToBool(Options.getOption(relativeURLKey, null), config.getb(relativeURLKey, true))) {
					// It's easier for CcdbApi to follow redirects if this is disabled, having the
					// full URL in the Location header
					pattern = "/download/UUID";
				}
				else {
					String hostname;

					try {
						hostname = InetAddress.getLocalHost().getCanonicalHostName();
					}
					catch (@SuppressWarnings("unused") final Throwable t) {
						hostname = "localhost";
					}

					pattern = "http://" + hostname + ":" + Options.getIntOption("tomcat.port", 8080) + "/download/UUID";
				}
			}
			else {
				if (replica.intValue() > 0) {
					final SE se = SEUtils.getSE(replica.intValue());

					if (se != null) {
						pattern = se.generateProtocol();
						if (!pattern.endsWith("/"))
							pattern += "/";

						pattern += "HASH.ccdb";
					}
				}

				if (pattern == null)
					pattern = "alien:///alice/data/CCDB/PATHHASH";
			}

			config.set("server." + replica + ".urlPattern", pattern);
		}

		if (pattern.contains("UUID"))
			pattern = Format.replace(pattern, "UUID", id.toString());

		if (pattern.contains("FOLDER"))
			pattern = Format.replace(pattern, "FOLDER", getFolder());

		if (pattern.contains("PATH"))
			pattern = Format.replace(pattern, "PATH", getPath());

		if (pattern.contains("HASH"))
			pattern = Format.replace(pattern, "HASH", SE.generatePath(id.toString()));

		if (pattern.startsWith("alien://")) {
			if (!resolveAliEn)
				return Arrays.asList(pattern);

			final JAliEnCOMMander commander = new JAliEnCOMMander(null, null, AsyncResolver.getSite(ipAddress, true),
					null);

			final LFN l = commander.c_api.getLFN(pattern.substring(8));

			if (l != null) {
				final Collection<PFN> pfns = commander.c_api.getPFNsToRead(l, null, null);

				if (pfns != null) {
					final List<String> ret = new ArrayList<>(pfns.size());

					for (final PFN p : pfns) {
						String envelope = null;

						if (p.ticket != null && p.ticket.envelope != null)
							envelope = p.ticket.envelope.getEncryptedEnvelope();

						final String httpUrl = p.getHttpURL();

						if (httpUrl != null) {
							if (envelope != null)
								ret.add(httpUrl + "?authz=" + XrootDEnvelope.urlEncodeEnvelope(envelope));
							else
								ret.add(httpUrl);
						}
						else {
							if (envelope != null)
								ret.add(p.getPFN() + "?authz=" + envelope);
							else
								ret.add(p.getPFN());
						}
					}

					return ret;
				}

				return Collections.emptyList();
			}
		}

		return Arrays.asList(pattern);
	}

	/**
	 * Get all URLs where replicas of this object can be retrieved from
	 *
	 * @param ipAddress client's IP address, to better sort the replicas function of
	 *            its location
	 * @param httpOnly whether or not to return http:// addresses only.
	 *            Alternatively alien:// and root:// URLs are also returned.
	 *
	 * @return the list of URLs where the content of this object can be retrieved
	 *         from
	 */
	public List<String> getAddresses(final String ipAddress, final boolean httpOnly) {
		final List<String> ret = new ArrayList<>();

		for (final Integer replica : replicas) {
			final List<String> toAdd = (SQLBacked.isLocalCopyFirst() && replica.intValue() == 0) ? new LinkedList<>()
					: null;

			for (final String addr : getAddress(replica, ipAddress, httpOnly))
				if (!httpOnly || (!addr.startsWith("alien://") && !addr.startsWith("root://")))
					(toAdd != null ? toAdd : ret).add(addr);

			if (toAdd != null)
				ret.addAll(0, toAdd);
		}

		return ret;
	}

	/**
	 * Get the directory on the local filesystem (starting from the directory
	 * structure under {@link SQLBacked#basePath}) where this file could be located.
	 * Optionally creates the directory structure to it, for when the files have to
	 * be uploaded.
	 *
	 * @param createIfMissing create the directory structure. Set this to
	 *            <code>true</code> only from upload methods, to
	 *            <code>false</code> on read queries
	 * @return the folder, if it exists or (if indicated so) could be created. Or
	 *         <code>null</code> if any problem.
	 */
	public File getLocalFolder(final boolean createIfMissing) {
		final File folder = new File(SQLBacked.basePath, getFolder());

		if (!folder.exists() && createIfMissing)
			if (!folder.mkdirs())
				return null;

		if (folder.exists() && folder.isDirectory())
			return folder;

		return null;
	}

	/**
	 * Get the local file that is a representation of this object.
	 *
	 * @param createIfMissing Whether or not this is a write operation. In this case
	 *            all intermediate folders are created (if possible).
	 *            Pass <code>false</code> for all read-only queries.
	 * @return the local file for this object ID. For uploads the folders are
	 *         created but not the end file. For read queries the entire structure
	 *         must exist and the file has to have the same size as the database
	 *         record. Will return <code>null</code> if the local file doesn't exist
	 *         and/or could not be created.
	 */
	public File getLocalFile(final boolean createIfMissing) {
		final File folder = getLocalFolder(createIfMissing);

		if (folder == null)
			return null;

		final File ret = new File(folder, id.toString());

		if (createIfMissing || (ret.exists() && ret.isFile() && ret.length() == size))
			return ret;

		return null;
	}

	/**
	 * Set a metadata field of this object. {@link #save(HttpServletRequest)} should
	 * be called afterwards to actually flush this change to the persistent store.
	 *
	 * @param key
	 * @param value
	 */
	public void setProperty(final String key, final String value) {
		final Integer keyID = getMetadataID(key, true);

		if (keyID == null)
			return;

		if (value == null || value.isBlank()) {
			final String oldValue = this.removeFromMetadata(keyID);

			tainted = tainted || oldValue != null;
		}
		else {
			final String oldValue = this.addToMetadata(keyID, value);

			tainted = tainted || !value.equals(oldValue);
		}
	}

	/**
	 * @return the metadata keys
	 */
	public Set<String> getPropertiesKeys() {
		return getMetadataKeyValue().keySet();
	}

	/**
	 * @param key
	 * @return the value for this key, if found, otherwise <code>null</code>
	 */
	public String getProperty(final String key) {
		return getProperty(key, null);
	}

	/**
	 * @param key
	 * @param defaultValue
	 * @return value for this key, or the default value if the requested metadata
	 *         key is not defined for this object
	 */
	public String getProperty(final String key, final String defaultValue) {
		String value = getMetadataKeyValue().get(key);

		return value == null ? defaultValue : value;
	}

	/**
	 * Modify the expiration time of an object
	 *
	 * @param newEndTime
	 * @return <code>true</code> if the value was modified
	 */
	public boolean setValidityLimit(final long newEndTime) {
		if (newEndTime != validUntil && newEndTime > validFrom) {
			validUntil = newEndTime;
			tainted = true;
			return true;
		}

		return false;
	}

	/**
	 * Delete this entry
	 *
	 * @return <code>true</code> if the removal was successful
	 */
	public boolean delete() {
		if (existing)
			try (DBFunctions db = getDB()) {
				final String q = "DELETE FROM ccdb WHERE id=?";

				if (!db.query(q, false, id)) {
					logger.log(Level.WARNING, "Query failed to execute: " + q + " [" + id.toString() + "]");
					return false;
				}

				final int updateCount = db.getUpdateCount();

				if (updateCount <= 0) {
					logger.log(Level.WARNING, "Query didn't remove anything: " + q + " [" + id.toString() + "]");
					return false;
				}

				return true;
			}

		logger.log(Level.WARNING,
				"Asked to delete something that is not persistently stored in the database: " + id.toString());
		return false;
	}

	private static final Cache pathsCache = new Cache(!multiMasterVersion);

	protected static synchronized Integer getPathID(final String path, final boolean createIfNotExists) {
		Integer value = pathsCache.getIdFromCache(path);

		if (value != null)
			return value;

		try (DBFunctions db = getDB()) {
			db.query("SELECT pathid FROM ccdb_paths WHERE path=?;", false, path);

			if (db.moveNext()) {
				value = db.geti(1);
				pathsCache.putInCache(value, path);
				return value;
			}

			if (createIfNotExists) {
				final Integer hashId = absHashCode(path);

				if (hashId > 0
						&& db.query("INSERT INTO ccdb_paths (pathId, path) VALUES (?, ?);", false, hashId, path)) {
					// could create the hash-based path ID, all good
					pathsCache.putInCache(hashId, path);
					return hashId;
				}

				// there is conflict on this hash code, take the next available value instead
				db.query("INSERT INTO ccdb_paths (path) VALUES (?);", false, path);

				// always execute the select, in case another instance has inserted it in the
				// mean time
				db.query("SELECT pathid FROM ccdb_paths WHERE path=?;", false, path);

				if (db.moveNext()) {
					value = db.geti(1);
					pathsCache.putInCache(hashId, path);
					return value;
				}
			}
		}

		return null;
	}

	/**
	 * @param pathID
	 * @return cleaned up value, if any
	 */
	public static synchronized String removePathID(final Integer pathID) {
		return pathsCache.removeById(pathID);
	}

	public static void selectFromCcdbPaths(final String columnName, final String pathPattern, DBFunctions db) {
		if (pathPattern.contains("%"))
			db.query("SELECT " + columnName + " FROM ccdb_paths WHERE path LIKE ? ORDER BY path;", false, pathPattern);
		else
			db.query("SELECT " + columnName + " FROM ccdb_paths WHERE path ~ ? ORDER BY path;", false, "^" + pathPattern);
	}

	protected static synchronized String getPath(final Integer pathId) {	// must be always correct
		String value = pathsCache.getValueFromCache(pathId);

		if(value != null) {
			return value;
		}

		try (DBFunctions db = getDB()) {
			db.query("SELECT path FROM ccdb_paths WHERE pathId=?;", false, pathId);

			if (db.moveNext()) {
				value = db.gets(1);

				pathsCache.putInCache(pathId, value);
			}
		}

		return value;
	}

	private static final Cache metadataCache = new Cache(!multiMasterVersion);

	protected static synchronized Integer getMetadataID(final String metadataKey, final boolean createIfNotExists) {
		if (metadataKey == null || metadataKey.isBlank())
			return null;

		Integer value = metadataCache.getIdFromCache(metadataKey);

		if (value != null)
			return value;

		try (DBFunctions db = getDB()) {
			db.query("SELECT metadataId FROM ccdb_metadata WHERE metadataKey=?;", false, metadataKey);

			if (db.moveNext()) {
				value = db.geti(1);
				metadataCache.putInCache(value, metadataKey);
				return value;
			}

			if (createIfNotExists) {
				final Integer hashId = absHashCode(metadataKey);

				if (hashId > 0
						&& db.query("INSERT INTO ccdb_metadata(metadataId, metadataKey) VALUES (?, ?);", false, hashId,
								metadataKey)) {
					metadataCache.putInCache(hashId, metadataKey);
					return hashId;
				}

				db.query("INSERT INTO ccdb_metadata (metadataKey) VALUES (?);", false, metadataKey);

				db.query("SELECT metadataId FROM ccdb_metadata WHERE metadataKey=?;", false, metadataKey);

				if (db.moveNext()) {
					value = db.geti(1);
					metadataCache.putInCache(value, metadataKey);
					return value;
				}
			}
		}

		return null;
	}

	/**
	 * Convert from metadata primary key (integer) to the String representation of
	 * it (as users passed them in the request)
	 *
	 * @param metadataId
	 * @return the string representation of this metadata key
	 */
	public static synchronized String getMetadataString(final Integer metadataId) {
		String value = metadataCache.getValueFromCache(metadataId);

		if (value != null)
			return value;

		try (DBFunctions db = getDB()) {
			db.query("SELECT metadataKey FROM ccdb_metadata WHERE metadataId=?;", false, metadataId);

			if (db.moveNext()) {
				value = db.gets(1);

				metadataCache.putInCache(metadataId, value);
			}
		}

		return value;
	}

	private static final Cache contentTypeCache = new Cache(!multiMasterVersion);

	protected static synchronized Integer getContentTypeID(final String contentType, final boolean createIfNotExists) {
		if (contentType == null || contentType.isBlank())
			return null;

		Integer value = contentTypeCache.getIdFromCache(contentType);

		if (value != null)
			return value;

		try (DBFunctions db = getDB()) {
			db.query("SELECT contentTypeId FROM ccdb_contenttype WHERE contentType=?", false, contentType);

			if (db.moveNext()) {
				value = db.geti(1);
				contentTypeCache.putInCache(value, contentType);
				return value;
			}

			if (createIfNotExists) {
				final Integer hashId = absHashCode(contentType);

				if (hashId > 0
						&& db.query("INSERT INTO ccdb_contenttype (contentTypeId, contentType) VALUES (?, ?);", false,
								hashId, contentType)) {
					contentTypeCache.putInCache(hashId, contentType);
					return hashId;
				}

				db.query("INSERT INTO ccdb_contenttype (contentType) VALUES (?);", false, contentType);

				db.query("SELECT contentTypeId FROM ccdb_contenttype WHERE contentType=?;", false, contentType);

				if (db.moveNext()) {
					value = db.geti(1);
					contentTypeCache.putInCache(value, contentType);
					return value;
				}
			}
		}

		return null;
	}

	protected static synchronized String getContentType(final Integer contentTypeId) {
		String value = contentTypeCache.getValueFromCache(contentTypeId);

		if (value != null)
			return value;

		try (DBFunctions db = getDB()) {
			db.query("SELECT contentType FROM ccdb_contenttype WHERE contentTypeId=?;", false, contentTypeId);

			if (db.moveNext()) {
				value = db.gets(1);

				contentTypeCache.putInCache(contentTypeId, value);
			}
		}

		return value;
	}

	/**
	 * Retrieve from the database the only object that has this object ID
	 *
	 * @param id the requested ID. Cannot be <code>null</code>.
	 * @return the object with this ID, if it exists. Or <code>null</code> if not.
	 */
	public static final SQLObject getObject(final UUID id) {
		try (Timing t = new Timing(monitor, "getObject_ms")) {
			if (id == null)
				return null;

			try (DBFunctions db = getDB()) {
				if (!db.query(
						selectAllFromCCDB() + " WHERE id=?;",
						false, id)) {
					System.err.println("Query execution error");
					return null;
				}

				if (db.moveNext())
					return SQLObject.fromDb(db);
			}

			return null;
		}
	}

	/**
	 * @param parser
	 * @return the most recent matching object
	 */
	public static SQLObject getMatchingObject(final RequestParser parser) {
		try (Timing t = new Timing(monitor, "getMatchingObject_ms")) {
			if(multiMasterVersion) {
				return SQLObjectCachelessImpl.getMatchingObject(parser);
			} else {
				return SQLObjectImpl.getMatchingObject(parser);
			}
		}
	}

	protected static boolean parseOptionsToQuery(
			final RequestParser parser,
			StringBuilder query,
			List<Object> arguments,
			boolean stopWhenNoSuchMetadata
	) {
		if (parser.uuidConstraint != null) {
			query.append(" AND id=?");

			arguments.add(parser.uuidConstraint);
		}

		if (parser.startTimeSet) {
			query.append(" AND to_timestamp(?) AT TIME ZONE 'UTC' <@ validity");

			arguments.add(parser.startTime / 1000.);
		}

		if (parser.notAfter > 0) {
			query.append(" AND createTime<=?");

			arguments.add(parser.notAfter);
		}

		if (parser.notBefore > 0) {
			query.append(" AND createTime>=?");

			arguments.add(parser.notBefore);
		}

		if (parser.flagConstraints.size() > 0)
			for (final Map.Entry<String, String> constraint : parser.flagConstraints.entrySet()) {
				final String key = constraint.getKey();

				final Integer metadataId = getMetadataID(key, false);

				if (metadataId == null)
					if (stopWhenNoSuchMetadata)
						return false;
					else
						continue;

				final String value = constraint.getValue();

				query.append(" AND metadata -> ? = ?");

				arguments.add(metadataId.toString());
				arguments.add(value);
			}
		return true;
	}



	/**
	 * @param parser
	 * @return the most recent matching object
	 */
	public static Collection<SQLObject> getAllMatchingObjects(final RequestParser parser) {
		if(multiMasterVersion) {
			return SQLObjectCachelessImpl.getAllMatchingObjects(parser);
		} else {
			return SQLObjectImpl.getAllMatchingObjects(parser);
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("ID: ").append(id.toString()).append('\n');
		sb.append("Path: ").append(getPath()).append('\n');
		sb.append("Validity: ").append(validFrom).append(" - ").append(validUntil).append(" (")
				.append(new Date(validFrom)).append(" - ").append(new Date(validUntil)).append(")\n");
		sb.append("Initial validity limit: ").append(initialValidity).append(" (").append(new Date(initialValidity))
				.append(")\n");
		sb.append("Created: ").append(createTime).append(" (").append(new Date(createTime)).append(")\n");
		sb.append("Last modified: ").append(lastModified).append(" (").append(new Date(lastModified)).append(")\n");
		sb.append("Original file: ").append(fileName).append(", size: ").append(size).append(", md5: ").append(md5)
				.append(", content type: ").append(getContentType()).append('\n');
		sb.append("Uploaded from: ").append(uploadedFrom).append('\n');

		Map<String, String> metadata = getMetadataKeyValue();
		if (metadata != null && metadata.size() > 0) {
			sb.append("Metadata:\n");

			for (final Map.Entry<String, String> entry : metadata.entrySet())
				sb.append("  ").append(entry.getKey()).append(" = ").append(entry.getValue())
						.append('\n');
		}

		return sb.toString();
	}

	/**
	 * @return an AliEn GUID with all the details of this object
	 */
	GUID toGUID() {
		final GUID guid = GUIDUtils.getGUID(id, true);

		if (guid.exists())
			// It should not exist in AliEn, these UUIDs are created only in CCDB's space
			return null;

		guid.size = size;
		guid.md5 = StringFactory.get(md5);
		guid.gowner = guid.owner = StringFactory.get("ccdb");
		guid.perm = "755";
		guid.ctime = new Date(createTime);
		guid.expiretime = null;
		guid.type = 0;
		guid.aclId = -1;

		return guid;
	}

	@Override
	public int compareTo(final SQLObject o) {
		final long diff = o.createTime - this.createTime;

		if (diff < 0)
			return -1;

		if (diff > 0)
			return 1;

		return o.id.compareTo(this.id);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == this)
			return true;

		if (obj == null || !(obj instanceof SQLObject))
			return false;

		return compareTo((SQLObject) obj) == 0;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	/**
	 * @param o
	 * @return strictly positive integer value of the hashcode of the given object
	 */
	public static Integer absHashCode(final Object o) {
		return Integer.valueOf((int) (Math.abs((long) (o.hashCode()) % Integer.MAX_VALUE)) + 1);
	}
}
