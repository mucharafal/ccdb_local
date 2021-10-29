package ch.alice.o2.ccdb.servlets;

import static ch.alice.o2.ccdb.servlets.ServletHelper.printUsage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import ch.alice.o2.ccdb.Options;
import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.multicast.Utils;
import ch.alice.o2.ccdb.webserver.EmbeddedTomcat;
import lazyj.DBFunctions;
import utils.CachedThreadPool;

/**
 * SQL-backed implementation of CCDB. Files reside on this server and/or a separate storage and clients are served directly or redirected to one of the other replicas for the actual file access.
 *
 * @author costing
 * @since 2017-10-13
 */
@WebServlet("/*")
@MultipartConfig(fileSizeThreshold = 1024 * 1024 * 100)
public class SQLBacked extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(SQLBacked.class.getCanonicalName());

	private static Logger logger = Logger.getLogger(SQLBacked.class.getCanonicalName());

	private static List<SQLNotifier> notifiers = new ArrayList<>();

	/**
	 * The base path of the file repository
	 */
	public static final String basePath = Options.getOption("file.repository.location", System.getProperty("user.home") + System.getProperty("file.separator") + "QC");

	/**
	 * Set to `true` on the Online instance, that should not cache any IDs but always use the database values to ensure consistency
	 */
	public static final boolean multiMasterVersion = lazyj.Utils.stringToBool(Options.getOption("multimaster", null), false);

	private static final boolean localCopyFirst = lazyj.Utils.stringToBool(Options.getOption("local.copy.first", null), false);

	private static boolean hasGridBacking = false;

	private static boolean hasUDPSender = false;

	private static CachedThreadPool asyncOperations = new CachedThreadPool(16, 1, TimeUnit.SECONDS);

	private static final boolean defaultSyncFetch = lazyj.Utils.stringToBool(Options.getOption("syncfetch", null), false);

	static {
		monitor.addMonitoring("stats", new SQLStatsExporter(null));

		MonitorFactory.getMonitor("ch.alice.o2.ccdb.servlets.qc_stats").addMonitoring("qc_stats", new SQLStatsExporter("qc"));

		if (Options.getIntOption("gridreplication.enabled", 0) == 1) {
			try {
				SEUtils.getSE(0);

				System.err.println("Grid replication enabled, central services connection tested OK");

				notifiers.add(AsyncReplication.getInstance());

				hasGridBacking = true;
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				System.err.println("Grid replication is enabled but upstream connection to JCentral doesn't work, disabling replication at this point");
				System.err.println("If this is an expected behavior please set gridreplication.enabled=0");
			}
		}
		else
			System.err.println("Replication to Grid storage elements is disabled, all data will stay on this machine, under " + basePath);

		if (notifiers.size() == 0)
			notifiers.add(SQLLocalRemoval.getInstance());

		final SQLtoUDP udpSender = SQLtoUDP.getInstance();

		if (udpSender != null) {
			notifiers.add(udpSender);
			hasUDPSender = true;
		}

		final SQLtoHTTP httpSender = SQLtoHTTP.getInstance();

		if (httpSender != null)
			notifiers.add(httpSender);
	}

	static boolean isLocalCopyFirst() {
		return localCopyFirst;
	}

	/**
	 * @return <code>true</code> if the instance has Grid backing and can upload / download files from SEs
	 */
	public static boolean gridBacking() {
		return hasGridBacking;
	}

	/**
	 * @return <code>true</code> if any unicast or multicast UDP sending is to be done by this instance
	 */
	public static boolean udpSender() {
		return hasUDPSender;
	}

	@Override
	protected void doHead(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "HEAD_ms")) {
			doGet(request, response, true);
		}
	}

	/**
	 * @return the notifiers to take actions on object events
	 */
	static Collection<SQLNotifier> getNotifiers() {
		return Collections.unmodifiableCollection(notifiers);
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "GET_ms")) {
			doGet(request, response, false);
		}
	}

	private static void doGet(final HttpServletRequest request, final HttpServletResponse response, final boolean head) throws IOException {
		// list of objects matching the request
		// URL parameters are:
		// task name / detector name [ / time [ / UUID ] | [ / query string]* ]
		// if time is missing - get the last available time
		// query string example: "quality=2"

		final RequestParser parser = new RequestParser(request);

		if (!parser.ok) {
			printUsage(request, response);
			return;
		}

		CCDBUtils.disableCaching(response);

		final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		if (parser.cachedValue != null && matchingObject.id.equals(parser.cachedValue)) {
			response.sendError(HttpServletResponse.SC_NOT_MODIFIED);
			return;
		}

		final String rPrepare = request.getParameter("prepare");

		final boolean syncPrepare = "sync".equalsIgnoreCase(rPrepare);

		final boolean asyncPrepare = lazyj.Utils.stringToBool(rPrepare, false) || syncPrepare;

		if (syncPrepare || defaultSyncFetch)
			AsyncMulticastQueue.stage(matchingObject);

		if (asyncPrepare)
			AsyncMulticastQueue.queueObject(matchingObject);

		final String clientIPAddress = request.getRemoteAddr();

		final boolean httpOnly = lazyj.Utils.stringToBool(request.getParameter("HTTPOnly"), false);

		if (setAltLocationHeaders(response, matchingObject, clientIPAddress, httpOnly) && localCopyFirst) {
			SQLDownload.download(head, matchingObject, request, response);
			return;
		}

		setHeaders(matchingObject, response);

		CCDBUtils.sendRedirect(response, matchingObject.getAddresses(clientIPAddress, httpOnly).iterator().next());
	}

	private static boolean setAltLocationHeaders(final HttpServletResponse response, final SQLObject obj, final String clientIPAddress, final boolean httpOnly) {
		boolean hasLocalReplica = false;

		for (final Integer replica : obj.replicas)
			if (replica.intValue() == 0 && obj.getLocalFile(false) != null) {
				hasLocalReplica = true;
				break;
			}

		for (final String address : obj.getAddresses(clientIPAddress, httpOnly))
			response.addHeader("Content-Location", address.replace('\n', '|'));

		return hasLocalReplica;
	}

	/**
	 * Write out the MD5 header, if known
	 *
	 * @param obj
	 * @param response
	 */
	static void setMD5Header(final SQLObject obj, final HttpServletResponse response) {
		if (obj.md5 != null && !obj.md5.isEmpty())
			response.setHeader("Content-MD5", obj.md5);
	}

	/**
	 * Set the response headers with the internal and any user-set metadata information
	 *
	 * @param obj
	 * @param response
	 */
	static void setHeaders(final SQLObject obj, final HttpServletResponse response) {
		response.setDateHeader("Date", System.currentTimeMillis());
		response.setHeader("Valid-Until", String.valueOf(obj.validUntil));
		response.setHeader("Valid-From", String.valueOf(obj.validFrom));

		if (obj.initialValidity != obj.validUntil)
			response.setHeader("InitialValidityLimit", String.valueOf(obj.initialValidity));

		response.setHeader("Created", String.valueOf(obj.createTime));
		response.setHeader("ETag", "\"" + obj.id + "\"");

		response.setDateHeader("Last-Modified", obj.getLastModified());

		for (final Map.Entry<String, String> metadataEntry : obj.getMetadataKeyValue().entrySet()) {
			response.setHeader(metadataEntry.getKey(), metadataEntry.getValue());
		}
	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		// create the given object and return the unique identifier to it
		// URL parameters are:
		// task name / detector name / start time [/ end time] [ / flag ]*
		// mime-encoded blob is the value to be stored
		// if end time is missing then it will be set to the same value as start time
		// flags are in the form "key=value"

		try (Timing t = new Timing(monitor, "POST_ms")) {
			final RequestParser parser = new RequestParser(request);

			if (!parser.ok) {
				printUsage(request, response);
				return;
			}

			final Collection<Part> parts = request.getParts();

			if (parts.size() == 0) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "POST request doesn't contain the data to upload");
				return;
			}

			if (parts.size() > 1) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "A single object can be uploaded at a time");
				return;
			}

			if (EmbeddedTomcat.getEnforceSSL() > 1) {
				// Role is checked

				final AliEnPrincipal account = UserFactory.get(request);

				if (account == null) {
					response.sendError(HttpServletResponse.SC_FORBIDDEN, "Show your ID please");
					return;
				}

				if (!canWrite(account, parser.path)) {
					response.sendError(HttpServletResponse.SC_FORBIDDEN, "You are not allowed to write to this path");
					return;
				}
			}

			final Part part = parts.iterator().next();

			final SQLObject newObject = SQLObject.fromRequest(request, parser.path, parser.uuidConstraint);

			final File targetFile = newObject.getLocalFile(true);

			if (targetFile == null) {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot create target file to perform the upload");
				return;
			}

			final MessageDigest md5;

			try {
				md5 = MessageDigest.getInstance("MD5");
			}
			catch (@SuppressWarnings("unused") final NoSuchAlgorithmException e) {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot initialize the MD5 digester");
				return;
			}

			newObject.size = 0;

			try (FileOutputStream fos = new FileOutputStream(targetFile); InputStream is = part.getInputStream()) {
				final byte[] buffer = new byte[1024 * 16];

				int n;

				while ((n = is.read(buffer)) >= 0) {
					fos.write(buffer, 0, n);
					md5.update(buffer, 0, n);
					newObject.size += n;
				}
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot upload the blob to the local file " + targetFile.getAbsolutePath());

				if (!targetFile.delete())
					logger.log(Level.WARNING, "Cannot delete target file of failed upload " + targetFile.getAbsolutePath());

				return;
			}

			for (final Map.Entry<String, String> constraint : parser.flagConstraints.entrySet())
				newObject.setProperty(constraint.getKey(), constraint.getValue());

			newObject.uploadedFrom = request.getRemoteHost();
			newObject.fileName = part.getSubmittedFileName();
			newObject.setContentType(part.getContentType());
			newObject.md5 = Utils.humanReadableChecksum(md5.digest()); // UUIDTools.getMD5(targetFile);
			newObject.setProperty("partName", part.getName());

			newObject.replicas.add(Integer.valueOf(0));

			newObject.validFrom = parser.startTime;

			newObject.setValidityLimit(parser.endTime);

			if (!newObject.save(request)) {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot insert the object in the database");
				return;
			}

			setHeaders(newObject, response);

			final String location = newObject.getAddress(Integer.valueOf(0)).iterator().next();

			response.setHeader("Location", location);
			response.setHeader("Content-Location", location);
			response.sendError(HttpServletResponse.SC_CREATED);

			asyncOperations.execute(() -> {
				for (final SQLNotifier notifier : notifiers) {
					if (notifier instanceof SQLtoUDP) {
						if (lazyj.Utils.stringToBool(newObject.getProperty("forSyncReco"), true))
							AsyncMulticastQueue.queueObject(newObject);
					}
					else
						notifier.newObject(newObject);
				}
			});

			if (monitor != null)
				monitor.addMeasurement("POST_data", newObject.size);
		}
	}

	private static boolean canWrite(final AliEnPrincipal account, final String path) {
		if (account.hasRole("ccdb"))
			return true;

		final String username = account.getName();

		final String match = "Users/" + username.charAt(0) + "/" + username + "/";

		if (path.startsWith(match))
			return true;

		return false;
	}

	@Override
	protected void doPut(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "PUT_ms")) {
			final RequestParser parser = new RequestParser(request);

			if (!parser.ok) {
				printUsage(request, response);
				return;
			}

			final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

			if (matchingObject == null) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
				return;
			}

			if (EmbeddedTomcat.getEnforceSSL() > 1) {
				// Role is checked

				final AliEnPrincipal account = UserFactory.get(request);

				if (account == null) {
					response.sendError(HttpServletResponse.SC_FORBIDDEN, "Show your ID please");
					return;
				}

				if (!canWrite(account, parser.path)) {
					response.sendError(HttpServletResponse.SC_FORBIDDEN, "You are not allowed to modify this path");
					return;
				}
			}

			for (final Map.Entry<String, String[]> param : request.getParameterMap().entrySet())
				if (param.getValue().length > 0)
					matchingObject.setProperty(param.getKey(), param.getValue()[0]);

			if (parser.endTimeSet)
				matchingObject.setValidityLimit(parser.endTime);

			final boolean changed = matchingObject.save(request);

			setHeaders(matchingObject, response);

			response.setHeader("Location", matchingObject.getAddresses(null, false).iterator().next());

			if (changed)
				response.sendError(HttpServletResponse.SC_NO_CONTENT);
			else
				response.sendError(HttpServletResponse.SC_NOT_MODIFIED);

			asyncOperations.execute(() -> {
				for (final SQLNotifier notifier : notifiers)
					notifier.updatedObject(matchingObject);
			});
		}
	}

	@Override
	protected void doDelete(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "DELETE_ms")) {
			final RequestParser parser = new RequestParser(request);

			if (!parser.ok) {
				printUsage(request, response);
				return;
			}

			final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

			if (matchingObject == null) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
				return;
			}

			if (!matchingObject.delete()) {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Could not delete the underlying record");
				return;
			}

			setHeaders(matchingObject, response);

			response.sendError(HttpServletResponse.SC_NO_CONTENT);

			asyncOperations.execute(() -> {
				for (final SQLNotifier notifier : notifiers)
					notifier.deletedObject(matchingObject);
			});
		}
	}

	@Override
	protected void doOptions(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "OPTIONS_ms")) {
			response.setHeader("Allow", "GET, POST, PUT, DELETE, OPTIONS");

			final RequestParser parser = new RequestParser(request);

			if (!parser.ok)
				return;

			final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

			if (matchingObject == null) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
				return;
			}

			setHeaders(matchingObject, response);
		}
	}

	static {
		// make sure the database structures exist when the server is initialized
		createDBStructure();
		runStatisticsRecompute();
	}

	/**
	 * Create the table structure to hold this object
	 */
	public static void createDBStructure() {
		try (DBFunctions db = SQLObject.getDB()) {
			if (db != null)
				if (db.isPostgreSQL()) {
					db.query("CREATE EXTENSION IF NOT EXISTS hstore;", true);

					db.query("CREATE TABLE IF NOT EXISTS ccdb_paths (pathId SERIAL PRIMARY KEY, path text UNIQUE NOT NULL);");
					db.query("CREATE TABLE IF NOT EXISTS ccdb_contenttype (contentTypeId SERIAL PRIMARY KEY, contentType text UNIQUE NOT NULL);");

					db.query(
							"CREATE TABLE IF NOT EXISTS ccdb (id uuid PRIMARY KEY, pathId int NOT NULL REFERENCES ccdb_paths(pathId) ON UPDATE CASCADE, validity tsrange, createTime bigint NOT NULL, replicas integer[], size bigint, "
									+ "md5 uuid, filename text, contenttype int REFERENCES ccdb_contenttype(contentTypeId) ON UPDATE CASCADE, uploadedfrom inet, initialvalidity bigint, metadata hstore, lastmodified bigint);");
					db.query("CREATE INDEX IF NOT EXISTS ccdb_pathId2_idx ON ccdb(pathId);");
					db.query("ALTER TABLE ccdb ALTER validity SET STATISTICS 10000;");
					db.query("CREATE INDEX IF NOT EXISTS ccdb_validity2_idx on ccdb using gist(validity);");

					db.query("CREATE TABLE IF NOT EXISTS ccdb_metadata (metadataId SERIAL PRIMARY KEY, metadataKey text UNIQUE NOT NULL);");
					db.query("CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT);");

					if (!db.query("SELECT * FROM ccdb LIMIT 0")) {
						System.err.println("Database communication cannot be established, please fix config.properties and/or the PostgreSQL server and try again");
						System.exit(1);
					}

					System.err.println("Database connection is verified to work");

					db.query("CREATE TABLE IF NOT EXISTS ccdb_stats (pathid int primary key, object_count bigint default 0, object_size bigint default 0);");

					db.query("CREATE TABLE IF NOT EXISTS ccdb_helper_table (key text primary key, value int);");

					db.query("SELECT count(1) FROM ccdb_helper_table;");

					if (db.geti(1) == 0)
						db.query("INSERT INTO ccdb_helper_table VALUES ('ccdb_stats_tainted', 1) ON CONFLICT (key) DO UPDATE SET value=1;");

					db.query("SELECT count(1) FROM ccdb_stats;");

					if (db.geti(1) == 0)
						recomputeStatistics();

					db.query("CREATE OR REPLACE FUNCTION ccdb_increment() RETURNS TRIGGER AS $_$\n" + "    BEGIN\n" + "        INSERT INTO \n"
							+ "            ccdb_stats (pathid, object_count, object_size) VALUES (NEW.pathid, 1, NEW.size)\n"
							+ "        ON CONFLICT (pathid) DO UPDATE SET object_count=ccdb_stats.object_count+1, object_size=ccdb_stats.object_size+NEW.size;\n" + "\n" + "        INSERT INTO\n"
							+ "            ccdb_stats (pathid, object_count, object_size) VALUES (0, 1, NEW.size)\n"
							+ "        ON CONFLICT (pathid) DO UPDATE SET object_count=ccdb_stats.object_count+1, object_size=ccdb_stats.object_size+NEW.size;\n" + "\n" + "        RETURN NEW;\n"
							+ "    END\n" + "$_$ LANGUAGE 'plpgsql';");

					db.query("CREATE OR REPLACE FUNCTION ccdb_decrement() RETURNS TRIGGER AS $_$\n" + "    BEGIN\n"
							+ "        UPDATE ccdb_stats SET object_count=object_count-1, object_size=object_size-OLD.size WHERE pathid IN (0, OLD.pathid);\n" + "        RETURN NEW;\n" + "    END\n"
							+ "$_$ LANGUAGE 'plpgsql';");

					db.query("CREATE TRIGGER ccdb_increment_trigger AFTER INSERT ON ccdb FOR EACH ROW EXECUTE PROCEDURE ccdb_increment();", true);
					db.query("CREATE TRIGGER ccdb_decrement_trigger AFTER DELETE ON ccdb FOR EACH ROW EXECUTE PROCEDURE ccdb_decrement();", true);

					// cache-less utils

					final HashMap<String, String> idNameInTable = new HashMap<>();
					idNameInTable.put("ccdb_paths", "pathid");
					idNameInTable.put("ccdb_contenttype", "contentTypeId");

					final HashMap<String, String> valueNameInTable = new HashMap<>();
					valueNameInTable.put("ccdb_paths", "path");
					valueNameInTable.put("ccdb_contenttype", "contentType");

					final String[] tables = { "ccdb_paths", "ccdb_contenttype" };
					for (final String tableName : tables) {
						final String idName = idNameInTable.get(tableName);
						final String valueName = valueNameInTable.get(tableName);
						db.query("create or replace function " + tableName + "_latest (value_to_insert text) returns bigint as $$\n"
								+ "begin\n"
								+ "    return (select " + idName + " from " + tableName + " where " + valueName + " = value_to_insert);\n"
								+ "end $$ language plpgsql;");
					}

					db.query("create or replace function ccdb_metadata_latest_keyid_value (values_to_insert hstore) returns hstore as $$\n" +
							"declare \n" +
							"	actual_ids_to_insert hstore := ''::hstore;\n" +
							"	value text;\n" +
							"begin\n" +
							"	foreach value in array akeys(values_to_insert) loop\n" +
							"		actual_ids_to_insert := actual_ids_to_insert || hstore((select metadataId from ccdb_metadata where metadataKey = value)::text, values_to_insert -> value);\n" +
							"	end loop;\n" +
							"	return actual_ids_to_insert;\n" +
							"end\n" +
							"$$ language plpgsql;");
					db.query("create or replace function ccdb_metadata_latest_key_value (keyid_value hstore) returns hstore as $$\n" +
							"declare \n" +
							"    key_value hstore := ''::hstore;\n" +
							"    id integer;\n" +
							"begin\n" +
							"    foreach id in array akeys(keyid_value) loop\n" +
							"        key_value := key_value || hstore((select metadataKey from ccdb_metadata where metadataId = id)::text, keyid_value -> cast(id as text));\n" +
							"    end loop;\n" +
							"    return key_value;\n" +
							"end\n" +
							"$$ language plpgsql;\n");
					db.query("create view ccdb_view as \n" +
							"    select ccdb.*, \n" +
							"        paths.path as path, \n" +
							"        ctype.contenttype as contenttype_value, \n" +
							"        ccdb_metadata_latest_key_value(ccdb.metadata) as metadata_key_value\n" +
							"    from ccdb  \n" +
							"        left outer join ccdb_paths as paths on ccdb.pathid = paths.pathid\n" +
							"        left outer join ccdb_contenttype as ctype on ccdb.contenttype = ctype.contenttypeid;");
				}
				else
					throw new IllegalArgumentException("Only PostgreSQL support is implemented at the moment");
		}
	}

	static final ScheduledExecutorService recomputeStatisticsScheduler = Executors.newScheduledThreadPool(1);

	private static void runStatisticsRecompute() {
		recomputeStatisticsScheduler.scheduleAtFixedRate(() -> {
			recomputeStatistics();
		}, 30, 30, TimeUnit.MINUTES);
	}

	private static void recomputeStatistics() {
		try (DBFunctions db = SQLObject.getDB()) {
			db.query("SELECT value FROM ccdb_helper_table WHERE key = 'ccdb_stats_tainted'");

			if (db.geti(1) == 1) {
				db.query("BEGIN;" +
						"TRUNCATE ccdb_stats;" +
						"INSERT INTO ccdb_stats SELECT pathid, count(1), sum(size) FROM ccdb GROUP BY 1;" +
						"INSERT INTO ccdb_stats SELECT 0, sum(object_count), sum(object_size) FROM ccdb_stats WHERE pathid!=0;" +
						"UPDATE ccdb_helper_table SET value = 0 WHERE key = 'ccdb_stats_tainted';" + 
						"COMMIT;");
			}
		}
	}
}
