package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import ch.alice.o2.ccdb.RequestParser;
import lazyj.DBFunctions;

/**
 * Remove all matching objects in a single operation.
 *
 * @author costing
 * @since 2018-06-08
 */
@WebServlet("/truncate/*")
public class SQLTruncate extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(SQLTruncate.class.getCanonicalName());

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "TRUNCATE_ms")) {
			final RequestParser parser = new RequestParser(request, true);

			final Collection<SQLObject> matchingObjects = SQLObject.getAllMatchingObjects(parser);

			if (matchingObjects != null && matchingObjects.size() > 0) {
				for (final SQLObject object : matchingObjects) {
					if (!object.delete()) {
						response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot delete " + object.id);
						return;
					}

					AsyncPhysicalRemovalThread.queueDeletion(object);
				}

				response.setHeader("Deleted", matchingObjects.size() + " objects");

				response.sendError(HttpServletResponse.SC_NO_CONTENT);

				try (DBFunctions db = SQLObject.getDB()) {
					SQLObject.selectFromCcdbPaths("pathid", parser.path, db);

					final List<Integer> pathIds = new LinkedList<>();

					while (db.moveNext())
						pathIds.add(Integer.valueOf(db.geti(1)));

					for (final Integer pathID : pathIds) {
						db.query("SELECT 1 FROM ccdb WHERE pathid=? LIMIT 1;", false, pathID);

						if (!db.moveNext())
							if (db.query("DELETE FROM ccdb_paths WHERE pathid=?;", false, pathID))
								SQLObject.removePathID(pathID);
					}

				}
			}
			else
				response.sendError(HttpServletResponse.SC_NOT_MODIFIED);
		}
	}
}
