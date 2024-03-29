package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.multicast.Blob;
import ch.alice.o2.ccdb.multicast.UDPReceiver;
import ch.alice.o2.ccdb.servlets.formatters.FormatterFactory;
import ch.alice.o2.ccdb.servlets.formatters.SQLFormatter;
import lazyj.Format;
import lazyj.Utils;

/**
 * In-memory implementation of CCDB. This servlet implements browsing of
 * objects in a particular path
 *
 * @author costing
 * @since 2018-04-26
 */
@WebServlet("/browse/*")
public class MemoryBrowse extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		final boolean prepare = lazyj.Utils.stringToBool(request.getParameter("prepare"), false);

		// list of objects matching the request
		// URL parameters are:
		// task name / detector name [ / time [ / UUID ] | [ / query string]* ]
		// if time is missing - get the last available time
		// query string example: "quality=2"

		final RequestParser parser = new RequestParser(request, true);

		CCDBUtils.disableCaching(response);

		if (prepare && parser.latestFlag && Memory.REDIRECT_TO_UPSTREAM) {
			// go to the authoritative source to make sure the correct object version is distributed to everybody
			response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
			response.setHeader("Location", Memory.getLocationURL(request));
		}

		final Collection<Blob> matchingObjects = getAllMatchingObjects(parser);

		final SQLFormatter formatter = FormatterFactory.getFormatter(request);

		response.setContentType(formatter.getContentType());

		try (PrintWriter pw = response.getWriter()) {
			formatter.start(pw);

			formatter.header(pw);

			boolean first = true;

			if (matchingObjects != null)
				for (final Blob object : matchingObjects) {
					if (first)
						first = false;
					else
						formatter.middle(pw);

					formatter.format(pw, object);
				}

			formatter.footer(pw);

			if (!parser.wildcardMatching) {
				formatter.setExtendedReport(Utils.stringToBool(request.getParameter("report"), false));

				// It is not clear which subfolders to list in case of a wildcard matching of
				// objects. As the full hierarchy was included in the search there is no point
				// in showing them, so just skip this section.
				formatter.subfoldersListingHeader(pw);

				final StringBuilder suffix = new StringBuilder();

				if (parser.startTimeSet)
					suffix.append('/').append(parser.startTime);

				if (parser.uuidConstraint != null)
					suffix.append('/').append(parser.uuidConstraint);

				for (final Map.Entry<String, String> entry : parser.flagConstraints.entrySet())
					suffix.append('/').append(entry.getKey()).append('=').append(entry.getValue());

				// TODO: search for all keys that have as prefix the current parser.path

				final Map<String, SubfolderStats> subfolderAggregate = new TreeMap<>();

				for (final Map.Entry<String, List<Reference<Blob>>> entry : UDPReceiver.currentCacheContent.entrySet()) {
					final String key = entry.getKey();

					if (parser.path == null || parser.path.length() == 0 || key.startsWith(parser.path + "/")) {
						String folder = parser.path != null ? key.substring(parser.path.length()) : key;

						while (folder.startsWith("/"))
							folder = folder.substring(1);

						String firstLevelFolder = folder;

						boolean isOwnData = true;

						if (firstLevelFolder.indexOf('/') > 0) {
							firstLevelFolder = firstLevelFolder.substring(0, firstLevelFolder.indexOf('/'));
							isOwnData = false;
						}

						final SubfolderStats stats = subfolderAggregate.computeIfAbsent(firstLevelFolder, k -> new SubfolderStats());

						for (final Reference<Blob> sb : entry.getValue()) {
							final Blob b = sb.get();

							if (b != null)
								stats.addObject(isOwnData, b.getSize());
						}
					}
				}

				for (final Map.Entry<String, SubfolderStats> entry : subfolderAggregate.entrySet()) {
					String subfolder = entry.getKey();

					if (parser.path != null && parser.path.length() > 0)
						subfolder = parser.path + "/" + subfolder;

					final SubfolderStats stats = entry.getValue();

					formatter.subfoldersListing(pw, subfolder, subfolder + suffix, stats.ownCount, stats.ownSize, stats.subfolderCount, stats.subfolderSize);
				}

				formatter.subfoldersListingFooter(pw, 0, 0);
			}

			formatter.end(pw);
		}
	}

	private static class SubfolderStats {
		int ownCount = 0;
		int ownSize = 0;

		int subfolderCount = 0;
		int subfolderSize = 0;

		public void addObject(final boolean own, final long size) {
			if (own) {
				ownCount++;
				ownSize += size;
			}
			else {
				subfolderCount++;
				subfolderSize += size;
			}
		}
	}

	/**
	 * @param parser
	 * @return all matching objects given the parser constraints
	 */
	public static final Collection<Blob> getAllMatchingObjects(final RequestParser parser) {
		final List<Blob> matchingObjects = new ArrayList<>();

		String pathFilter = parser.path != null ? parser.path : "";

		Pattern pFilter = null;

		if (pathFilter.indexOf('*') >= 0 || pathFilter.indexOf('%') >= 0) {
			parser.wildcardMatching = true;

			pathFilter = Format.replace(pathFilter, "%", ".*");

			pFilter = Pattern.compile("^" + pathFilter + ".*$");
		}
		else {
			if (pathFilter.endsWith("/"))
				pathFilter = pathFilter.substring(0, pathFilter.length() - 1);
		}

		for (final Map.Entry<String, List<Reference<Blob>>> entry : UDPReceiver.currentCacheContent.entrySet()) {
			final String path = entry.getKey();

			if ((pFilter == null && path.equals(pathFilter)) || (pFilter != null && pFilter.matcher(path).matches())) {
				Blob bBest = null;

				for (final Reference<Blob> sb : entry.getValue()) {
					final Blob b = sb.get();

					if (b == null)
						continue;

					if (Memory.blobMatchesParser(b, parser)) {
						if (parser.latestFlag) {
							if (bBest == null || b.compareTo(bBest) < 0)
								bBest = b;
						}
						else
							matchingObjects.add(b);
					}
					else
						System.err.println("Ignoring: " + b);
				}

				if (bBest != null)
					matchingObjects.add(bBest);
			}
		}

		if (parser.browseLimit > 0 && parser.browseLimit < matchingObjects.size()) {
			// apply the limit to the entire set, which can in principle contain several paths if regex was used

			Collections.sort(matchingObjects);
			return matchingObjects.subList(0, parser.browseLimit);
		}

		return matchingObjects;
	}
}
