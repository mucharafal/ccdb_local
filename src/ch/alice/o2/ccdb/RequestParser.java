package ch.alice.o2.ccdb;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import lazyj.Format;

/**
 * Parse the request parameters and make the constraints available to the application
 *
 * @author costing
 * @since 2017-09-22
 */
public class RequestParser {
	/**
	 * Whether or not the request was successfully parsed
	 */
	public boolean ok = false;

	/**
	 * Requested object path
	 */
	public String path;

	/**
	 * Start validity of the object (default is `now`)
	 */
	public long startTime = System.currentTimeMillis();

	/**
	 * End validity of the request (default is `now` - an open end interval, so +1ms)
	 */
	public long endTime = startTime + 1;

	/**
	 * UUID to get the detail for
	 */
	public UUID uuidConstraint = null;

	/**
	 * Client's current UUID, to be checked if it is still valid or not
	 */
	public UUID cachedValue = null;

	/**
	 * Whether or not the start time was defined
	 */
	public boolean startTimeSet = false;

	/**
	 * If the end time was set for the request
	 */
	public boolean endTimeSet = false;

	/**
	 * Snapshot timestamp (the cutoff time after which any newer created object is ignored)
	 */
	public long notAfter = 0;

	/**
	 * Only recent objects, for QC investigations
	 */
	public long notBefore = 0;

	/**
	 * Quality or other metadata constraints that have to match (or be set)
	 */
	public final Map<String, String> flagConstraints = new HashMap<>();

	/**
	 * If just the latest object version is requested, default is <code>true</code> for retrieving or getting the latest matching set. It is automatically set to <code>false</code> when the request is
	 * for /browse/..., in order to list all matching objects.
	 */
	public boolean latestFlag = true;

	/**
	 * Will be set to true when during the processing it is found to be a pattern
	 */
	public boolean wildcardMatching = false; // todo should not it be set during initialization?

	/**
	 * If strictly positive, return the most recent L number of objects.
	 * The <code>/browse/</code> area can use this in particular, and the value is read from the <code>Browse-Limit</code> HTTP header.
	 */
	public int browseLimit = -1;

	/**
	 * @param request
	 *            request to wrap around
	 */
	public RequestParser(final HttpServletRequest request) {
		this(request, false);
	}

	/**
	 * @param request
	 *            request to wrap around
	 * @param optionalTimeConstraints
	 *            whether or not the time constraints are required. If <code>false</code> then at least one time parameter has to be indicated (start time / validity moment). If <code>true</code> then
	 *            the time constraint is optional, any object found for the given key would match.
	 */
	public RequestParser(final HttpServletRequest request, final boolean optionalTimeConstraints) {
		if (request == null)
			return;

		final String servletPath = request.getServletPath();

		if (servletPath.contains("browse") || servletPath.contains("truncate"))
			latestFlag = false;

		String pathInfo = request.getRequestURI();

		if (pathInfo == null || pathInfo.isEmpty())
			return;

		if (pathInfo.startsWith(servletPath))
			pathInfo = pathInfo.substring(servletPath.length());

		final StringTokenizer st = new StringTokenizer(pathInfo, "/");

		if (st.countTokens() < 1)
			return;

		final List<String> pathElements = new ArrayList<>();

		ok = true;

		try {
			final String browseLimitHeader = request.getHeader("Browse-Limit");

			if (browseLimitHeader != null && !browseLimitHeader.isBlank())
				browseLimit = Integer.parseInt(browseLimitHeader);
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			// ignore
		}

		try {
			String previousUUID = request.getHeader("If-None-Match");

			if (previousUUID.indexOf('"') >= 0)
				previousUUID = previousUUID.substring(previousUUID.indexOf('"') + 1, previousUUID.lastIndexOf('"'));

			if (previousUUID != null && previousUUID.length() > 0)
				cachedValue = UUID.fromString(previousUUID);
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			notAfter = Long.parseLong(request.getHeader("If-Not-After"));
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			notBefore = Long.parseLong(request.getHeader("If-Not-Before"));
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		// search for path tokens, stop at the first numeric value which would be the start time
		while (st.hasMoreTokens()) {
			final String token = st.nextToken();

			if (token.isEmpty() || token.indexOf(0) >= 0)
				continue;

			try {
				final long tmp = Long.parseLong(token);

				startTime = tmp;
				endTime = startTime + 1;
				startTimeSet = true;
				break;
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				final int idx = token.indexOf('=');

				if (idx >= 0) {
					final String key = Format.decode(token.substring(0, idx).trim());
					final String value = Format.decode(token.substring(idx + 1).trim());
					flagConstraints.put(key, value);
				}
				else {
					final String decodedToken = Format.decode(token);
					pathElements.add(decodedToken);
				}
			}
		}

		// require at least one path element, but not more than 10 (safety limit)
		if (pathElements.size() < (optionalTimeConstraints ? 0 : 1) || pathElements.size() > 10 || (!optionalTimeConstraints && !startTimeSet)) {
			ok = false;
			return;
		}

		// optional arguments after the path and start time are end time, flags and UUID
		while (st.hasMoreTokens()) {
			final String token = st.nextToken();

			final int idx = token.indexOf('=');

			if (idx >= 0) {
				final String key = Format.decode(token.substring(0, idx).trim());
				final String value = Format.decode(token.substring(idx + 1).trim());
				flagConstraints.put(key, value);
			}
			else
				try {
					final long tmp = Long.parseLong(token);

					if (!endTimeSet && endTime > startTime) {
						endTime = tmp;
						endTimeSet = true;
					}
					else {
						// unexpected time constraint showing up in the request
						ok = false;
						return;
					}
				}
				catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
					try {
						uuidConstraint = UUID.fromString(token);
					}
					catch (@SuppressWarnings("unused") final IllegalArgumentException iae) {
						ok = false;
						return;
					}
				}
		}

		final StringBuilder pathBuilder = new StringBuilder();

		for (final String token : pathElements) {
			if (pathBuilder.length() > 0)
				pathBuilder.append('/');

			pathBuilder.append(token);
		}

		path = pathBuilder.toString();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("Path: ").append(path).append('\n');

		if (startTimeSet)
			sb.append("Start time: ").append(startTime).append(" (").append(new Date(startTime)).append(")\n");
		else
			sb.append("Start time not set\n");

		if (endTimeSet)
			sb.append("End time: ").append(endTime).append(" (").append(new Date(endTime)).append(")\n");

		if (uuidConstraint != null)
			sb.append("Requested UUID: ").append(uuidConstraint).append("\n");

		if (cachedValue != null)
			sb.append("Cached value: ").append(cachedValue).append("\n");

		if (notAfter > 0)
			sb.append("Snapshot time limit: ").append(notAfter).append(" (").append(new Date(notAfter)).append(")\n");

		if (notBefore > 0)
			sb.append("Newer objects limit: ").append(notBefore).append(" (").append(new Date(notBefore)).append(")\n");

		return sb.toString();
	}
}