package ch.alice.o2.ccdb.servlets.formatters;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Map;

import ch.alice.o2.ccdb.multicast.Blob;
import ch.alice.o2.ccdb.servlets.LocalObjectWithVersion;
import ch.alice.o2.ccdb.servlets.SQLObject;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-04-26
 */
class HTMLFormatter implements SQLFormatter {

	private boolean extendedReport = false;

	@Override
	public void header(final PrintWriter writer) {
		writer.print(
				"<table style='font-size:10px' border=1 cellspacing=0 cellpadding=2><thead><tr><th>ID</th><th>Valid from</th><th>Valid until</th><th>Initial validity limit</th><th>Created at</th><th>Last modified</th><th>MD5</th><th>File name</th><th>Content type</th><th>Size</th><th>Path</th><th>Metadata</th><th>Replicas</th></thead>\n");
	}

	@Override
	public void format(final PrintWriter writer, final SQLObject obj) {
		writer.print("<tr><td nowrap align=left>");
		writer.print(obj.id.toString());

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.validFrom);
		writer.println("<br>");
		final Date dFrom = new Date(obj.validFrom);
		writer.print(Format.showDate(dFrom));

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.validUntil);
		writer.println("<br>");
		final Date dUntil = new Date(obj.validUntil);
		writer.print(Format.showDate(dUntil));

		writer.print("</td><td align=right>");
		writer.print(obj.initialValidity);
		writer.println("<br>");
		final Date dInitial = new Date(obj.initialValidity);
		writer.print(Format.showDate(dInitial));

		writer.print("</td><td align=right>");
		writer.print(obj.createTime);
		writer.println("<br>");
		final Date dCreated = new Date(obj.createTime);
		writer.print(Format.showDate(dCreated));

		writer.print("</td><td align=right>");
		writer.print(obj.getLastModified());
		writer.println("<br>");
		final Date dLastModified = new Date(obj.getLastModified());
		writer.print(Format.showDate(dLastModified));

		writer.print("</td><td align=center nowrap>");
		writer.print(Format.escHtml(obj.md5));

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.fileName));

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.getContentType()));

		writer.print("</td><td align=right nowrap>");
		writer.print(obj.size);

		writer.print("</td><td align=left nowrap>");
		writer.print(Format.escHtml(obj.getPath()));

		writer.print("</td><td align=left><dl>");
		for (final Map.Entry<String, String> entry : obj.getMetadataKeyValue().entrySet()) {
			writer.print("<dt>");
			writer.print(Format.escHtml(entry.getKey()));
			writer.print("</dt><dd>");
			writer.print(Format.escHtml(entry.getValue()));
			writer.print("</dd>\n");
		}

		writer.print("</dl></td><td align=left><ul>");

		for (final Integer replica : obj.replicas) {
			for (final String address : obj.getAddress(replica, null, false)) {
				writer.print("<li><a href='");
				writer.print(Format.escHtml(address));
				writer.print("'>");

				if (replica.intValue() == 0)
					writer.print("local repo (http)");
				else
					if (replica.intValue() < 0) {
						writer.print("alien");

						if (address.startsWith("root://"))
							writer.print(" (root)");
						else
							if (address.startsWith("http"))
								writer.print(" (http)");
							else
								if (address.startsWith("alien://"))
									writer.print(" (plugin)");
					}
					else
						writer.print("SE #" + replica);

				writer.print("</a>");

				if (replica.intValue() == 0 && obj.fileName.toLowerCase().endsWith(".root")) {
					writer.print(" <a target=_blank href='/JSRoot?f=");
					writer.print(Format.encode(address));
					writer.print("&n=");
					writer.print(Format.encode(obj.fileName));
					writer.print(getJSRootOptions(obj.getProperty("drawOptions"), obj.getProperty("displayHints"), obj.getProperty("ObjectType"), obj.getProperty("item"), obj.getPath()));
					writer.print("'>ROOT browser</a>");
				}

				writer.println("</li>");
			}
		}

		writer.print("</ul></td></tr>\n");
	}

	private static String getJSRootOptions(final String displayHints, final String drawOptions, final String objectType, final String itemName, final String path) {
		String ret = "";

		if (displayHints != null)
			ret = "&opt=" + Format.encode(displayHints);

		if (drawOptions != null)
			ret += (ret.length() > 0 ? "," : "&opt=") + Format.encode(drawOptions);

		String item = itemName;

		if (item == null) {
			if (path.startsWith("qc/")) {
				item = "ccdb_object";

				// default display options for TH2D objects in the /qc/ namespace
				if (ret.length() == 0 && "TH2D".equals(objectType))
					ret = "&opt=colz,logz";
			}
		}

		if (item != null)
			ret += "&item=" + Format.encode(item);

		return ret;
	}

	@Override
	public void format(final PrintWriter writer, final LocalObjectWithVersion obj) {
		writer.print("<tr><td nowrap align=left>");
		writer.print(obj.getID());

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.getStartTime());
		writer.println("<br>");
		final Date dFrom = new Date(obj.getStartTime());
		writer.print(Format.showDate(dFrom));

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.getEndTime());
		writer.println("<br>");
		final Date dUntil = new Date(obj.getEndTime());
		writer.print(Format.showDate(dUntil));

		writer.print("</td><td align=right>");
		writer.print(obj.getInitialValidity());
		writer.println("<br>");
		final Date dInitial = new Date(obj.getInitialValidity());
		writer.print(Format.showDate(dInitial));

		writer.print("</td><td align=right>");
		writer.print(obj.getCreateTime());
		writer.println("<br>");
		final Date dCreated = new Date(obj.getCreateTime());
		writer.print(Format.showDate(dCreated));

		writer.print("</td><td align=right>");
		writer.print(obj.getLastModified());
		writer.println("<br>");
		final Date dLastModified = new Date(obj.getLastModified());
		writer.print(Format.showDate(dLastModified));

		writer.print("</td><td align=center nowrap>");
		writer.print(Format.escHtml(obj.getProperty("Content-MD5")));

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.getOriginalName()));

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.getProperty("Content-Type", "application/octet-stream")));

		writer.print("</td><td align=right nowrap>");
		writer.print(obj.getSize());

		writer.print("</td><td align=left nowrap>");
		writer.print(Format.escHtml(obj.getFolder()));

		writer.print("</td><td align=left><dl>");
		for (final Object key : obj.getUserPropertiesKeys()) {
			writer.print("<dt>");
			writer.print(Format.escHtml(key.toString()));
			writer.print("</dt><dd>");
			writer.print(Format.escHtml(obj.getProperty(key.toString())));
			writer.print("</dd>\n");
		}

		writer.print("</dl></td><td align=left><ul>");

		writer.print("<li><a href='");
		writer.print(Format.escHtml(obj.getPath()));
		writer.print("'>0");
		writer.print("</a>");

		if (obj.getOriginalName().toLowerCase().endsWith(".root")) {
			writer.print(" <a target=_blank href='/JSRoot?f=");
			writer.print(Format.encode(obj.getPath()));
			writer.print("&n=");
			writer.print(Format.encode(obj.getOriginalName()));
			writer.print(getJSRootOptions(obj.getProperty("drawOptions"), obj.getProperty("displayHints"), obj.getProperty("ObjectType"), obj.getProperty("item"), obj.getFolder()));
			writer.print("'>ROOT browser</a>");
		}

		writer.print("</li>\n");

		writer.print("</ul></td></tr>\n");
	}

	@Override
	public void footer(final PrintWriter writer) {
		writer.print("</table>\n");
	}

	@Override
	public void middle(final PrintWriter writer) {
		// nothing
	}

	@Override
	public void start(final PrintWriter writer) {
		writer.write("<!DOCTYPE html><html>\n");
	}

	@Override
	public void subfoldersListingHeader(final PrintWriter writer) {
		writer.write("<br><br><table style='font-size:10px' border=1 cellspacing=0 cellpadding=2><thead><tr><th>Subfolder</th>");

		if (extendedReport)
			writer.write("<th>Own objects</th><th>Own size</th><th>Subfolder objects</th><th>Subfolder total size</th>");

		writer.write("</tr></thead>\n");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path, final String url) {
		writer.write("<tr><td><a href='/browse/");
		writer.write(Format.escHtml(url));
		writer.write("'>");
		writer.write(Format.escHtml(path));
		writer.write("</a></td></tr>\n");
	}

	private long objectCount = 0;
	private long objectSize = 0;

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path, final String url, final long ownCount, final long ownSize, final long subfolderCount, final long subfolderSize) {
		writer.write("<tr><td><a href='/browse/");
		writer.write(Format.escHtml(url));

		if (extendedReport)
			writer.write("?report=true");

		writer.write("'>");
		writer.write(Format.escHtml(path));
		writer.write("</a></td>");

		if (extendedReport) {
			writer.write("<td align=right>");
			writer.write(ownCount > 0 ? String.valueOf(ownCount) : "-");
			writer.write("</td><td align=right>");
			writer.write(ownSize > 0 ? Format.size(ownSize) : "-");
			writer.write("</td><td align=right>");
			writer.write(subfolderCount > 0 ? String.valueOf(subfolderCount) : "-");
			writer.write("</td><td align=right>");
			writer.write(subfolderSize > 0 ? Format.size(subfolderSize) : "-");
			writer.write("</td>");
		}

		objectCount += ownCount + subfolderCount;
		objectSize += ownSize + subfolderSize;

		writer.write("</tr>\n");
	}

	@Override
	public void subfoldersListingFooter(final PrintWriter writer, final long ownCount, final long ownSize) {
		if (extendedReport) {
			writer.write("<tfoot><tr><th>TOTAL</th><th align=right>");
			writer.write(ownCount > 0 ? String.valueOf(ownCount) : "-");
			writer.write("</th><th align=right>");
			writer.write(ownSize > 0 ? Format.size(ownSize) : "-");
			writer.write("</th><th align=right>");
			writer.write(objectCount > 0 ? String.valueOf(objectCount) : "-");
			writer.write("</th><th align=right>");
			writer.write(objectSize > 0 ? Format.size(objectSize) : "-");
			writer.write("</th></tfoot>");
		}

		writer.write("</table>\n");
	}

	@Override
	public void end(final PrintWriter writer) {
		writer.write("</html>");
	}

	@Override
	public void setExtendedReport(final boolean extendedReport) {
		this.extendedReport = extendedReport;
	}

	@Override
	public void format(final PrintWriter writer, final Blob obj) {
		writer.print("<tr><td nowrap align=left>");
		writer.print(obj.getUuid());

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.getStartTime());
		writer.println("<br>");
		final Date dFrom = new Date(obj.getStartTime());
		writer.print(Format.showDate(dFrom));

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.getEndTime());
		writer.println("<br>");
		final Date dUntil = new Date(obj.getEndTime());
		writer.print(Format.showDate(dUntil));

		writer.print("</td><td align=right>");
		writer.print(obj.getInitialValidity());
		writer.println("<br>");
		final Date dInitial = new Date(obj.getInitialValidity());
		writer.print(Format.showDate(dInitial));

		writer.print("</td><td align=right>");
		writer.print(obj.getCreateTime());
		writer.println("<br>");
		final Date dCreated = new Date(obj.getCreateTime());
		writer.print(Format.showDate(dCreated));

		writer.print("</td><td align=right>");
		writer.print(obj.getLastModified());
		writer.println("<br>");
		final Date dLastModified = new Date(obj.getLastModified());
		writer.print(Format.showDate(dLastModified));

		writer.print("</td><td align=center nowrap>");
		writer.print(Format.escHtml(obj.getMD5()));

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.getOriginalName()));

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.getProperty("Content-Type", "application/octet-stream")));

		writer.print("</td><td align=right nowrap>");
		writer.print(obj.getSize());

		writer.print("</td><td align=left nowrap>");
		writer.print(Format.escHtml(obj.getKey()));

		writer.print("</td><td align=left><dl>");
		for (final Object key : obj.getMetadataMap().keySet()) {
			writer.print("<dt>");
			writer.print(Format.escHtml(key.toString()));
			writer.print("</dt><dd>");
			writer.print(Format.escHtml(obj.getProperty(key.toString())));
			writer.print("</dd>\n");
		}

		writer.print("</dl></td><td align=left><ul>");

		boolean isComplete = false;

		try {
			if (obj.isComplete()) {
				isComplete = true;

				writer.print("<li><a href='/download/" + obj.getUuid() + "'>0</a>");

				if (obj.getOriginalName().toLowerCase().endsWith(".root")) {
					writer.print(" <a target=_blank href='/JSRoot?f=");
					writer.print(Format.encode("/download/" + obj.getUuid()));
					writer.print("&n=");
					writer.print(Format.encode(obj.getOriginalName()));
					writer.print(getJSRootOptions(obj.getProperty("drawOptions"), obj.getProperty("displayHints"), obj.getProperty("ObjectType"), obj.getProperty("item"), obj.getKey()));
					writer.print("'>ROOT browser</a>");
				}

				writer.println("</li>");
			}
		}
		catch (@SuppressWarnings("unused") final IOException | NoSuchAlgorithmException e) {
			// ignore
		}

		if (!isComplete) {
			writer.print("<li>INCOMPLETE!</li>\n");
		}

		writer.print("</ul></td></tr>\n");
	}

	@Override
	public String getContentType() {
		return "text/html";
	}
}
