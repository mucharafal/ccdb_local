package ch.alice.o2.ccdb.webserver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Service;
import org.apache.catalina.Valve;
import org.apache.catalina.Wrapper;
import org.apache.catalina.authenticator.SSLAuthenticator;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.util.ServerInfo;
import org.apache.catalina.valves.ErrorReportValve;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;
import org.apache.tomcat.util.descriptor.web.LoginConfig;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.JAKeyStore;
import alien.user.LdapCertificateRealm;
import alien.user.UserFactory;
import ch.alice.o2.ccdb.Options;

/**
 * Configure an embedded Tomcat instance
 *
 * @author costing
 * @since 2017-10-13
 */
public class EmbeddedTomcat extends Tomcat {

	private static final String VERSION = "1.0.12";

	private static transient final Monitor monitor = MonitorFactory.getMonitor(EmbeddedTomcat.class.getCanonicalName());

	private static final String tempDir;

	private static int enforceSSL = 0;

	static {
		System.setProperty("force_fork", "false");

		final String tmpDir = System.getenv("TMPDIR");

		if (tmpDir != null && !tmpDir.isBlank()) {
			final File fTmpDir = new File(tmpDir);

			if (!fTmpDir.exists() || !fTmpDir.isDirectory() || !fTmpDir.canWrite())
				System.err.println("Indicated TMPDIR is not a valid temp dir: " + tmpDir + ", ignoring and falling back to " + System.getProperty("java.io.tmpdir"));
			else
				System.setProperty("java.io.tmpdir", tmpDir);
		}

		tempDir = System.getProperty("java.io.tmpdir");

		System.setProperty(Globals.CATALINA_HOME_PROP, tempDir);

		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		System.setProperty("org.apache.catalina.connector.CoyoteAdapter.ALLOW_BACKSLASH", "true");
	}

	/**
	 * Debug level, set to TOMCAT_DEBUG to &gt;0 to get more info on errors.
	 */
	final int debugLevel;

	/**
	 * Address to bind to
	 */
	final String address;

	private final StandardContext ctx;

	private static final List<Logger> hardReferencesToLoggers = new ArrayList<>();

	/**
	 * @param defaultAddress
	 *            default listening address for the Tomcat server. Either "localhost" (default for testing servers) or "*" for production instances.
	 * @throws ServletException
	 */
	public EmbeddedTomcat(final String defaultAddress) throws ServletException {
		super();

		// This is to disable Tomcat from creating work directories, nothing needs to be compiled on the fly
		debugLevel = Options.getIntOption("tomcat.debug", 1);

		// Disable all console logging by default
		if (debugLevel < 2)
			LogManager.getLogManager().reset();

		if (debugLevel > 2) {
			for (final String s : Arrays.asList("org.apache.catalina", "alien")) {
				final Logger l = Logger.getLogger(s);
				l.setLevel(Level.FINEST);
				hardReferencesToLoggers.add(l);
			}
		}

		address = Options.getOption("tomcat.address", defaultAddress);

		setPort(Options.getIntOption("tomcat.port", 8080));

		decorateConnector(getConnector());

		// Add a dummy ROOT context

		ctx = (StandardContext) addContext(getHost(), "", null);
		ctx.setWorkDir(tempDir);

		if (Options.getIntOption("ccdb.ssl", 0) > 0)
			initializeSSLEndpoint();
		else
			getConnector().setRedirectPort(0);

		addCORSHeaderFilter();
	}

	/**
	 * Set the `Access-Control-Allow-Origin: *` header to all answers
	 *
	 * @author costing
	 * @since Nov 19, 2020
	 */
	public static final class CORSFilter implements Filter {
		@Override
		public void destroy() {
			// nothing yet
		}

		@Override
		public void init(final FilterConfig arg0) throws ServletException {
			// nothing yet
		}

		@Override
		public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain chain) throws IOException, ServletException {
			final HttpServletResponse response = (HttpServletResponse) resp;
			response.setHeader("Access-Control-Allow-Origin", "*");
			response.setHeader("Access-Control-Allow-Headers", "range");
			response.setHeader("Access-Control-Expose-Headers", "content-range,content-length,accept-ranges");
			response.setHeader("Access-Control-Allow-Methods", "HEAD,GET");

			chain.doFilter(req, resp);
		}
	}

	/**
	 *
	 */
	private void addCORSHeaderFilter() {
		final FilterDef filter1definition = new FilterDef();
		filter1definition.setFilterName(CORSFilter.class.getSimpleName());
		filter1definition.setFilterClass(CORSFilter.class.getName());
		ctx.addFilterDef(filter1definition);

		final FilterMap filter1mapping = new FilterMap();
		filter1mapping.setFilterName(CORSFilter.class.getSimpleName());
		filter1mapping.addURLPattern("/*");
		ctx.addFilterMap(filter1mapping);
	}

	private static void passConnectorProperty(final Connector connector, final String key, final int defaultValue) {
		connector.setProperty(key, String.valueOf(Options.getIntOption(key, defaultValue)));
	}

	private void decorateConnector(final Connector connector) {
		connector.setProperty("address", address);
		connector.setProperty("encodedSolidusHandling", "PASS_THROUGH");

		passConnectorProperty(connector, "maxKeepAliveRequests", 1000);

		// large headers are needed since alternate locations include access envelopes, that are rather large (default is 8KB)
		passConnectorProperty(connector, "maxHttpHeaderSize", 100000);

		// same, let's allow for a lot of custom headers to be set (default is 100)
		passConnectorProperty(connector, "maxHeaderCount", 1000);

		// clients have 10s to connect
		passConnectorProperty(connector, "connectionTimeout", 10000);

		// and 5 minutes max to upload an object
		connector.setProperty("disableUploadTimeout", "false");
		passConnectorProperty(connector, "connectionUploadTimeout", 300000);

		// default connector value is 100 in Tomcat
		passConnectorProperty(connector, "acceptCount", 200);
		passConnectorProperty(connector, "acceptorThreadPriority", Thread.MAX_PRIORITY);

		// default number of theads in Tomcat is also 20
		passConnectorProperty(connector, "maxThreads", 200);

		// same default value as in Tomcat
		connector.setProperty("compression", Options.getOption("compression", "off"));
	}

	/**
	 * @param connector
	 * @return <code>true</code> if monitoring was attached to this connector
	 */
	public static boolean attachMonitoring(final Connector connector) {
		final Executor executor = connector.getProtocolHandler().getExecutor();

		if (executor instanceof ThreadPoolExecutor) {
			final ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;

			monitor.addMonitoring("server_status_" + connector.getPort(), (names, values) -> {
				names.add("active_threads");
				values.add(Double.valueOf(tpe.getActiveCount()));

				names.add("max_threads");
				values.add(Double.valueOf(tpe.getMaximumPoolSize()));

				names.add("tomcat_version");
				values.add(ServerInfo.getServerNumber());

				names.add("ccdb_version");
				values.add(VERSION);
			});

			return true;
		}
		else
			if (executor instanceof org.apache.tomcat.util.threads.ThreadPoolExecutor) {
				final org.apache.tomcat.util.threads.ThreadPoolExecutor tpe = (org.apache.tomcat.util.threads.ThreadPoolExecutor) executor;

				monitor.addMonitoring("server_status", (names, values) -> {
					names.add("active_threads");
					values.add(Double.valueOf(tpe.getActiveCount()));

					names.add("max_threads");
					values.add(Double.valueOf(tpe.getMaximumPoolSize()));

					names.add("tomcat_version");
					values.add(ServerInfo.getServerNumber());

					names.add("ccdb_version");
					values.add(VERSION);
				});

				return true;
			}

		System.err.println("Cannot monitor Tomcat executor on port " + connector.getPort() + (executor != null ? " of type " + executor.getClass().getCanonicalName() : ""));
		return false;
	}

	/**
	 * @param className
	 * @param mapping
	 * @return the newly created wrapper around the
	 */
	public Wrapper addServlet(final String className, final String mapping) {
		final Wrapper wrapper = Tomcat.addServlet(ctx, className.substring(className.lastIndexOf('.') + 1), className);
		wrapper.addMapping(mapping);
		wrapper.setLoadOnStartup(0);
		return wrapper;
	}

	@Override
	public void start() throws LifecycleException {
		super.start();

		for (final Connector connector : getService().findConnectors())
			if (connector.getState() == LifecycleState.FAILED) {
				System.err.println("Failed to start the embedded Tomcat listening on " + address + ":" + connector.getPort() + ".");

				if (debugLevel < 2)
					System.err.println("Set -Dtomcat.debug=2 (or export TOMCAT_DEBUG=2) to see the logging messages from the server.");

				throw new LifecycleException("Cannot bind on " + address + ":" + port);
			}

		// everything is ok, we can start monitoring the pools
		for (final Connector connector : getService().findConnectors())
			attachMonitoring(connector);

		final StandardHost host = (StandardHost) getHost();

		for (final Valve v : host.getPipeline().getValves())
			if (v instanceof ErrorReportValve) {
				final ErrorReportValve erv = (ErrorReportValve) v;
				erv.setShowServerInfo(false);
			}
	}

	/**
	 * @return the port for the default connector
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Block forever waiting for the server to exit (will never do normally)
	 */
	public void blockWaiting() {
		getServer().await();
	}

	/**
	 * @return <code>true</code> if the SSL endpoint could be created ok, <code>false</code> otherwise
	 */
	public boolean initializeSSLEndpoint() {
		final Service service = getService();
		try {
			final Connector sslConnector = createSslConnector();
			service.addConnector(sslConnector);
		}
		catch (final Exception e) {
			System.err.println("Could not initialize the SSL connector: " + e.getMessage());
			return false;
		}

		final LdapCertificateRealm ldapRealm = new LdapCertificateRealm();
		ldapRealm.setTransportGuaranteeRedirectStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
		getEngine().setRealm(ldapRealm);

		// Use AliEn's LDAP to look up users and roles
		final LoginConfig loginConfig = new LoginConfig();
		loginConfig.setRealmName(LdapCertificateRealm.class.getCanonicalName());
		loginConfig.setAuthMethod("CLIENT-CERT");

		ctx.setLoginConfig(loginConfig);

		enforceSSL = Options.getIntOption("ccdb.ssl.enforce", 1);

		if (enforceSSL > 1) {
			addSecurityConstraint(getAddOrUpdateConstraint());
			addSecurityConstraint(getRemovalConstraint());
		}

		ctx.setRealm(ldapRealm);
		ctx.getPipeline().addValve(new SSLAuthenticator());

		return true;
	}

	private void addSecurityConstraint(final SecurityConstraint constraint) {
		for (final String role : constraint.findAuthRoles())
			if (!ctx.findSecurityRole(role))
				ctx.addSecurityRole(role);

		ctx.addConstraint(constraint);
	}

	private static SecurityConstraint getAddOrUpdateConstraint() {
		// Protect all write operations
		final SecurityCollection defaultPath = new SecurityCollection("defaultPath", "Default path protection");
		defaultPath.addPattern("/*");
		defaultPath.addMethod("POST");
		defaultPath.addMethod("PUT");

		// Require SSL for the data changing methods of the default path
		final SecurityConstraint addOrUpdateConstraint = new SecurityConstraint();
		addOrUpdateConstraint.setDisplayName("SSL certificate required");
		addOrUpdateConstraint.addCollection(defaultPath);
		addOrUpdateConstraint.addAuthRole(Options.getOption("ldap.role", "users"));
		addOrUpdateConstraint.setAuthConstraint(true);
		addOrUpdateConstraint.setUserConstraint("CONFIDENTIAL");

		return addOrUpdateConstraint;
	}

	private static SecurityConstraint getRemovalConstraint() {
		final SecurityCollection truncateCalls = new SecurityCollection("truncateCalls", "Protect TRUNCATE calls");
		truncateCalls.addPattern("/truncate/*");

		final SecurityCollection defaultPathRemoval = new SecurityCollection("defaultPathRemoval", "Removal requests on the default path");
		defaultPathRemoval.addPattern("/*");
		defaultPathRemoval.addMethod("DELETE");

		// restrict access to /truncate/ and data removal requests to admin only
		final SecurityConstraint removalConstraint = new SecurityConstraint();
		removalConstraint.setDisplayName("SSL certificate required");
		removalConstraint.addCollection(truncateCalls);
		removalConstraint.addCollection(defaultPathRemoval);
		removalConstraint.addAuthRole("admin");
		removalConstraint.setAuthConstraint(true);
		removalConstraint.setUserConstraint("CONFIDENTIAL");

		return removalConstraint;
	}

	/**
	 * Create SSL connector for the Tomcat server
	 *
	 * @param tomcatPort
	 * @throws Exception
	 */
	private Connector createSslConnector() throws Exception {
		final int tomcatPort = Options.getIntOption("tomcat.port.ssl", 8443);

		getConnector().setRedirectPort(tomcatPort);

		final String keystorePass = new String(JAKeyStore.pass);

		final String dirName = System.getProperty("java.io.tmpdir") + File.separator;
		final String keystoreName = dirName + "keystore.jks_" + UserFactory.getUserID();
		final String truststoreName = dirName + "truststore.jks_" + UserFactory.getUserID();

		if (!JAKeyStore.saveKeyStore(JAKeyStore.getKeyStore(), keystoreName, JAKeyStore.pass))
			return null;

		if (!JAKeyStore.saveKeyStore(JAKeyStore.trustStore, truststoreName, JAKeyStore.pass))
			return null;

		final Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");

		connector.setPort(tomcatPort);
		connector.setSecure(true);
		connector.setScheme("https");
		connector.setProperty("keyAlias", "User.cert");
		connector.setProperty("keystorePass", keystorePass);
		connector.setProperty("keystoreType", "JKS");
		connector.setProperty("keystoreFile", keystoreName);
		connector.setProperty("truststorePass", keystorePass);
		connector.setProperty("truststoreType", "JKS");
		connector.setProperty("truststoreFile", truststoreName);
		connector.setProperty("clientAuth", "true");
		connector.setProperty("sslProtocol", "TLS");
		connector.setProperty("SSLEnabled", "true");
		connector.setProperty("maxThreads", "200");

		decorateConnector(connector);

		return connector;
	}

	/**
	 * @return the SSL enforcing level. Possible values are:<ul>
	 * <li>0 : no enforcing, writing can be done on either HTTP or HTTPS</li>
	 * <li>1 : HTTPS is required for uploads with a valid Grid identity, role is not checked</li>
	 * <li>2 : Production roles are required for upload in arbitrary paths, other users can write in their home directories</li>
	 * </ul>
	 */
	public static int getEnforceSSL() {
		return enforceSSL;
	}
}
