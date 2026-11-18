package swiss.sib.swissprot.servicedescription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;

import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs the tutorial fixture through Generate.update() against a real
 * QLever instance, the same way VirtuosoContainerIT does for Virtuoso.
 * QLever has no counterpart to Virtuoso's separate JDBC counting pipeline
 * -- it goes through the ordinary SparqlCounters/SPARQLRepository path,
 * same as the "sparql"/"sparqlunion" backends -- but it is its own SPARQL
 * dialect/engine with its own quirks (see OptimizeFor.QLEVER,
 * optimizeFor.preferGroupBy()), so it is worth exercising for real too.
 *
 * qlever-server can be started directly, with no separate qlever-index
 * build step first, and loaded the same way Virtuoso is: over an RDF4J
 * RepositoryConnection, adding the tutorial.trig fixture directly (QLever
 * handles multi-graph TriG as SPARQL UPDATE INSERT DATA natively, so no
 * format conversion is needed either).
 *
 * SPARQL UPDATE (but not SELECT) requires an access token, set with -a at
 * server startup. RDF4J's SPARQLRepository refuses to send URL-encoded
 * POST requests to an endpoint URL that itself contains a query string
 * ("?access-token=..."), so the token is passed as an `Authorization:
 * Bearer <token>` HTTP header on a custom HttpClient instead, matching
 * the pattern already used elsewhere for a real QLever deployment (see
 * ManagedQLever.getWriteRepository() in the expasy4j-sparql project).
 *

 * Needs a reachable Docker daemon -- only runs under `mvn verify
 * -Pqlever-it`, never under plain `mvn test`.
 */
public class QleverContainerIT {

	private static final int QLEVER_PORT = 7918;
	private static final String ACCESS_TOKEN = "void-generator-test-token";

	@TempDir
	private File tempDir;

	private GenericContainer<?> server;
	private SPARQLRepository repository;

	@BeforeEach
	public void startServerAndLoadTestData() throws Exception {
		// The image's docker-entrypoint.sh hardcodes an exact check,
		// [ "$(pwd)" != "/data" ], and prints its welcome banner and exits
		// whenever that's true -- regardless of command, TTY, or anything
		// else. The working directory must be exactly /data, not any other
		// path. Once there, the entrypoint itself prepends "-c" to a bare
		// command if not already present, so passing it explicitly here is
		// redundant but harmless.
		server = new GenericContainer<>("adfreiburg/qlever:latest")
				.withExposedPorts(QLEVER_PORT)
				.withWorkingDirectory("/data")
				// Running as the image's default "qlever" user hits the
				// entrypoint's usermod/groupmod-then-su-qlever path, which
				// changes the qlever account's UID/GID to whatever UID/GID env
				// vars are given -- but /data (auto-created by Docker as the
				// working directory) was already created owned by qlever's
				// *original* baked-in UID, before that remap happens. Any UID
				// we pick there ends up not matching /data's actual owner,
				// "permission denied" either way. The entrypoint takes a much
				// simpler path if the container's runtime user isn't "qlever"
				// at all: just exec the command directly, no user-switching,
				// no UID/GID games -- force root so that path is taken, and
				// root doesn't care who owns /data anyway.
				.withCreateContainerCmdModifier(cmd -> cmd.withTty(true).withUser("0:0"))
				// qlever-server refuses to start at all without an existing index
				// (it tries to open voidtest.meta-data.json and fails) -- there's
				// no way to start truly index-less and populate everything via
				// SPARQL UPDATE. Build a trivial empty one first (/dev/null, zero
				// triples) in the same command, then chain straight into starting
				// the server over it; the real data is loaded via SPARQL UPDATE
				// afterward regardless, so the index content itself doesn't matter
				// here, just that one exists.
				.withCommand("-c",
						"echo '{}' > voidtest.settings.json && "
								+ "qlever-index -i voidtest -s voidtest.settings.json "
								+ "--vocabulary-type on-disk-compressed -F nt -f /dev/null && "
								+ "qlever-server -i voidtest -p " + QLEVER_PORT + " -a " + ACCESS_TOKEN)
				// Wait.forListeningPort() falsely reported success here (no `nc`
				// in the image, container networking apparently confusing its
				// internal/external port probing) while nothing was actually
				// listening. Server.cpp logs this exact line once it's really
				// ready; match that instead of probing the port at all.
				.waitingFor(Wait.forLogMessage(".*The server is ready, listening for requests.*", 1))
				.withStartupTimeout(Duration.ofMinutes(2));
		server.start();

		String endpoint = "http://" + server.getHost() + ":" + server.getMappedPort(QLEVER_PORT) + "/sparql";
		repository = new SPARQLRepository(endpoint);
		repository.setHttpClient(HttpClientBuilder.create()
				.setDefaultHeaders(List.of(new BasicHeader("Authorization", "Bearer " + ACCESS_TOKEN))).build());
		repository.init();

		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			URL resource = QleverContainerIT.class.getResource("/tutorial.trig");
			assertNotNull(resource, "test fixture tutorial.trig not found on the classpath");
			connection.add(resource, RDFFormat.TRIG);
			connection.commit();
		}
	}

	@AfterEach
	public void stopServer() {
		if (repository != null) {
			repository.shutDown();
		}
		if (server != null) {
			server.stop();
		}
	}

	@Test
	public void generateAgainstRealQlever() throws Exception {
		Generate g = new Generate();
		g.setGraphNames(new HashSet<>());
		File sdFile = new File(tempDir, "void.ttl");
		g.setSdFile(sdFile);
		g.setIriOfVoidAsString("https://example.org/.well-known/void");
		g.setRepository(repository);
		g.setRepositoryLocator("http://" + server.getHost() + ":" + server.getMappedPort(QLEVER_PORT) + "/sparql");
		g.setOptimizeFor(OptimizeFor.QLEVER.name());

		int failed = g.update();

		assertEquals(0, failed, "Generate.update() reported unfinished queries");
		assertEquals(g.getFinishedQueries(), g.getScheduledQueries());
		assertTrue(sdFile.exists());
		assertTrue(sdFile.length() > 0);
		assertNoDoubleHashInAnyIri(sdFile);
	}

	/**
	 * Same blanket invariant VirtuosoContainerIT/GenerateTest check for every
	 * other backend: no IRI void-generator mints should ever contain a
	 * doubled '#' fragment separator.
	 */
	private void assertNoDoubleHashInAnyIri(File sdFile) throws IOException {
		Model model = Rio.parse(sdFile.toURI().toURL().openStream(), RDFFormat.TURTLE);
		for (var statement : model) {
			for (Value term : new Value[] { statement.getSubject(), statement.getPredicate(), statement.getObject() }) {
				if (term instanceof IRI iri) {
					assertTrue(!iri.stringValue().contains("##"),
							() -> "IRI has a doubled '#' fragment separator: " + iri.stringValue());
				}
			}
		}
	}
}
