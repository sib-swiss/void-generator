package swiss.sib.swissprot.servicedescription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import virtuoso.rdf4j.driver.VirtuosoRepository;

/**
 * Runs the exact same tutorial fixture Generate.update() is exercised
 * against in GenerateTest, but against a real Virtuoso instance instead of
 * an in-memory RDF4J store. This is the one backend with its own, entirely
 * separate counting pipeline (JDBC/SQL via VirtuosoCounters, with
 * RoaringBitmap-backed distinct-subject/-object tracking persisted as
 * sidecar files next to the void file -- see Generate.createSaver()) that
 * had zero test coverage before this: GenerateTest's "virtuoso" runs all
 * went through the same SPARQL-over-RDF4J path as every other backend,
 * because Generate.update() only switches to the real Virtuoso counting
 * code when `repository instanceof VirtuosoRepository`, which a SailRepository
 * never is, however it is configured via --optimize-for.
 *
 * Needs a reachable Docker daemon -- only runs under
 * `mvn verify -Pvirtuoso-it`, never under plain `mvn test`.
 */
@Testcontainers
public class VirtuosoContainerIT {

	@Container
	private static final GenericContainer<?> VIRTUOSO = new GenericContainer<>("openlink/virtuoso-opensource-7:latest")
			.withExposedPorts(1111, 8890)
			// The image declares an anonymous VOLUME at /opt/virtuoso-opensource/database
			// for persistence. Rootless Podman's overlay storage driver can fail trying
			// to "copy up" the image's original content into that volume on container
			// creation ("permission denied" doing an lstat under .../overlay/.../merged).
			// A tmpfs mount here needs no copy-up and sidesteps that entirely -- fine
			// for this test, which never needs the data to survive the container.
			.withTmpFs(Map.of("/opt/virtuoso-opensource/database", "rw"))
			// Without DBA_PASSWORD, the image generates a random dba password
			// instead of defaulting to "dba" -- match the credentials this test
			// (and void-generator's own --user/--password CLI defaults) use.
			.withEnv("DBA_PASSWORD", "dba")
			.waitingFor(Wait.forListeningPorts(1111, 8890))
			.withStartupTimeout(Duration.ofMinutes(3));

	@TempDir
	private File tempDir;

	private VirtuosoRepository repository;

	@BeforeEach
	public void loadTestData() throws Exception {
		String jdbcUrl = "jdbc:virtuoso://" + VIRTUOSO.getHost() + ":" + VIRTUOSO.getMappedPort(1111)
				+ "/charset=UTF-8";
		// Virtuoso's own well-known default credentials, which also happen to be
		// void-generator's own --user/--password CLI defaults.
		repository = new VirtuosoRepository(jdbcUrl, "dba", "dba");
		repository.init();
		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			URL resource = VirtuosoContainerIT.class.getResource("/tutorial.trig");
			assertNotNull(resource, "test fixture tutorial.trig not found on the classpath");
			connection.add(resource, RDFFormat.TRIG);
			connection.commit();
		}
	}

	@AfterEach
	public void shutdownRepository() {
		repository.shutDown();
	}

	@Test
	public void generateAgainstRealVirtuoso() throws Exception {
		Generate g = new Generate();
		g.setGraphNames(new HashSet<>());
		File sdFile = new File(tempDir, "void.ttl");
		g.setSdFile(sdFile);
		g.setIriOfVoidAsString("https://example.org/.well-known/void");
		g.setRepository(repository);
		g.setRepositoryLocator("https://example.org/sparql");
		// Generate.update() auto-detects `repository instanceof VirtuosoRepository`
		// and switches to VirtuosoCounters regardless of this -- set for clarity.
		g.setOptimizeFor(OptimizeFor.VIRTUOSO.name());

		int failed = g.update();

		assertEquals(0, failed, "Generate.update() reported unfinished queries");
		assertEquals(g.getFinishedQueries(), g.getScheduledQueries());
		assertTrue(sdFile.exists());
		assertTrue(sdFile.length() > 0);
		assertNoDoubleHashInAnyIri(sdFile);
	}

	/**
	 * Same blanket invariant GenerateTest checks for every other backend (see
	 * its own assertNoDoubleHashInAnyIri): no IRI void-generator mints should
	 * ever contain a doubled '#' fragment separator, regardless of which
	 * counting pipeline produced the data behind it.
	 */
	private void assertNoDoubleHashInAnyIri(File sdFile) throws Exception {
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
