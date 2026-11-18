package swiss.sib.swissprot.servicedescription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class GenerateTest {


	@TempDir
	private File tempDir;
	private SailRepository sr;

//	@BeforeAll
//	public static void setLoggingToDebug() {
//		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
//	}
	
	
	@BeforeEach
	public void loadTestData() throws RDFParseException, RepositoryException, IOException {
		MemoryStore ms = new MemoryStore();
		sr = new SailRepository(ms);
		sr.init();
		try (SailRepositoryConnection connection = sr.getConnection()) {
			connection.begin();
			// pom.xml's testResources copy *.trig to the root of target/test-classes
			// (no package-preserving targetPath), so this must be an absolute lookup --
			// a package-relative GenerateTest.class.getResource("tutorial.trig") returns
			// null here, and silently loading nothing let every generate() run below
			// pass against an empty repository without any test ever failing.
			URL resource = GenerateTest.class.getResource("/tutorial.trig");
			assertNotNull(resource, "test fixture tutorial.trig not found on the classpath");
			connection.add(resource, RDFFormat.TRIG);
			connection.commit();
		}
	
	}

	@AfterEach
	public void shutdownSail() {
		sr.shutDown();
	}

	@ParameterizedTest
	@MethodSource("optimizeFor")
	public void generate(String of) throws Exception {
		Generate g = new Generate();
		assertTrue(tempDir.isDirectory());
		g.setGraphNames(new HashSet<>());
		File sdFile = new File(tempDir, "void.ttl");
		g.setSdFile(sdFile);
		g.setIriOfVoidAsString("https://example.org/.well-known/void");
		g.setRepository(sr);
		g.setRepositoryLocator("https://example.org/sparql");
		g.setOptimizeFor(of);
		g.update();
		assertEquals(g.getFinishedQueries(), g.getScheduledQueries());
		assertTrue(sdFile.exists());
		assertTrue(sdFile.length() > 0);
		assertNoMalformedSubPartitionIris(sdFile);
	}

	/**
	 * iri-of-void above ("https://example.org/.well-known/void") does not end
	 * in '#', which used to make getResourceForSubPartition mint IRIs with no
	 * separator at all between the void location and the hashed local name
	 * (e.g. ".../voidfruits!...!-Fruit-color" instead of
	 * ".../void#fruits!...!-Fruit-color") -- still syntactically valid, but
	 * wrong, and inconsistent with getResourceForPartition's own IRIs in the
	 * same file. Guard against regressing that fix.
	 */
	private void assertNoMalformedSubPartitionIris(File sdFile) throws IOException {
		Model model = Rio.parse(sdFile.toURI().toURL().openStream(), RDFFormat.TURTLE);
		for (var statement : model) {
			for (Value term : new Value[] { statement.getSubject(), statement.getObject() }) {
				if (term instanceof IRI iri) {
					String s = iri.stringValue();
					int bang = s.indexOf('!');
					if (bang >= 0) {
						assertTrue(s.lastIndexOf('#', bang) >= 0,
								"sub-partition IRI is missing a '#' separator before its hashed local name: " + s);
					}
				}
			}
		}
	}
	
	static String[] optimizeFor() {
		return new String[] { OptimizeFor.QLEVER.name(), OptimizeFor.SPARQL.name(), OptimizeFor.SPARQLUNION.name(), "lala" };
	}
}
