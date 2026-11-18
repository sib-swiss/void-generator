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
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
		assertTrue(tempDir.isDirectory());
		File sdFile = new File(tempDir, "void.ttl");
		Generate g = newGenerate(of, sdFile);
		g.update();
		assertEquals(g.getFinishedQueries(), g.getScheduledQueries());
		assertTrue(sdFile.exists());
		assertTrue(sdFile.length() > 0);
		assertNoMalformedSubPartitionIris(sdFile);
		assertNoDoubleHashInAnyIri(sdFile);
		assertDistinctSubjectsProvesFunctional(sdFile);
	}

	/**
	 * --iri-of-void ending in '#' is a documented, common convention (the
	 * project's own README example uses one) but was never exercised by
	 * generate() above, whose fixture iri-of-void does not end in '#'. That
	 * gap let getResourceForGraph, generateLinkset and the sd:Service-level
	 * IRIs in generateStatements (sparql-default-dataset/-graph/
	 * -graph-collection) each mint a doubled '#' for this exact, ordinary
	 * configuration -- caught by manually reproducing it, not by this suite,
	 * which is why this test exists now.
	 */
	@ParameterizedTest
	@MethodSource("optimizeFor")
	public void generateWithTrailingHashIriOfVoid(String of) throws Exception {
		File sdFile = new File(tempDir, "void.ttl");
		Generate g = newGenerate(of, sdFile, "https://example.org/.well-known/void#");
		g.update();
		assertTrue(sdFile.length() > 0);
		assertNoMalformedSubPartitionIris(sdFile);
		assertNoDoubleHashInAnyIri(sdFile);
	}

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

	/**
	 * Shared setup for a Generate run against the tutorial fixture, used by
	 * every test in this class -- extracted so they can't silently drift out
	 * of sync with each other.
	 */
	private Generate newGenerate(String optimizeFor, File sdFile) {
		return newGenerate(optimizeFor, sdFile, "https://example.org/.well-known/void");
	}

	private Generate newGenerate(String optimizeFor, File sdFile, String iriOfVoid) {
		Generate g = new Generate();
		g.setGraphNames(new HashSet<>());
		g.setSdFile(sdFile);
		g.setIriOfVoidAsString(iriOfVoid);
		g.setRepository(sr);
		g.setRepositoryLocator("https://example.org/sparql");
		g.setOptimizeFor(optimizeFor);
		return g;
	}

	/**
	 * In the tutorial fixture every Apple has exactly one color (3 color
	 * triples, 3 distinct subjects with a color): triples == distinctSubjects
	 * proves color is functional for Apple (sh:maxCount 1), which is new
	 * information a downstream SHACL/LinkML generator did not have before
	 * count_subjects_with_a_type_and_predicate gained its COUNT(DISTINCT
	 * ?subject) -- void:distinctSubjects was never set on a class-scoped
	 * property partition at all before that. This holds (and is checked)
	 * under every backend, unlike the minCount side (see
	 * distinctSubjectsProvesRequiredProperty below): the class-scoped
	 * triples/distinctSubjects counts come from this one query regardless of
	 * --optimize-for, but the class' own void:entities count comes from a
	 * different, backend-specific query path, and QLever's happens to count
	 * Apple's entities as 8 instead of 3 for this same fixture -- a
	 * pre-existing discrepancy unrelated to this change.
	 */
	private void assertDistinctSubjectsProvesFunctional(File sdFile) throws IOException {
		Model model = Rio.parse(sdFile.toURI().toURL().openStream(), RDFFormat.TURTLE);
		Resource colorPropertyPartitionUnderApple = findApplesColorPropertyPartition(model);

		long triples = asLong(singleObject(model, colorPropertyPartitionUnderApple, VOID.TRIPLES));
		long distinctSubjects = asLong(singleObject(model, colorPropertyPartitionUnderApple, VOID.DISTINCT_SUBJECTS));

		assertEquals(triples, distinctSubjects, "triples == distinctSubjects should prove color is functional (maxCount 1)");
	}

	/**
	 * Same fixture, but only under the plain "sparql" backend, where the
	 * class' own void:entities count is known to be exact (see the class
	 * comment above): distinctSubjects == entities proves color is required
	 * for Apple (sh:minCount 1).
	 */
	@Test
	public void distinctSubjectsProvesRequiredProperty() throws Exception {
		File sdFile = new File(tempDir, "void.ttl");
		Generate g = newGenerate(OptimizeFor.SPARQL.name(), sdFile);
		g.update();

		Model model = Rio.parse(sdFile.toURI().toURL().openStream(), RDFFormat.TURTLE);
		IRI appleClass = SimpleValueFactory.getInstance()
				.createIRI("https://example.org/void-generator/test/schema#Apple");
		Resource appleClassPartition = (Resource) singleSubject(model, VOID.CLASS, appleClass);
		long appleEntities = asLong(singleObject(model, appleClassPartition, VOID.ENTITIES));
		long distinctSubjects = asLong(
				singleObject(model, findApplesColorPropertyPartition(model), VOID.DISTINCT_SUBJECTS));

		assertEquals(3L, appleEntities, "Apple's void:entities");
		assertEquals(appleEntities, distinctSubjects, "distinctSubjects should prove color is required (minCount 1)");
	}

	private static Resource findApplesColorPropertyPartition(Model model) {
		IRI appleClass = SimpleValueFactory.getInstance()
				.createIRI("https://example.org/void-generator/test/schema#Apple");
		IRI colorProperty = SimpleValueFactory.getInstance()
				.createIRI("https://example.org/void-generator/test/schema#color");

		Resource appleClassPartition = (Resource) singleSubject(model, VOID.CLASS, appleClass);
		Resource colorPropertyPartitionUnderApple = null;
		for (Value pp : model.filter(appleClassPartition, VOID.PROPERTY_PARTITION, null).objects()) {
			if (model.contains((Resource) pp, VOID.PROPERTY, colorProperty)) {
				colorPropertyPartitionUnderApple = (Resource) pp;
			}
		}
		assertNotNull(colorPropertyPartitionUnderApple, "Apple's color property partition not found");
		return colorPropertyPartitionUnderApple;
	}

	private static Value singleSubject(Model model, IRI p, Value o) {
		var it = model.filter(null, p, o).iterator();
		assertTrue(it.hasNext(), () -> "expected exactly one subject for " + p + ", found none");
		Value v = it.next().getSubject();
		assertTrue(!it.hasNext(), () -> "expected exactly one subject for " + p + ", found more than one");
		return v;
	}

	private static Value singleObject(Model model, Resource s, IRI p) {
		var it = model.filter(s, p, null).iterator();
		assertTrue(it.hasNext(), () -> "expected exactly one " + p + " statement on " + s + ", found none");
		Value v = it.next().getObject();
		assertTrue(!it.hasNext(), () -> "expected exactly one " + p + " statement on " + s + ", found more than one");
		return v;
	}

	private static long asLong(Value value) {
		return ((org.eclipse.rdf4j.model.Literal) value).longValue();
	}

	/**
	 * iri-of-void above ("https://example.org/.well-known/void") does not end
	 * in '#', which used to make getResourceForSubPartition mint IRIs with no
	 * separator at all between the void location and the hashed local name
	 * (e.g. ".../voidfruits!...!-Fruit-color" instead of
	 * ".../void#fruits!...!-Fruit-color") -- still syntactically valid, but
	 * wrong, and inconsistent with getResourceForPartition's own IRIs in the
	 * same file. Guard against regressing that fix.
	 *
	 * Checks RDF4J's own namespace/localName split (the same split any
	 * consumer of these IRIs will use) rather than hand-rolled index
	 * arithmetic: a prior version of this check only confirmed *some* '#'
	 * existed before the first '!', which would also pass on an unrelated
	 * earlier '#' followed by a still-missing separator, or on a genuinely
	 * doubled '#' (both fragments present) -- neither is a valid IRI.
	 */
	private void assertNoMalformedSubPartitionIris(File sdFile) throws IOException {
		Model model = Rio.parse(sdFile.toURI().toURL().openStream(), RDFFormat.TURTLE);
		for (var statement : model) {
			for (Value term : new Value[] { statement.getSubject(), statement.getObject() }) {
				if (term instanceof IRI iri && iri.getLocalName().indexOf('!') >= 0) {
					String ns = iri.getNamespace();
					long hashCount = ns.chars().filter(c -> c == '#').count();
					assertEquals(1, hashCount,
							() -> "sub-partition IRI's namespace has " + hashCount
									+ " '#' characters, expected exactly 1: " + iri.stringValue());
					assertTrue(ns.endsWith("#"),
							() -> "sub-partition IRI is missing a '#' separator right before its hashed local name: "
									+ iri.stringValue());
				}
			}
		}
	}
	
	static String[] optimizeFor() {
		return new String[] { OptimizeFor.QLEVER.name(), OptimizeFor.SPARQL.name(), OptimizeFor.SPARQLUNION.name(), "lala" };
	}
}
