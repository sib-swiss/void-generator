package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class SparqlCounterTest extends CommonTest {

	private static String prev;

	@BeforeAll
	private static void setRdf4jIteratorDebugToTrue() {
		prev = System.getProperty("org.eclipse.rdf4j.repository.debug");
		System.setProperty("org.eclipse.rdf4j.repository.debug", "true");
	}

	@AfterAll
	private static void resetRdf4jIteratorDebugToTrue() {
		if (prev != null) {
			System.setProperty("org.eclipse.rdf4j.repository.debug", prev);
		}
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void findAllGraphs(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		var e = sparqlCounters.findAllGraphs(cv).join();
		assertNull(e);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctBnodeObjectsInAGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctBnodeObjectsInAGraph(cv.with(emptyGd));
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctBnodeObjectsInDefaultGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctBnodeObjectsInDefaultGraph(cv);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctBnodeSubjectsInAgraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctBnodeSubjectsInAgraph(cv.with(emptyGd));
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctBnodeSubjectsInDefaultGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctBnodeSubjectsInDefaultGraph(cv);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctIriObjectsForDefaultGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctIriObjectsForDefaultGraph(cv);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctIriObjectsInAGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctIriObjectsInAGraph(cv.with(emptyGd));
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctIriSubjectsAndObjectsInAGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctIriSubjectsAndObjectsInAGraph(cv.with(emptyGd));
		if (OptimizeFor.QLEVER.equals(of)) {
			assertEquals(2, cv.finishedQueries().get());
		} else {
			assertEquals(1, cv.finishedQueries().get());
		}
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctIriSubjectsForDefaultGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctIriSubjectsForDefaultGraph(cv);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctLiteralObjectsInAGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctLiteralObjectsInAGraph(cv.with(emptyGd));
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctLiteralObjectsForDefaultGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctLiteralObjectsForDefaultGraph(cv);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countDistinctObjectsSubjectsInDefaultGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countDistinctObjectsSubjectsInDefaultGraph(cv, true, true);
		assertEquals(4, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countTriplesInNamedGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countTriplesInNamedGraph(cv.with(emptyGd));
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countTriplesLinkingTwoTypesInDifferentGraphs(OptimizeFor of) {
		var cv = createCommonVariables();
		var predicatePartition = new PredicatePartition(RDF.PREDICATE);
		var sourceClass = new ClassPartition(
				SimpleValueFactory.getInstance().createIRI("http://example.com/sourceClass"));
		var targetClass = new ClassPartition(
				SimpleValueFactory.getInstance().createIRI("http://example.com/targetClass"));
		
		var sourceGraph = new GraphDescription();
		sourceGraph.setGraphName("http://example.com/graph");
		var targetGraph = new GraphDescription();
		targetGraph.setGraphName("http://example.com/otherGraph");
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countTriplesLinkingTwoTypesInDifferentGraphs(cv.with(emptyGd), sourceClass.getClazz(), Set.of(targetClass), targetGraph, predicatePartition);
		assertEquals(1, cv.finishedQueries().get());
	}
	
	
	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countTriplesLinkingManyTypesInDifferentGraphs(OptimizeFor of) {
		var cv = createCommonVariables();
		var predicatePartition = new PredicatePartition(RDF.PREDICATE);
		var sourceClass1 = new ClassPartition(
				SimpleValueFactory.getInstance().createIRI("http://example.com/sourceClass"));
		var tc1 = new ClassPartition(
				SimpleValueFactory.getInstance().createIRI("http://example.com/targetClass1"));
		
		var tc2 = new ClassPartition(
				SimpleValueFactory.getInstance().createIRI("http://example.com/targetClass2"));
		
		var tc3 = new ClassPartition(
				SimpleValueFactory.getInstance().createIRI("http://example.com/targetClass3"));
		
		var sourceGraph = new GraphDescription();
		sourceGraph.setGraphName("http://example.com/graph");
		var targetGraph = new GraphDescription();
		targetGraph.setGraphName("http://example.com/otherGraph");
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		sparqlCounters.countTriplesLinkingTwoTypesInDifferentGraphs(cv.with(emptyGd), sourceClass1.getClazz(), Set.of(tc1, tc2, tc3), targetGraph, predicatePartition);
		assertTrue(cv.finishedQueries().get() == 1 || cv.finishedQueries().get() == 3, "If optimized for qlever or group by, only one query is needed, otherwise 3");
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countUniqueObjectsPerPredicateInGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		var predicatePartition = new PredicatePartition(RDF.PREDICATE);
		sparqlCounters.countUniqueObjectsPerPredicateInGraph(cv.with(emptyGd), predicatePartition);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void countUniqueSubjectPerPredicateInGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		var predicatePartition = new PredicatePartition(RDF.PREDICATE);
		sparqlCounters.countUniqueSubjectPerPredicateInGraph(cv.with(emptyGd), predicatePartition);
		assertEquals(1, cv.finishedQueries().get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void findPredicatesAndClassesInAGraph(OptimizeFor of) {
		var cv = createCommonVariables();
		var sparqlCounters = new SparqlCounters(of, schedule, schedule);
		var knownPredicates = Set.of(RDF.TYPE);
		sparqlCounters.findPredicatesAndClassesInAGraph(cv.with(emptyGd), knownPredicates, null);
		assertEquals(3, futures.size());
		assertEquals(3, cv.finishedQueries().get());
	}
}
