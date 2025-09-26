package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class CountTriplesLinkingTypesInDifferentGraphsTest extends CommonTest {

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void empty(OptimizeFor of) throws IOException {
		var cv = createCommonVariables();
		var pre = new PredicatePartition(RDF.PREDICATE);
		var otherGd = new GraphDescription();
		otherGd.setGraphName("http://example.org/otherGraph");
		var ls = new LinkSetToOtherGraph(pre, OWL.CLASS, OWL.ANNOTATION, otherGd, otherGd.getGraph());

		var counter = new CountTriplesLinkingTwoTypesInDifferentGraphs(cv.with(emptyGd), ls, pre, of);
		assertNull(counter.call());
		assertEquals(1, cv.finishedQueries().get());
	}
}
