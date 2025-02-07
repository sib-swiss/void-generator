package swiss.sib.swissprot.servicedescription;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import swiss.sib.swissprot.servicedescription.sparql.Helper;

public class FindGraphs {

	private static final String PREFFERED_QUERY = "SELECT DISTINCT ?g WHERE {GRAPH ?g {} }";

	private static final String FALLBACK_QUERY = "SELECT DISTINCT ?g WHERE {GRAPH ?g { ?s ?p ?o}}";

	public static final Set<IRI> VIRTUOSO_GRAPHS = Stream
			.of("http://www.openlinksw.com/schemas/virtrdf#", "http://www.w3.org/ns/ldp#",
					"urn:activitystreams-owl:map", "urn:core:services:sparql")
			.map(SimpleValueFactory.getInstance()::createIRI).collect(Collectors.toSet());

	public static Set<IRI> findAllNonVirtuosoGraphs(RepositoryConnection connection, AtomicInteger scheduledQueries,
			AtomicInteger finishedQueries) {
		Set<IRI> res = new HashSet<>();
		findGraphs(connection, res, PREFFERED_QUERY, scheduledQueries, finishedQueries);
		if (res.isEmpty()) {
			findGraphs(connection, res, FALLBACK_QUERY, scheduledQueries, finishedQueries);
		}

		return res;

	}

	private static void findGraphs(RepositoryConnection connection, Set<IRI> res, String query,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		scheduledQueries.incrementAndGet();
		try (final TupleQueryResult foundGraphs = Helper.runTupleQuery(query, connection)) {
			while (foundGraphs.hasNext()) {
				final BindingSet next = foundGraphs.next();
				Binding binding = next.getBinding("g");
				if (binding != null) {
					final IRI graphIRI = (IRI) binding.getValue();
					if (!VIRTUOSO_GRAPHS.contains(graphIRI))
						res.add(graphIRI);
				}
			}
		} catch (RDF4JException e) {
			// Ignore this failure!
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

}
