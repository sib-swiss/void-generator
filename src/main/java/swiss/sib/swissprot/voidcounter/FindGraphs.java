package swiss.sib.swissprot.voidcounter;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.voidcounter.sparql.Helper;

/**
 * This class is responsible for finding all graphs in the repository that are
 * not Virtuoso graphs. It uses SPARQL queries to retrieve the graph names and
 * stores them in a set.
 * 
 * If the preferred query does not return any results, it falls back to a query 
 * that is supported by more backend stores.
 */
public class FindGraphs extends QueryCallable<Set<String>, CommonVariables> {
	private static final Logger log = LoggerFactory.getLogger(FindGraphs.class);

	private final OptimizeFor optimizeFor;
	
	public FindGraphs(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		this.optimizeFor = optimizeFor;
	}


	private static final Set<String> VIRTUOSO_GRAPHS = Set.of("http://www.openlinksw.com/schemas/virtrdf#",
			"http://www.w3.org/ns/ldp#", "urn:activitystreams-owl:map", "urn:core:services:sparql",
			// Virtuoso's own RDF 1.2/RDF-star vocabulary graph, not user data --
			// newer Virtuoso builds expose this, which void-generator's authors
			// didn't know about yet.
			"urn:rdf12:vocab");

	private void findGraphs(RepositoryConnection connection, Set<String> res, String query) {
		setQuery(query);
		try (final TupleQueryResult foundGraphs = Helper.runTupleQuery(getQuery(), connection)) {
			while (foundGraphs.hasNext()) {
				final BindingSet next = foundGraphs.next();
				Binding binding = next.getBinding("g");
				if (binding != null) {
					final String graphIRI = binding.getValue().stringValue();
					if (!VIRTUOSO_GRAPHS.contains(graphIRI))
						res.add(graphIRI);
				}
			}
		} catch (RDF4JException e) {
			// Ignore this failure!
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding all graphs");
	}

	@Override
	protected void logEnd() {
		log.debug("Found all graphs");

	}

	@Override
	protected Set<String> run(RepositoryConnection connection) throws Exception {
		Set<String> res = new HashSet<>();
		String prefQuery = Helper.loadSparqlQuery("find_graphs_preferred", optimizeFor);
		findGraphs(connection, res, prefQuery);
		if (res.isEmpty()) {
			String fallbackQuery = Helper.loadSparqlQuery("find_graphs_fallback", optimizeFor);
			findGraphs(connection, res, fallbackQuery);
		}
		return res;
	}

	@Override
	protected void set(Set<String> t) {
		try {
			cv.writeLock().lock();
			for (String graph : t) {
				GraphDescription gd = new GraphDescription();
				gd.setGraphName(graph);
				cv.sd().putGraphDescription(gd);
			}
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();

	}

	@Override
	public Logger getLog() {
		return log;
	}

}
