package swiss.sib.swissprot.voidcounter.virtuoso;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.Repository;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

public final class CountDistinctIriObjectsInAGraphVirtuoso extends CountDistinctIriInAGraphVirtuoso {

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjectsInAGraphVirtuoso.class);
	
	public CountDistinctIriObjectsInAGraphVirtuoso(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Map<IRI, Roaring64NavigableMap> graphIriIds,
			IRI graphIri, Semaphore limit, AtomicInteger finishedQueries) {
		super(repository, sd, saver, graphIri, writeLock, sd::setDistinctIriObjectCount, GraphDescription::setDistinctIriObjectCount,
		 graphIriIds,
		  limit, finishedQueries);

	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri objects for " + graphIri);
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri objects " + graphIri, e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct objects iri for graph " + graphIri);
	}

	@Override
	protected String queryForGraph() {
		query = "SELECT iri_id_num(RDF_QUAD.O) from RDF_QUAD table option (index RDF_QUAD_POGS) where isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) = 0 AND RDF_QUAD.G = iri_to_id('"
				+ graphIri + "')";
		return query;
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}