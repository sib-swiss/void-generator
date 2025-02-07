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

public final class CountDistinctIriSubjectsInAGraphVirtuoso extends CountDistinctIriInAGraphVirtuoso {
//table option (index RDF_QUAD_GS)

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsInAGraphVirtuoso.class);

	

	public CountDistinctIriSubjectsInAGraphVirtuoso(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Map<IRI, Roaring64NavigableMap> graphIriIds,
			IRI graphIri, Semaphore limit, AtomicInteger finishedQueries) {
		super(repository, sd, saver, graphIri, writeLock, sd::setDistinctIriSubjectCount, GraphDescription::setDistinctIriSubjectCount,
				 graphIriIds,
				  limit, finishedQueries);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subjects for " + graphIri);
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri subjects " + graphIri, e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri subjects for graph " + graphIri);
	}

	@Override
	protected String queryForGraph() {
		return "select iri_id_num(RDF_QUAD.S) from RDF_QUAD table option (index RDF_QUAD_GS) where RDF_QUAD.G = iri_to_id('" + graphIri+ "') and isiri_id(RDF_QUAD.S) > 0 and is_bnode_iri_id(RDF_QUAD.S) = 0";
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}