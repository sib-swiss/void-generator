package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class CountTriplesLinkingTwoTypesInDifferentGraphs extends QueryCallable<Long, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(CountTriplesLinkingTwoTypesInDifferentGraphs.class);

	private static final String LSC = "lsc";
	private final String countTriplesLinking;

	private final LinkSetToOtherGraph ls;

	private final PredicatePartition predicatePartition;

	public CountTriplesLinkingTwoTypesInDifferentGraphs(CommonGraphVariables cv, LinkSetToOtherGraph ls,
			PredicatePartition predicatePartition, OptimizeFor optimizeFor) {
		super(cv);
		this.ls = ls;
		this.predicatePartition = predicatePartition;
		this.countTriplesLinking = Helper.loadSparqlQuery("inter_graph_links", optimizeFor);
	}

	public CountTriplesLinkingTwoTypesInDifferentGraphs(CommonGraphVariables cv, IRI sourceType, ClassPartition cp,
			GraphDescription otherGraph, PredicatePartition pp, OptimizeFor optimizeFor) {
		this(cv, new LinkSetToOtherGraph(pp, sourceType, cp.getClazz(), otherGraph, cv.gd().getGraph()), pp, optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct triples between for {}", ls);
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled())
			log.error("failed counting triples between classses " + ls, e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct triples: {} for {}", ls.getTripleCount(), ls);
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		IRI predicate = ls.getPredicatePartition().getPredicate();
		String otherGraphName = ls.getOtherGraph().getGraphName();
		IRI targetType = ls.getTargetType();
		IRI sourceType = ls.getSourceType();
		assert targetType != null;
		assert sourceType != null;
		assert otherGraphName != null;
		assert predicate != null;

		MapBindingSet bs = new MapBindingSet();

		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		bs.setBinding("graphName", vf.createIRI(cv.gd().getGraphName()));
		bs.setBinding("sourceType", sourceType);
		bs.setBinding("predicate", predicate);
		bs.setBinding("otherGraphName", vf.createIRI(otherGraphName));
		bs.setBinding("targetType", targetType);
		bs.setBinding("linkingGraphName", ls.getLinkingGraph());
		setQuery(countTriplesLinking, bs);

		try (TupleQueryResult tq = Helper.runTupleQuery(getQuery(), connection)){
			if (tq.hasNext()) {
				BindingSet next = tq.next();
				return ((Literal) next.getBinding(LSC).getValue()).longValue();
			} else {
				return 0L;
			}
		}
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			if (count > 0) {
				predicatePartition.putLinkPartition(ls);
				ls.setTripleCount(count);
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