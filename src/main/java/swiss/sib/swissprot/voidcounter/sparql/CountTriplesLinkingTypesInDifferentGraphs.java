package swiss.sib.swissprot.voidcounter.sparql;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

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

public class CountTriplesLinkingTypesInDifferentGraphs extends QueryCallable<Set<LinkSetToOtherGraph>, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(CountTriplesLinkingTypesInDifferentGraphs.class);

	private static final String LSC = "lsc";
	private final String countTriplesLinking;

	private final PredicatePartition predicatePartition;

	private final IRI sourceType;

	private final Set<ClassPartition> cps;

	private final GraphDescription otherGraph;


	public CountTriplesLinkingTypesInDifferentGraphs(CommonGraphVariables cv, IRI sourceType, Set<ClassPartition> cps,
			GraphDescription otherGraph, PredicatePartition predicatePartition, OptimizeFor optimizeFor) {
		super(cv);
		this.predicatePartition = predicatePartition;
		this.sourceType = sourceType;
		this.cps = cps;
		this.otherGraph = otherGraph;
		this.countTriplesLinking = Helper.loadSparqlQuery("inter_graph_links_values", optimizeFor);
		
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct triples between {} in {} and other types in {}", sourceType, cv.gd().getGraphName(), otherGraph.getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled())
			log.error("failed counting triples between {} in {} and other types in {} due to {} ", sourceType, cv.gd().getGraphName(), otherGraph.getGraphName(), e.getMessage());
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct connections {} in {} and other types in {}", sourceType, cv.gd().getGraphName(), otherGraph.getGraphName());
	}

	@Override
	protected Set<LinkSetToOtherGraph> run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		IRI predicate = predicatePartition.getPredicate();
		String otherGraphName = otherGraph.getGraphName();
		assert sourceType != null;
		assert otherGraphName != null;
		assert predicate != null;

		MapBindingSet bs = new MapBindingSet();

		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		bs.setBinding("graphName", vf.createIRI(cv.gd().getGraphName()));
		bs.setBinding("sourceType", sourceType);
		bs.setBinding("predicate", predicate);
		bs.setBinding("otherGraphName", otherGraph.getGraph());
		bs.setBinding("linkingGraphName", otherGraph.getGraph());
		String query = countTriplesLinking + " VALUES (?targetType) { " + cps.stream().map(ClassPartition::getClazz)
				.map(IRI::toString).map(s -> "(<" + s + ">)").collect(Collectors.joining(" ")) + " }";
		setQuery(query, bs);
		Set<LinkSetToOtherGraph> results = new HashSet<>();
		try (TupleQueryResult tq = Helper.runTupleQuery(getQuery(), connection)){
			while (tq.hasNext()) {
				BindingSet next = tq.next();
				long count = ((Literal) next.getBinding(LSC).getValue()).longValue();
				IRI targetType = (IRI) next.getBinding("targetType").getValue();
				LinkSetToOtherGraph ls = new LinkSetToOtherGraph(predicatePartition, targetType, sourceType, otherGraph, cv.gd().getGraph());
				ls.setTripleCount(count);
				results.add(ls);
			}
		} catch (Exception e) {
			if (log.isErrorEnabled())
				log.error("Error evaluating query: {}", getQuery(), e);
			throw e;
		}
		return results;
	}

	@Override
	protected void set(Set<LinkSetToOtherGraph> count) {
		try {
			cv.writeLock().lock();
			for (LinkSetToOtherGraph ls : count) {
				predicatePartition.putLinkPartition(ls);
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
