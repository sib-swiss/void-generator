package swiss.sib.swissprot.voidcounter.sparql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class IsSourceClassLinkedToDistinctClassInOtherGraph extends QueryCallable<List<LinkSetToOtherGraph>, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToDistinctClassInOtherGraph.class);
	private static final String CLAZZ = "clazz";
	private static final String COUNT = "count";


	private final String rawQuery;

	private final IRI predicate;
	private final PredicatePartition pp;
	private final ClassPartition source;
	private final String classExclusion;

	private final GraphDescription otherGraph;

	private final Counters counters;
	private final OptimizeFor optimizeFor;


	//TODO pass in a different waiter/limiter and logic to make sure that the other graph class list is known before we start here.
	//this can't use the current limiter logic as that would deadlock.
	public IsSourceClassLinkedToDistinctClassInOtherGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source,
			GraphDescription otherGraph,
			String classExclusion, Counters counters, OptimizeFor optimizeFor) {
		super(cv);
		this.counters = counters;
		this.optimizeFor = optimizeFor;
		this.predicate = predicatePartition.getPredicate();
		this.pp = predicatePartition;
		this.source = source;
		this.otherGraph = otherGraph;
		this.classExclusion = classExclusion;
		this.rawQuery = Helper.loadSparqlQuery("inter_graph_links_grouped_by_type", optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Checking if {} in {} connected to {} via {}", source.getClazz(), cv.gd().getGraphName(), otherGraph.getGraphName(), pp.getPredicate());
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if {} in {} connected to {} via {}", source.getClazz(), cv.gd().getGraphName(), otherGraph.getGraphName(), pp.getPredicate());
	}

	@Override
	protected List<LinkSetToOtherGraph> run(RepositoryConnection connection) throws Exception {
		final IRI sourceType = source.getClazz();

		if (otherGraph.getPredicates().isEmpty() || optimizeFor.preferGroupBy()) {
			return rediscoverPossibleLinkClasses(connection, sourceType);
		} else {
			counters.countTriplesLinkingTwoTypesInDifferentGraphs(cv, sourceType, otherGraph.getClasses(), otherGraph, pp);
			return List.of();    
		}
	}

	public List<LinkSetToOtherGraph> rediscoverPossibleLinkClasses(RepositoryConnection connection,
			final IRI sourceType) {
		String rq = makeQuery(rawQuery, otherGraph.getClasses(), classExclusion);
		final MapBindingSet pbq = new MapBindingSet();
		pbq.setBinding("sourceGraphName", cv.gd().getGraph());
		pbq.setBinding("predicate", predicate);
		pbq.setBinding("targetGraphName", otherGraph.getGraph());
		pbq.setBinding("sourceType", sourceType);
		setQuery(rq, pbq);
		List<LinkSetToOtherGraph> pps = new ArrayList<>();
		try (TupleQueryResult tqr = Helper.runTupleQuery(getQuery(), connection)) {
			while (tqr.hasNext()) {
				BindingSet next = tqr.next();
				long count = ((Literal) next.getBinding(COUNT).getValue()).longValue();
				Value clazz = next.getBinding(CLAZZ).getValue();
				if (count > 0 && clazz instanceof IRI targetType ) {
					LinkSetToOtherGraph subTarget = new LinkSetToOtherGraph(pp, targetType, sourceType,
							otherGraph, cv.gd().getGraph());
					subTarget.setTripleCount(count);
					pps.add(subTarget);
				}
			}
		}
		return pps;
	}

	static String makeQuery(String query, Set<ClassPartition> classes , String classExclusion) {
		String withKnownClasses = addKnownClasses(query, classes);
		//If we already know the other classes then we do not need to filter them out
		if (classExclusion == null || !classes.isEmpty()) {
			return withKnownClasses;
		} else {
			return insertBeforeEnd(withKnownClasses, "\tFILTER(" + classExclusion + ")\n");
		}
	}

	static String addKnownClasses(String query2, Set<ClassPartition> classes) {
		if (classes.isEmpty())
			return query2;
		else {
			String collect = classes.stream().map(ClassPartition::getClazz).map(c -> "(<" + c.stringValue() + ">)")
					.collect(Collectors.joining(" "));
			return insertAfterWhere(query2, "\tVALUES (?clazz) {" + collect + "}\n");
		}
	}

	static String insertBeforeEnd(String query, String collect) {
		return new StringBuilder(query).insert(query.lastIndexOf('}') - 1, collect).toString();
	}
	
	static String insertAfterWhere(String query, String collect) {
		return new StringBuilder(query).insert(query.indexOf('{') + 1, collect).toString();
	}

	@Override
	protected void set(List<LinkSetToOtherGraph> pps) {
		try {
			cv.writeLock().lock();
			for (LinkSetToOtherGraph lstog : pps) {
				pp.putLinkPartition(lstog);
			}
		} finally {
			cv.writeLock().unlock();
		}
	}
	
	@Override
	public Logger getLog() {
		return log;
	}
}
