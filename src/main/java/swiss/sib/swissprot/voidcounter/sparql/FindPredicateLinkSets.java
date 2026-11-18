package swiss.sib.swissprot.voidcounter.sparql;

import java.util.Set;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class FindPredicateLinkSets extends QueryCallable<PredicatePartition, CommonGraphVariables> {
	public static final Logger log = LoggerFactory.getLogger(FindPredicateLinkSets.class);
	private final String rawQuery;
	private final Set<ClassPartition> classes;
	private final PredicatePartition pp;
	private final ClassPartition source;
	
	private final String classExclusion;
	private final Counters counters;
	private final OptimizeFor optimizeFor;

	public FindPredicateLinkSets(CommonGraphVariables cv, Set<ClassPartition> classes, PredicatePartition predicate,
			ClassPartition source,
			String classExclusion, Counters counters, OptimizeFor optimizeFor) {
		super(cv);
		
		this.classes = classes;
		this.pp = predicate;
		this.source = source;
		this.classExclusion = classExclusion;
		this.counters = counters;
		this.optimizeFor = optimizeFor;
		this.rawQuery = Helper.loadSparqlQuery("count_subjects_with_a_type_and_predicate", optimizeFor);
	}

	private static void nextSteps(Set<ClassPartition> targetClasses, PredicatePartition predicatePartition,
			ClassPartition source, Counters counters, CommonGraphVariables cv, OptimizeFor optimizeFor, String classExclusion) {
		counters.findNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source);

		if (optimizeFor.preferGroupBy()) {
			counters
					.isSourceClassLinkedToDistinctClassInGraphs(cv, predicatePartition, source, 
							classExclusion);
		} else {
			for (ClassPartition target : targetClasses) {
				counters.isSourceClassLinkedToTargetClass(cv,target,
						predicatePartition, source);
			}
			for (GraphDescription og : cv.sd().getGraphs()) {
				if (!og.getGraphName().equals(cv.gd().getGraphName())) {
						counters.isSourceClassLinkedToDistinctClassInOtherGraph(cv,predicatePartition,
								source, og, classExclusion);
				}
			}
		}
		counters.findDataTypePartition(cv, predicatePartition, source);
	}

	/**
	 * Sets both the triple count and the distinct-subject count for
	 * (source class, predicate) on subpredicatePartition. The latter is what
	 * lets a downstream shape/schema generator tell a required property from
	 * an optional one: compared against the source class' void:entities
	 * count, distinctSubjects == entities proves every instance has a value
	 * (minCount 1); count == distinctSubjects proves no subject has more
	 * than one value (maxCount 1). Before this, void-generator only ever
	 * emitted the plain triple count here, which conflates "500 triples, one
	 * each on 500 instances" with "500 triples, five each on 100 instances
	 * and none on the rest" -- both impossible to tell apart downstream.
	 */
	private void countTriplesInPredicateClassPartition(final RepositoryConnection connection,
			PredicatePartition predicatePartition, ClassPartition source, PredicatePartition subpredicatePartition) {

		MapBindingSet bs = new MapBindingSet();
		bs.setBinding("graph", cv.gd().getGraph());
		bs.setBinding("sourceClass", source.getClazz());
		bs.setBinding("predicate", predicatePartition.getPredicate());
		setQuery(rawQuery, bs);

		try (TupleQueryResult triples = Helper.runTupleQuery(getQuery(), connection)) {
			if (triples.hasNext()) {
				var bindingSet = triples.next();
				subpredicatePartition
						.setTripleCount(((Literal) bindingSet.getBinding("count").getValue()).longValue());
				subpredicatePartition.setDistinctSubjectCount(
						((Literal) bindingSet.getBinding("distinctSubjects").getValue()).longValue());
			}
		} catch (MalformedQueryException | QueryEvaluationException e) {
			log.error("query failed", e);
		}
	}

	@Override
	protected void logStart() {
		log.debug(
				"Finding predicate linksets " + cv.gd().getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());

	}

	@Override
	protected void logEnd() {
		log.debug("Found predicate linksets " + cv.gd().getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());
	}

	@Override
	protected PredicatePartition run(RepositoryConnection connection) throws Exception {

		var subpredicatePartition = new PredicatePartition(pp.getPredicate());
		countTriplesInPredicateClassPartition(connection, pp, source, subpredicatePartition);

		return subpredicatePartition;
	}

	@Override
	protected void set(PredicatePartition subpredicatePartition) {
		if (subpredicatePartition.getTripleCount() > 0) {
			try {
				cv.writeLock().lock();
				source.putPredicatePartition(subpredicatePartition);
			} finally {
				cv.writeLock().unlock();
			}
			if (subpredicatePartition.getTripleCount() != 0) {
				nextSteps(classes, subpredicatePartition, source, counters, cv, optimizeFor, classExclusion);
			}
			cv.save();
		}
	}
	
	@Override
	public Logger getLog() {
		return log;
	}
}