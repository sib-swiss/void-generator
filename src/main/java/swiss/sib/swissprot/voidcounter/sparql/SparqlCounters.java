package swiss.sib.swissprot.voidcounter.sparql;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.FindGraphs;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.Variables;

public class SparqlCounters implements Counters {
	private final OptimizeFor optimizeFor;
	private final Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule;
	private final Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> scheduleAgain;

	public SparqlCounters(OptimizeFor optimizeFor,
			Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule,
			Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> scheduleAgain) {
		this.optimizeFor = optimizeFor;
		this.schedule = schedule;
		this.scheduleAgain = scheduleAgain;
	}

	@Override
	public void countDistinctBnodeSubjectsInAgraph(CommonGraphVariables cv) {
		attempt(0, () -> new CountDistinctBnodeSubjectsInAGraph(cv, optimizeFor));
	}

	private <T> CompletableFuture<Exception> attempt(int attempts,
			Supplier<QueryCallable<T, ? extends Variables>> supplier) {
		QueryCallable<?, ? extends Variables> qc = supplier.get();
		CompletableFuture<Exception> apply;
		if (attempts == 0) {
			apply = schedule.apply(qc);
		} else {
			apply = scheduleAgain.apply(qc);
		}
		return apply.handleAsync((BiFunction<? super Exception, Throwable, Exception>) (e, t) -> {
			if (e instanceof QueryEvaluationException qe && attempts < 3 && qleverReasonToRetry(qe)) {
				qc.getLog().info("Qlever query failed: {}, scheduling to retry for the {} time", e.getMessage(),
						attempts + 1);
				return attempt(attempts + 1, supplier).join();
			} else {
				return e;
			}
		});

	}

	private boolean qleverReasonToRetry(QueryEvaluationException qe) {
		return OptimizeFor.QLEVER.equals(optimizeFor)
				&& (qe.getMessage().contains("allocate") || qe.getMessage().contains("insert"));
	}

	public CompletableFuture<Exception> findAllGraphs(CommonVariables cv) {
		return schedule(new FindGraphs(cv, optimizeFor));
	}

	public void countDistinctIriSubjectsAndObjectsInAGraph(CommonGraphVariables cv) {
		// If we are optimizing for Qlever, we count subjects and objects separately
		// this allows lazy evaluation of the results, meaning a lot less memory is
		// used.
		if (optimizeFor.equals(OptimizeFor.QLEVER)) {
			attempt(0, () -> new CountDistinctIriSubjectsInAGraph(cv, optimizeFor));
			attempt(0, () -> new CountDistinctIriObjectsInAGraph(cv, optimizeFor));
		} else {
			attempt(0, () -> new CountDistinctIriSubjectsAndObjectsInAGraph(cv, optimizeFor));
		}
	}

	public void countDistinctIriSubjectsAndObjectsInDefaultGraph(CommonVariables cv) {
		attempt(0, () -> new CountDistinctIriSubjectsAndObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv) {
		attempt(0, () -> new CountDistinctBnodeObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriObjectsForDefaultGraph(CommonVariables cv) {
		attempt(0, () -> new CountDistinctIriObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriObjectsInAGraph(CommonGraphVariables cv) {
		attempt(0, () -> new CountDistinctIriObjectsInAGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv) {
		attempt(0, () -> new CountDistinctLiteralObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriSubjectsForDefaultGraph(CommonVariables cv) {
		attempt(0, () -> new CountDistinctIriSubjectsForDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriSubjectsInAGraph(CommonGraphVariables cvgd) {
		attempt(0, () -> new CountDistinctIriSubjectsInAGraph(cvgd, optimizeFor));
	}

	@Override
	public void countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv) {
		attempt(0, () -> new CountDistinctBnodeSubjectsInDefaultGraph(cv, optimizeFor));
	}

	/**
	 * Schedule without retrying.
	 */
	@Override
	public void findPredicatesAndClassesInAGraph(CommonGraphVariables cvg, Set<IRI> knownPredicates, String classExclusion) {
		var findDistinctClasssesInAGraph = new FindDistinctClasssesInAGraph(cvg, classExclusion, optimizeFor, this);
		var findPredicatesAndCountObjects = new FindPredicatesAndCountObjects(cvg, knownPredicates, optimizeFor);
		var fdcg = schedule(findDistinctClasssesInAGraph);
		var fpco = schedule(findPredicatesAndCountObjects);
		var findClassPredicatePairs = new FindClassPredicatePairs(cvg, classExclusion, this, fdcg, fpco);
		schedule(findClassPredicatePairs);
	}

	@Override
	public void findPredicatesInAGraph(CommonGraphVariables cv, Set<IRI> knownPredicates) {
		attempt(0, () -> new FindPredicatesAndCountObjects(cv, knownPredicates, optimizeFor));
	}

	@Override
	public void findDistinctClasssesInAGraph(CommonGraphVariables cv, String classExclusion) {
		attempt(0, () -> new FindDistinctClasssesInAGraph(cv, classExclusion, optimizeFor, this));
	}

	@Override
	public void countDistinctLiteralObjectsInAGraph(CommonGraphVariables cv) {
		attempt(0, () -> new CountDistinctLiteralObjects(cv, optimizeFor));
	}

	@Override
	public void countTriplesInNamedGraph(CommonGraphVariables cv) {
		attempt(0, () -> new TripleCount(cv, optimizeFor));
	}

	@Override
	public void isSourceClassLinkedToTargetClass(CommonGraphVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source) {
		attempt(0, () -> new IsSourceClassLinkedToTargetClass(cv, target, predicatePartition, source, optimizeFor));
	}

	@Override
	public void isSourceClassLinkedToDistinctClassInGraphs(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, String classExclusion) {
		attempt(0, () -> new IsSourceClassLinkedToDistinctClassInOtherGraphs(cv, predicatePartition, source,
				classExclusion, optimizeFor));
	}

	@Override
	public void isSourceClassLinkedToDistinctClassInOtherGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og, String classExclusion) {
		attempt(0, () -> new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, predicatePartition, source, og,
				classExclusion, this, optimizeFor));
	}

	@Override
	public void findDataTypePartition(CommonGraphVariables cv, PredicatePartition predicatePartition,
			ClassPartition source) {
		attempt(0, () -> new FindDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source, optimizeFor));
	}

	@Override
	public void findPredicateLinkSets(CommonGraphVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source, String classExclusion) {
		attempt(0, () -> new FindPredicateLinkSets(cv, classes, predicate, source, classExclusion, this, optimizeFor));
	}

	@Override
	public void findNamedIndividualObjectSubjectForPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		attempt(0, () -> new FindNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source,
				optimizeFor));
	}

	@Override
	public void countUniqueSubjectPerPredicateInGraph(CommonGraphVariables cv, PredicatePartition predicatePartition) {
		attempt(0, () -> new CountUniqueSubjectPerPredicateInGraph(cv, predicatePartition, optimizeFor));
	}

	@Override
	public void countUniqueObjectsPerPredicateInGraph(CommonGraphVariables cv, PredicatePartition predicatePartition) {
		attempt(0, () -> new CountUniqueObjectsPerPredicateInGraph(cv, predicatePartition, optimizeFor));
	}

	@Override
	public void countTriplesLinkingTwoTypesInDifferentGraphs(CommonGraphVariables cv, IRI sourceType, Set<ClassPartition> cps, 
			GraphDescription otherGraph,
			PredicatePartition pp) {
		if ((optimizeFor.preferGroupBy() || OptimizeFor.QLEVER.equals(optimizeFor)) && cps.size() > 1)
			attempt(0, () -> new CountTriplesLinkingTypesInDifferentGraphs(cv, sourceType, cps, otherGraph, pp, optimizeFor));
		else
			for (ClassPartition cp : cps)
				attempt(0, () -> new CountTriplesLinkingTwoTypesInDifferentGraphs(cv, sourceType, cp, otherGraph, pp, optimizeFor));
	}

	@Override
	public boolean allInUnionGraph() {
		return optimizeFor.allInUnionGraph();
	}

	@Override
	public void countDistinctBnodeObjectsInAGraph(CommonGraphVariables gdcv) {
		attempt(0, () -> new CountDistinctBnodeObjectsInAGraph(gdcv, optimizeFor));
	}

	@Override
	public CompletableFuture<Exception> schedule(QueryCallable<?, ?> toRun) {
		return schedule.apply(toRun);
	}
}
