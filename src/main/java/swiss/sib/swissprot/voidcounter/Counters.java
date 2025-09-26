package swiss.sib.swissprot.voidcounter;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public interface Counters {

	CompletableFuture<Exception> findAllGraphs(CommonVariables cv);
	
	default void countDistinctObjectsSubjectsInDefaultGraph(CommonVariables cv,
			boolean countDistinctSubjects, boolean countDistinctObjects) {
		if (countDistinctObjects) {
			countDistinctBnodeObjectsInDefaultGraph(cv);
			countDistinctLiteralObjectsForDefaultGraph(cv);
			if (!allInUnionGraph() && !countDistinctSubjects) {
				countDistinctIriObjectsForDefaultGraph(cv);
			}
		}
		if (countDistinctSubjects) {
			countDistinctBnodeSubjectsInDefaultGraph(cv);
			if (!allInUnionGraph() && !countDistinctObjects) {
				countDistinctIriSubjectsForDefaultGraph(cv);
			}
		}
		if (countDistinctSubjects && countDistinctObjects) {
			countDistinctIriSubjectsAndObjectsInDefaultGraph(cv);
		}
	}
	
	default void findPredicatesAndClasses(CommonVariables cv, Set<IRI> knownPredicates, GraphDescription gd, boolean findDistinctClasses, boolean findPredicates, boolean detailedCount, String classExclusion) {
		CommonGraphVariables gcv = cv.with(gd);
		if (findDistinctClasses && findPredicates && detailedCount) {
			findPredicatesAndClassesInAGraph(gcv, knownPredicates, classExclusion);
		} else {
			if (findPredicates) {
				findPredicatesInAGraph(gcv, knownPredicates);
			}
			if (findDistinctClasses) {
				findDistinctClasssesInAGraph(gcv, classExclusion);
			}
		}
	}
	
	default void countSpecifics(CommonVariables cv, GraphDescription gd, boolean countDistinctSubjects,
			boolean countDistinctObjects) {
		CommonGraphVariables gdcv = cv.with(gd);
		// Objects are hardest to count so schedules first.
		if (countDistinctObjects) {
			countDistinctLiteralObjectsInAGraph(gdcv);
			if (! countDistinctSubjects) {
				countDistinctIriObjectsInAGraph(gdcv);
				countDistinctBnodeObjectsInAGraph(gdcv);
			}
		}
		if (countDistinctSubjects) {
			countDistinctBnodeSubjectsInAgraph(gdcv);
			if (! countDistinctObjects) {
				countDistinctIriSubjectsInAGraph(gdcv);
			}
		}
		if (countDistinctObjects && countDistinctSubjects) {
			countDistinctIriSubjectsAndObjectsInAGraph(gdcv);
		}
		countTriplesInNamedGraph(gdcv);
	}

	void countDistinctBnodeSubjectsInAgraph(CommonGraphVariables cv);

	void countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv);

	void countDistinctIriObjectsForDefaultGraph(CommonVariables cv);

	void countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv);

	void countDistinctIriSubjectsForDefaultGraph(CommonVariables cv);

	void countDistinctIriSubjectsInAGraph(CommonGraphVariables cvgd);

	void countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv);

	void findPredicatesAndClassesInAGraph(CommonGraphVariables cv,
			Set<IRI> knownPredicates,
			String classExclusion);

	void findPredicatesInAGraph(CommonGraphVariables cv, Set<IRI> knownPredicates);

	void findDistinctClasssesInAGraph(CommonGraphVariables cv, String classExclusion);

	void countDistinctLiteralObjectsInAGraph(CommonGraphVariables cv);

	void countTriplesInNamedGraph(CommonGraphVariables cv);

	void countDistinctIriSubjectsAndObjectsInAGraph(CommonGraphVariables cv);
	
	void countDistinctIriSubjectsAndObjectsInDefaultGraph(CommonVariables cv);

	void isSourceClassLinkedToTargetClass(CommonGraphVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source);

	void isSourceClassLinkedToDistinctClassInGraphs(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, 
			String classExclusion);
	
	void isSourceClassLinkedToDistinctClassInOtherGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			String classExclusion);

	void findDataTypePartition(CommonGraphVariables cv, PredicatePartition predicatePartition,
			ClassPartition source);

	void findPredicateLinkSets(CommonGraphVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source,
			String classExclusion);

	void findNamedIndividualObjectSubjectForPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source);

	void countUniqueSubjectPerPredicateInGraph(CommonGraphVariables cv, PredicatePartition predicatePartition);

	void countUniqueObjectsPerPredicateInGraph(CommonGraphVariables cv, PredicatePartition predicatePartition);

	void countTriplesLinkingTwoTypesInDifferentGraphs(CommonGraphVariables cv, IRI sourceType, Set<ClassPartition> cp, 
			GraphDescription otherGraph, PredicatePartition pp);

	boolean allInUnionGraph();

	void countDistinctIriObjectsInAGraph(CommonGraphVariables cvgd);

	void countDistinctBnodeObjectsInAGraph(CommonGraphVariables gdcv);
	
	CompletableFuture<Exception> schedule(QueryCallable<?, ? extends Variables> toRun);
}