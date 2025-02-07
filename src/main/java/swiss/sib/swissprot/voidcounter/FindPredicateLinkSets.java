package swiss.sib.swissprot.voidcounter;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.Generate;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

public class FindPredicateLinkSets extends QueryCallable<Exception> {
	public static final Logger log = LoggerFactory.getLogger(FindPredicateLinkSets.class);
	private static final String PER_GRAPH_QUERY ="""
			SELECT (COUNT(?subject) AS ?count) 
			WHERE {GRAPH ?graph {
				?subject a ?sourceClass ; 
					?predicate ?target. 
				}
			}
			""";
	
	private static final String DEFAULT_GRAPH_QUERY ="""
			SELECT (COUNT(?subject) AS ?count) 
			WHERE {
				?subject a ?sourceClass ; 
					?predicate ?target. 
			}
			""";
	private final Set<ClassPartition> classes;
	private final PredicatePartition pp;
	private final ClassPartition source;
	private final Lock writeLock;
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;
	private final GraphDescription gd;

	private PredicatePartition subpredicatePartition;

	private final Consumer<ServiceDescription> saver;

	private final ServiceDescription sd;

	private final String classExclusion;

	public FindPredicateLinkSets(Repository repository, Set<ClassPartition> classes, PredicatePartition predicate,
			ClassPartition source, Lock writeLock, Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Semaphore limit,
			GraphDescription gd, AtomicInteger finishedQueries,
			Consumer<ServiceDescription> saver, ServiceDescription sd, String classExclusion) {
		super(repository, limit, finishedQueries);
		this.classes = classes;
		this.pp = predicate;
		this.source = source;
		this.writeLock = writeLock;
		this.schedule = schedule;
		this.gd = gd;
		this.saver = saver;
		this.sd = sd;
		this.classExclusion = classExclusion;
	}

	private void findDatatypeOrSubclassPartitions(final Repository repository, Set<ClassPartition> targetClasses,
			ClassPartition source, PredicatePartition subpredicatePartition, Lock writeLock) {
		if (subpredicatePartition.getDataTypePartitions().isEmpty()) {
			findSubClassParititions(targetClasses, subpredicatePartition, source, repository, writeLock);
		}
	}

	private void findSubClassParititions(Set<ClassPartition> targetClasses, PredicatePartition predicatePartition,
			ClassPartition source, Repository repository, Lock writeLock) {
		final IRI predicate = predicatePartition.getPredicate();
		schedule.apply(new FindNamedIndividualObjectSubjectForPredicateInGraph(gd, predicatePartition, source,
				repository, writeLock, limiter, finishedQueries));

		for (ClassPartition target : targetClasses) {

			schedule.apply(new IsSourceClassLinkedToTargetClass(repository, predicate, target,
					predicatePartition, source, gd, writeLock, limiter, finishedQueries));
		}

		for (GraphDescription og : sd.getGraphs()) {
			if (!og.getGraphName().equals(gd.getGraphName())) {
				schedule.apply(
						new IsSourceClassLinkedToDistinctClassInOtherGraph(repository, predicate, predicatePartition,
								source, gd, writeLock, limiter, finishedQueries, og, schedule, classExclusion));
			}
		}
		schedule.apply(new FindDataTypeIfNoClassOrDtKnown(predicatePartition, source, repository, gd,
				writeLock, limiter, finishedQueries));
	}

	private long countTriplesInPredicateClassPartition(final Repository repository,
			PredicatePartition predicatePartition, ClassPartition source) {

		try (RepositoryConnection localConnection = repository.getConnection()) {
			TupleQuery tq = localConnection.prepareTupleQuery(DEFAULT_GRAPH_QUERY);
			if (!Generate.isOnlyDefaultGraph(gd.getGraph()))
			{
				tq = localConnection.prepareTupleQuery(PER_GRAPH_QUERY);
				tq.setBinding("graph", gd.getGraph());
			}
			tq.setBinding("sourceClass", source.getClazz());
			tq.setBinding("predicate", predicatePartition.getPredicate());
			setQuery(PER_GRAPH_QUERY, tq.getBindings());
			try (TupleQueryResult triples = tq.evaluate()) {
				if (triples.hasNext()) {
					return ((Literal) triples.next().getBinding("count").getValue()).longValue();
				}
			}
			tq.clearBindings();
		} catch (MalformedQueryException | QueryEvaluationException e) {
			log.error("query failed", e);
		}
		return 0;
	}

	@Override
	protected void logStart() {
		log.debug(
				"Finding predicate linksets " + gd.getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());

	}

	@Override
	protected void logEnd() {
		log.debug("Found predicate linksets " + gd.getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());
	}

	@Override
	protected Exception run(RepositoryConnection connection) throws Exception {

		try {
			subpredicatePartition = new PredicatePartition(pp.getPredicate());
			long tripleCount = countTriplesInPredicateClassPartition(repository, pp, source);
			subpredicatePartition.setTripleCount(tripleCount);

		} catch (RepositoryException e) {
			log.error("Finding class and predicate link sets failed", e);
			return e;
		}
		return null;
	}

	@Override
	protected void set(Exception t) {
		if (subpredicatePartition.getTripleCount() > 0) {
			try {
				writeLock.lock();
				source.putPredicatePartition(subpredicatePartition);
			} finally {
				writeLock.unlock();
			}
			if (subpredicatePartition.getTripleCount() != 0) {
				findDatatypeOrSubclassPartitions(repository, classes, source, subpredicatePartition, writeLock);
			}
			saver.accept(sd);
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}