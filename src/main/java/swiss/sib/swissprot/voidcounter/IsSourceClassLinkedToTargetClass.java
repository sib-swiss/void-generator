package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.Generate;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public final class IsSourceClassLinkedToTargetClass extends QueryCallable<Long> {

	private static final String COUNT_LINKS_IN_SAME_GRAPH = """
			SELECT 
				(COUNT (?subject) AS ?subjects) 
			WHERE { 
				GRAPH  ?graph { 
					?subject a ?sourceType ; 
						?predicate ?target . 
					?target a ?targetType .
				}
			}
			""";
	
	private static final String COUNT_LINKS_IN_DEFAULT_GRAPH = """
			SELECT 
				(COUNT (?subject) AS ?subjects) 
			WHERE { 
				?subject a ?sourceType ; 
					?predicate ?target . 
				?target a ?targetType .
			}
			""";

	private static final String SUBJECTS = "subjects";

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToTargetClass.class);

	private final IRI predicate;
	private final ClassPartition target;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;
	
	public IsSourceClassLinkedToTargetClass(Repository repository, IRI predicate, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription gd, Lock writeLock,
			Semaphore limiter, AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.predicate = predicate;
		this.target = target;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart() {
		log.debug("Checking if " + source.getClazz() + " connected to " + target.getClass() + " via " + predicate
				+ " in " + gd.getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if" + source.getClazz() + " connected to " + target.getClass() + " via " + predicate + " in "
				+ gd.getGraphName());
	}

	@Override
	protected Long run(RepositoryConnection connection) throws Exception {
		final IRI sourceType = source.getClazz();
		final IRI targetType = target.getClazz();
		TupleQuery tq = connection.prepareTupleQuery(COUNT_LINKS_IN_DEFAULT_GRAPH);
		setBindings(sourceType, targetType, tq);
		if (Generate.isOnlyDefaultGraph(gd.getGraph())) {
			tq = connection.prepareTupleQuery(COUNT_LINKS_IN_SAME_GRAPH);
			setBindings(sourceType, targetType, tq);
			tq.setBinding("graph", gd.getGraph());
			setQuery(COUNT_LINKS_IN_SAME_GRAPH, tq.getBindings());
		} else {
			setQuery(COUNT_LINKS_IN_DEFAULT_GRAPH, tq.getBindings());
		}
		return Helper.getSingleLongFromSparql(tq, connection, SUBJECTS);
	}

	public void setBindings(final IRI sourceType, final IRI targetType, TupleQuery tq) {
		tq.setBinding("sourceType", sourceType);
		tq.setBinding("targetType", targetType);
		tq.setBinding("predicate", predicate);
	}

	@Override
	protected void set(Long has) {
		if (has > 0) {
			try {
				writeLock.lock();
				final IRI targetType = target.getClazz();
				ClassPartition subTarget = new ClassPartition(targetType);
				subTarget.setTripleCount(has);
				predicatePartition.putClassPartition(subTarget);
			} finally {
				writeLock.unlock();
			}
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}