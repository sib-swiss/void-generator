package swiss.sib.swissprot.voidcounter.virtuoso;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

final class CountDistinctIriObjectsInDefaultGraph
    extends QueryCallable<Long, CommonVariables>
{
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjectsInDefaultGraph.class);
	
	public CountDistinctIriObjectsInDefaultGraph(CommonVariables cv)
	{
		super(cv);
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct iri or bnode objects for default graph");
	}
	
	@Override
	protected void logFailed(Exception e)
	{
		if (log.isErrorEnabled())
			log.error("failed counting distinct iri or bnode objects for default graph", e);
	}

	@Override
	protected void logEnd()
	{
		log.debug("Counted distinct iri or bnode {} objects for default graph", getCount());
	}

	private long getCount() {
		return cv.sd().getDistinctIriObjectCount();
	}

	@Override
	protected Long run(RepositoryConnection connection)
	    throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		if (connection instanceof VirtuosoRepositoryConnection vrc)
		{
			//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			setQuery("SELECT (COUNT(DISTINCT(iri_id_num(RDF_QUAD.O)))) FROM RDF_QUAD WHERE isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) = 0");
			return VirtuosoFromSQL.getSingleLongFromSql(getQuery(), vrc);
		} else {
			throw new IllegalStateException("Connection is not VirtuosoRepositoryConnection");
		}
	}

	@Override
	protected void set(Long count)
	{
		try 
		{
			cv.writeLock().lock();
			cv.sd().setDistinctIriObjectCount(count);
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