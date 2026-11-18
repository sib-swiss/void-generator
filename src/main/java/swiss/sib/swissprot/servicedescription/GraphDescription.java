package swiss.sib.swissprot.servicedescription;

import java.util.Set;
import java.util.TreeSet;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;


public class GraphDescription
{

	private IRI graph;
	private String graphName;
	private long tripleCount;
	private String ftpLocation;
	private final Set<ClassPartition> classes = new TreeSet<>();
	private final Set<PredicatePartition> predicates = new TreeSet<>();
	private long distinctLiteralObjectCount;
	private long distinctBnodeObjectCount;
	private long distinctIriObjectCount;
	private long distinctBnodeSubjectCount;
	private long distinctIriSubjectCount;

	private IRI license;
	private IRI creator;
	private String title;

	public long getDistinctClassesCount()
	{
		return classes.size();
	}

	public long getDistinctSubjectCount()
	{
		return distinctIriSubjectCount + distinctBnodeSubjectCount;
	}

	public long getDistinctObjectCount()
	{
		return distinctLiteralObjectCount + distinctIriObjectCount + distinctBnodeObjectCount;
	}

	public long getTripleCount()
	{
		return tripleCount;
	}

	public void setGraphName(String graphName)
	{
		this.graphName = graphName;
		this.graph = valueOf(graphName);
	}

	private IRI valueOf(String s)
	{
		// Splitting on '/' alone leaves a stray '#' inside the "local name" for any
		// graph IRI with a fragment (e.g. ".../test/trees#trees"), which is invisible
		// until that local name is itself embedded into another IRI's local name
		// (e.g. a cross-graph linkset's objectsTarget) -- that produces a URI with
		// two '#' characters, invalid per RFC 3986 and rejected by a strict parser.
		int position = Math.max(s.lastIndexOf('/'), s.lastIndexOf('#'));
		if (position == -1)
			throw new IllegalArgumentException("Invalid PURL: " + s);
		return SimpleValueFactory.getInstance().createIRI(s.substring(0, position + 1), s.substring(position + 1));
	}

	public String getGraphName()
	{
		return graphName;
	}


//	public void setDistinctObjectCount(long longValue)
//	{
//		this.distinctObjectCount = longValue;
//
//	}

	public void setTripleCount(long size)
	{
		this.tripleCount = size;

	}

	public void setDatadump(String ftpLocation)
	{
		this.ftpLocation = ftpLocation;
	}

	public String getDatadump()
	{
		return this.ftpLocation;
	}

	public Set<ClassPartition> getClasses()
	{
		return classes;
	}

	public Set<PredicatePartition> getPredicates()
	{
		return predicates;
	}

	public IRI getGraph()
	{
		return graph;
	}

	public void setGraph(IRI graph)
	{
		this.graph = graph;
		this.graphName = graph.stringValue();
	}

	public long getDistinctLiteralObjectCount()
	{
		return distinctLiteralObjectCount;
	}

	public void setDistinctLiteralObjectCount(long distinctLiteralObjectCount)
	{
		this.distinctLiteralObjectCount = distinctLiteralObjectCount;
	}

	public void setDistinctBnodeSubjectCount(long count)
	{
		this.distinctBnodeSubjectCount = count;

	}

	public long getDistinctBnodeSubjectCount()
	{
		return distinctBnodeSubjectCount;
	}

	public void setDistinctIriSubjectCount(long parseLong)
	{
		this.distinctIriSubjectCount = parseLong;
	}

	public long getDistinctIriSubjectCount()
	{
		return distinctIriSubjectCount;
	}

	public long getDistinctBnodeObjectCount()
	{
		return distinctBnodeObjectCount;
	}

	public void setDistinctBnodeObjectCount(long distinctBnodeObjectCount)
	{
		this.distinctBnodeObjectCount = distinctBnodeObjectCount;
	}

	public long getDistinctIriObjectCount()
	{
		return distinctIriObjectCount;
	}

	public void setDistinctIriObjectCount(long distinctIriObjectCount)
	{
		this.distinctIriObjectCount = distinctIriObjectCount;
	}

	public IRI getLicense()
	{
		return license;
	}

	public void setLicense(IRI license)
	{
		this.license = license;
	}

	public void setCreator(IRI creator) {
		this.creator = creator;
	}

	public IRI getCreator() {
		return creator;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return title;
	}
}
