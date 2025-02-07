package swiss.sib.swissprot.servicedescription;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.rdf4j.model.IRI;


public class ServiceDescription
{
	private final Map<String, GraphDescription> gds = Collections.synchronizedMap(new TreeMap<>());
	private long totalTripleCount;
	private String version;
	private LocalDate releaseDate;
	private IRI endpoint;
	private String title;
	private long distinctBnodeObjectCount;
	private long distinctIriObjectCount;
	private long distinctLiteralObjectCount;
	private long distinctBnodeSubjectCount;
	private long distinctIriSubjectCount;

	public ServiceDescription()
	{

	}
	
	public void reset() {
		gds.clear();
		totalTripleCount = 0;
		releaseDate = null;
		title = null;
		distinctBnodeObjectCount = 0L;
		distinctIriObjectCount = 0L;
		distinctLiteralObjectCount = 0L;
		distinctBnodeSubjectCount = 0L;
		distinctIriSubjectCount = 0L;
	}

	public GraphDescription getGraph(IRI graphName)
	{
		return gds.get(graphName.stringValue());
	}
	
	public GraphDescription getGraph(String graphName)
	{
		return gds.get(graphName);
	}

	public Collection<GraphDescription> getGraphs()
	{
		return Collections.unmodifiableCollection(sortByGraphSize(gds.values()));
	}

	/**
	 * Makes hashes a bit more consistent and in the long run makes the main page a bit nicer to read.
	 * @param graphs the list to be sorted (not modified you get a sorted copy back).
	 * @return sorted graph description objects sorted by name.
	 */
	protected List<GraphDescription> sortByGraphSize(Collection<GraphDescription> graphs)
	{
		List<GraphDescription> sortedBySize = new ArrayList<>(graphs);
		Collections.sort(sortedBySize, (o1, o2) -> {
			if (o1 == null && o2 == null)
				return 0;
			else if (o1 == null)
				return 1;
			else if (o2 == null)
				return -1;
			else
				//We want the big graphs first.
				return Long.compare(o2.getTripleCount(), o1.getTripleCount());
		});
		return sortedBySize;
	}

	public void putGraphDescription(GraphDescription gd)
	{
		assert gd != null;
		gds.put(gd.getGraphName(), gd);
	}

	public long getTotalTripleCount()
	{
		return totalTripleCount;
	}

	public void setTotalTripleCount(long totalTripleCount)
	{
		this.totalTripleCount = totalTripleCount;
	}

	/**
	 * The version of the dataset hosted in the endpoint.
	 * @return version as a string
	 */
	public String getVersion()
	{
		return version;
	}

	/**
	 * Set the version of the dataset hosted in the endpoint
	 * 
	 * Not found by the void-generator, add in rdf afterwards manually.
	 * @param version
	 */
	public void setVersion(String version)
	{
		this.version = version;
	}

	/**
	 * The date the dataset was released.
	 * @return
	 */
	public LocalDate getReleaseDate()
	{
		return releaseDate;
	}

	/**
	 * Set the date the dataset was released.
	 * 
	 * Not found by the void-generator, add in rdf afterwards manually.
	 * @param releaseDate
	 */
	public void setReleaseDate(LocalDate releaseDate)
	{
		this.releaseDate = releaseDate;
	}


	public void deleteGraph(String graphName)
	{
		gds.remove(graphName);
	}

	public String getTitle()
	{
		return title;
	}

	public void setTitle(String title)
	{
		this.title = title;
	}

	public void setDistinctBnodeObjectCount(Long count)
	{
		this.distinctBnodeObjectCount = count;
	}

	public long getDistinctBnodeObjectCount()
	{
		return this.distinctBnodeObjectCount;
	}

	public void setDistinctIriObjectCount(long distinctIriObjectCount)
	{
		this.distinctIriObjectCount = distinctIriObjectCount;
	}

	public long getDistinctIriObjectCount()
	{
		return distinctIriObjectCount;
	}

	public void setDistinctLiteralObjectCount(Long count)
	{
		this.distinctLiteralObjectCount = count;
	}

	public long getDistinctLiteralObjectCount()
	{
		return this.distinctLiteralObjectCount;
	}

	public long getDistinctObjectCount()
	{
		return distinctIriObjectCount + distinctBnodeObjectCount + distinctLiteralObjectCount;
	}

	
	public long getDistinctSubjectCount()
	{
		return distinctIriSubjectCount + distinctBnodeSubjectCount;
	}
	
	public void setDistinctBnodeSubjectCount(long distinctBnodeSubjectCount)
	{
		this.distinctBnodeSubjectCount = distinctBnodeSubjectCount;
	}

	public long getDistinctBnodeSubjectCount()
	{
		return this.distinctBnodeSubjectCount;
	}

	public void setDistinctIriSubjectCount(long distinctIriSubjectCount)
	{
		this.distinctIriSubjectCount = distinctIriSubjectCount;
	}

	public long getDistinctIriSubjectCount()
	{
		return this.distinctIriSubjectCount;
	}

	public IRI getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(IRI endpoint) {
		this.endpoint = endpoint;
	}	
}
