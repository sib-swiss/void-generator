package swiss.sib.swissprot.servicedescription;

import java.time.LocalDate;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.algebra.evaluation.function.hash.MD5;
import org.eclipse.rdf4j.rio.RDFHandler;

import swiss.sib.swissprot.vocabulary.FORMATS;
import swiss.sib.swissprot.vocabulary.PAV;
import swiss.sib.swissprot.vocabulary.VOID_EXT;

public class ServiceDescriptionStatementGenerator {
	private final RDFHandler handler;
	private final ValueFactory vf;

	public ServiceDescriptionStatementGenerator(RDFHandler handler) {
		this.handler = handler;
		this.vf = SimpleValueFactory.getInstance();
	}

	public void generateStatements(IRI endpoint, IRI iriOfVoid, ServiceDescription item) {

		Resource defaultDatasetId = vf.createIRI(endpoint.getNamespace(),
				endpoint.getLocalName() + "#sparql-default-dataset");
		Resource defaultGraphId = vf.createIRI(iriOfVoid.getNamespace(),
				iriOfVoid.getLocalName() + "#sparql-default-graph");
		statement(endpoint, RDF.TYPE, SD.SERVICE);
		statement(endpoint, SD.DEFAULT_DATASET, defaultDatasetId);
		statement(endpoint, SD.ENDPOINT, endpoint);
		supportedFormats(endpoint);
		statement(endpoint, SD.SUPPORTED_LANGUAGE, SD.SPARQL_11_QUERY);
		statement(endpoint, SD.FEATURE_PROPERTY, SD.UNION_DEFAULT_GRAPH);
		statement(endpoint, SD.FEATURE_PROPERTY, SD.BASIC_FEDERATED_QUERY);
		if (item.getTitle() != null) {
			statement(endpoint, DCTERMS.TITLE, vf.createLiteral(item.getTitle()));	
		}
		LocalDate calendar = describeDefaultDataset(item, defaultDatasetId, defaultGraphId);

		for (GraphDescription gd : item.getGraphs()) {
			statement(defaultDatasetId, SD.NAMED_GRAPH_PROPERTY, getIRI(gd.getGraphName()));
		}

		describeDefaultGraph(item, defaultGraphId, calendar);
		Resource graphCollectionId = vf.createIRI(iriOfVoid.getNamespace(),
				iriOfVoid.getLocalName() + "#sparql-graph-collection");
		
		statement(endpoint, SD.AVAILBLE_GRAPHS, graphCollectionId);

		for (GraphDescription gd : item.getGraphs()) {
			statement(graphCollectionId, SD.NAMED_GRAPH_PROPERTY, getIRI(gd.getGraphName()));
		}

		for (GraphDescription gd : item.getGraphs())
			statementsAboutGraph(defaultDatasetId, gd, item, iriOfVoid);
	}

	protected LocalDate describeDefaultDataset(ServiceDescription item, Resource defaultDatasetId,
			Resource defaultGraphId) {
		statement(defaultDatasetId, RDF.TYPE, SD.DATASET);
		statement(defaultDatasetId, SD.DEFAULT_GRAPH, defaultGraphId);
		LocalDate calendar = item.getReleaseDate();
		if (item.getVersion() != null)
			statement(defaultDatasetId, PAV.VERSION, vf.createLiteral(item.getVersion()));
		if (calendar != null) {
			statement(defaultDatasetId, DCTERMS.ISSUED, vf.createLiteral(calendar));
		}
		if (item.getTitle() != null)
			statement(defaultGraphId, DCTERMS.TITLE, vf.createLiteral(item.getTitle()));
		return calendar;
	}

	protected void describeDefaultGraph(ServiceDescription item, Resource defaultGraphId, LocalDate calendar) {
		statement(defaultGraphId, RDF.TYPE, SD.GRAPH_CLASS);
		if (item.getTotalTripleCount() > 0) {
			statement(defaultGraphId, VOID.TRIPLES, vf.createLiteral(item.getTotalTripleCount()));
		}

		if (calendar != null) {
			statement(defaultGraphId, DCTERMS.ISSUED, vf.createLiteral(calendar));
		}

		long distinctObjects = item.getDistinctObjectCount();
		if (distinctObjects > 0)
			statement(defaultGraphId, VOID.DISTINCT_OBJECTS, vf.createLiteral(distinctObjects));

		if (item.getDistinctLiteralObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_LITERALS,
					vf.createLiteral(item.getDistinctLiteralObjectCount()));

		if (item.getDistinctIriObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS,
					vf.createLiteral(item.getDistinctIriObjectCount()));

		if (item.getDistinctBnodeObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS,
					vf.createLiteral(item.getDistinctBnodeObjectCount()));

		long distinctSubjects = item.getDistinctSubjectCount();
		if (distinctSubjects > 0)
			statement(defaultGraphId, VOID.DISTINCT_SUBJECTS, vf.createLiteral(distinctSubjects));
		if (item.getDistinctIriSubjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS,
					vf.createLiteral(item.getDistinctIriSubjectCount()));
		if (item.getDistinctBnodeSubjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS,
					vf.createLiteral(item.getDistinctBnodeSubjectCount()));
	}

	protected void statementsAboutGraph(Resource defaultDatasetId, GraphDescription gd, ServiceDescription sd,
			IRI iriOfVoid) {
		final String rawGraphName = gd.getGraphName();
		IRI graphName = getIRI(rawGraphName);
		String voidLocation = iriOfVoid.stringValue();

		IRI namedGraph = graphName;
		statement(defaultDatasetId, SD.NAMED_GRAPH_PROPERTY, namedGraph);
		statement(namedGraph, RDF.TYPE, SD.NAMED_GRAPH_CLASS);
		IRI graph = getResourceForGraph(graphName, voidLocation);
		statement(namedGraph, SD.NAME, graphName);
		statement(namedGraph, SD.GRAPH_PROPERTY, graph);
		if (gd.getTitle() != null)
			statement(namedGraph, DCTERMS.TITLE, vf.createLiteral(gd.getTitle()));

		if (gd.getLicense() != null)
			statement(namedGraph, DCTERMS.LICENSE, gd.getLicense());

		statement(graph, RDF.TYPE, SD.GRAPH_CLASS);
		statement(graph, VOID.TRIPLES, vf.createLiteral(gd.getTripleCount()));
		long distinctClasses = gd.getDistinctClassesCount();
		if (distinctClasses > 0)
			statement(graph, VOID.CLASSES, vf.createLiteral(distinctClasses));

		for (ClassPartition cp : gd.getClasses()) {
			final IRI iriOfType = getIRI(cp.getClazz().toString());
			IRI dataSetClassPartition = getResourceForPartition(namedGraph, iriOfType, voidLocation);
			statement(graph, VOID.CLASS_PARTITION, dataSetClassPartition);
		}

		for (PredicatePartition predicate : gd.getPredicates()) {
			IRI dataSetPropertyPartition = getResourceForPartition(namedGraph, getIRI(predicate.getPredicate()),
					voidLocation);
			statement(graph, VOID.PROPERTY_PARTITION, dataSetPropertyPartition);
		}

		long distinctObjects = gd.getDistinctObjectCount();
		if (distinctObjects > 0)
			statement(graph, VOID.DISTINCT_OBJECTS, vf.createLiteral(distinctObjects));

		if (gd.getDistinctLiteralObjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_LITERALS, vf.createLiteral(gd.getDistinctLiteralObjectCount()));

		if (gd.getDistinctIriObjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS, vf.createLiteral(gd.getDistinctIriObjectCount()));

		if (gd.getDistinctBnodeObjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS, vf.createLiteral(gd.getDistinctBnodeObjectCount()));

		long distinctSubjects = gd.getDistinctSubjectCount();
		if (distinctSubjects > 0)
			statement(graph, VOID.DISTINCT_SUBJECTS, vf.createLiteral(distinctSubjects));
		if (gd.getDistinctIriSubjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS,
					vf.createLiteral(gd.getDistinctIriSubjectCount()));
		if (gd.getDistinctBnodeSubjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS,
					vf.createLiteral(gd.getDistinctBnodeSubjectCount()));

		describeClassPartitions(gd, voidLocation, namedGraph, graph);

		describePredicatePartitions(gd, voidLocation, namedGraph, graph);
	}

	public IRI getResourceForGraph(IRI graphName, String voidLocation) {
		return vf.createIRI(voidLocation, "#_graph_" + graphName.getLocalName() + "!" + hash(graphName.stringValue()));
	}

	private String hash(String graphName) {
		return new MD5().evaluate(vf, vf.createLiteral(graphName)).stringValue().substring(0, 8);
	}
	
	private String hash(Value... iris) {
		StringBuilder sb = new StringBuilder();
		for (Value iri : iris) {
			sb.append(iri.stringValue());
			sb.append('_');
		}
		return hash(sb.toString());
	}

	protected void describePredicatePartitions(GraphDescription gd, String voidLocation, IRI namedGraph, IRI graph) {
		for (PredicatePartition predicate : gd.getPredicates()) {
			IRI dataSetPropertyPartition = getResourceForPartition(namedGraph, getIRI(predicate.getPredicate()),
					voidLocation);
			statement(dataSetPropertyPartition, VOID.PROPERTY, predicate.getPredicate());
			statement(dataSetPropertyPartition, RDF.TYPE, VOID.DATASET);
			for (ClassPartition ppcp : predicate.getClassPartitions()) {
				Resource cppr = getResourceForSubPartition(namedGraph, predicate.getPredicate(), ppcp.getClazz(),
						voidLocation);
				statement(dataSetPropertyPartition, VOID.CLASS_PARTITION, cppr);
				statement(cppr, VOID.CLASS, ppcp.getClazz());
				if (ppcp.getTripleCount() > 0) {
					statement(cppr, VOID.ENTITIES, vf.createLiteral(ppcp.getTripleCount()));
				}
				Resource bNode = vf.createBNode();
				statement(bNode, RDF.TYPE, VOID.LINKSET);
				statement(bNode, VOID.LINK_PREDICATE, predicate.getPredicate());
				statement(bNode, VOID.OBJECTS_TARGET, cppr);
				statement(bNode, VOID.SUBJECTS_TARGET, dataSetPropertyPartition);
				if (ppcp.getTripleCount() > 0) {
					statement(bNode, VOID.ENTITIES, vf.createLiteral(ppcp.getTripleCount()));
				}

			}
			for (LinkSetToOtherGraph ls : predicate.getLinkSets()) {
				generateLinkset(voidLocation, dataSetPropertyPartition, ls, graph);
			}
			generateDatatypePartitions(namedGraph, predicate, dataSetPropertyPartition, voidLocation);
			generateObjectPartitions(namedGraph, predicate, dataSetPropertyPartition, voidLocation);
			if (predicate.getTripleCount() > 0L)
				statement(dataSetPropertyPartition, VOID.TRIPLES, vf.createLiteral(predicate.getTripleCount()));
			if (predicate.getDistinctSubjectCount() > 0L)
				statement(dataSetPropertyPartition, VOID.DISTINCT_SUBJECTS,
						vf.createLiteral(predicate.getDistinctSubjectCount()));
			if (predicate.getDistinctObjectCount() > 0L)
				statement(dataSetPropertyPartition, VOID.DISTINCT_OBJECTS,
						vf.createLiteral(predicate.getDistinctObjectCount()));
		}
	}

	protected void describeClassPartitions(GraphDescription gd, String voidLocation, IRI namedGraph, IRI graph) {
		for (ClassPartition cp : gd.getClasses()) {
			final IRI iriOfType = getIRI(cp.getClazz().toString());
			IRI classClassPartition = getResourceForPartition(namedGraph, iriOfType, voidLocation);
			statement(classClassPartition, RDF.TYPE, VOID.DATASET);
			statement(classClassPartition, VOID.CLASS, iriOfType);
			if (cp.getTripleCount() > 0) {
				statement(classClassPartition, VOID.ENTITIES, vf.createLiteral(cp.getTripleCount()));
			}
			for (PredicatePartition pp : cp.getPredicatePartitions()) {
				IRI ppr = getResourceForSubPartition(namedGraph, cp.getClazz(), pp.getPredicate(), voidLocation);
				statement(classClassPartition, VOID.PROPERTY_PARTITION, ppr);
				statement(ppr, RDF.TYPE, VOID.DATASET);
				statement(ppr, VOID.PROPERTY, pp.getPredicate());
				generateClassPartitions(namedGraph, cp, pp, ppr, voidLocation);
				generateDatatypePartitions(namedGraph, pp, ppr, voidLocation);
				generateObjectPartitions(namedGraph, pp, ppr, voidLocation);
				if (pp.getTripleCount() > 0L)
					statement(ppr, VOID.TRIPLES, vf.createLiteral(pp.getTripleCount()));
				if (pp.getDistinctSubjectCount() > 0L)
					statement(ppr, VOID.DISTINCT_SUBJECTS, vf.createLiteral(pp.getDistinctSubjectCount()));
				if (pp.getDistinctObjectCount() > 0L)
					statement(ppr, VOID.DISTINCT_OBJECTS, vf.createLiteral(pp.getDistinctObjectCount()));
				for (LinkSetToOtherGraph ls : pp.getLinkSets()) {

					generateLinkset(voidLocation, classClassPartition, ls, graph);
				}
				generateClassPartitionsAsLinkset(namedGraph, cp, pp, voidLocation, classClassPartition, gd, graph);
			}
		}
	}

	private void generateClassPartitionsAsLinkset(IRI namedGraph, ClassPartition cp, PredicatePartition pp,
			String voidLocation, IRI iriOfType, GraphDescription gp, IRI graph) {
		for (ClassPartition cpp : pp.getClassPartitions()) {
			String hash = hash(pp.getPredicate(),  cp.getClazz(), cpp.getClazz(), gp.getGraph(), gp.getGraph());
			Resource bNode = vf.createIRI(voidLocation,
					"#linkset_" + hash + pp.getPredicate().getLocalName());
			statement(bNode, RDF.TYPE, VOID.LINKSET);
			statement(bNode, VOID.LINK_PREDICATE, pp.getPredicate());
			statement(bNode, VOID.SUBJECTS_TARGET, iriOfType);
			statement(bNode, VOID.OBJECTS_TARGET, getResourceForPartition(namedGraph, cpp.getClazz(), voidLocation));
			statement(graph, VOID.SUBSET, bNode);
		}
	}

	public void generateLinkset(String voidLocation, IRI dataSetClassPartition, LinkSetToOtherGraph ls, IRI graph) {
		var pp = ls.getPredicatePartition();
		var hash = hash(pp.getPredicate(), ls.getSourceType(), ls.getTargetType(), ls.getOtherGraph().getGraph(),
				graph);

		Resource bNode = vf.createIRI(voidLocation, "#linkset_"
				+ hash
				+ pp.getPredicate().getLocalName());
		statement(bNode, RDF.TYPE, VOID.LINKSET);
		statement(bNode, VOID.LINK_PREDICATE, pp.getPredicate());
		statement(bNode, VOID.SUBJECTS_TARGET, dataSetClassPartition);
		if (ls.getTripleCount() > 0) {
			statement(bNode, VOID.TRIPLES, vf.createLiteral(ls.getTripleCount()));
		}
		statement(bNode, VOID.OBJECTS_TARGET,
				getResourceForPartition(ls.getOtherGraph().getGraph(), ls.getTargetType(), voidLocation));
		if (ls.getLinkingGraph() != null) {
			statement(graph, VOID.SUBSET, bNode);
		}
	}

	private void generateClassPartitions(IRI namedGraph, ClassPartition cp, PredicatePartition pp, IRI ppr,
			String voidLocation) {
		for (ClassPartition ppcp : pp.getClassPartitions()) {
			IRI cppr = getResourceForSubPartition(namedGraph, cp.getClazz(), pp.getPredicate(), ppcp.getClazz(),
					voidLocation);
			statement(ppr, VOID.CLASS_PARTITION, cppr);
			statement(cppr, RDF.TYPE, VOID.DATASET);
			statement(cppr, VOID.CLASS, ppcp.getClazz());
			if (ppcp.getTripleCount() > 0) {
				statement(cppr, VOID.TRIPLES, vf.createLiteral(ppcp.getTripleCount()));
			}
		}
	}

	private void generateDatatypePartitions(IRI namedGraph, PredicatePartition pp, IRI ppr, String voidLocation) {
		for (DataTypePartition dtpr : pp.getDataTypePartitions()) {
			final IRI datatype = dtpr.getDatatype();
			IRI cppr = getResourceForSubPartition(namedGraph, pp.getPredicate(), datatype, voidLocation);
			statement(ppr, VOID_EXT.DATATYPE_PARTITION, cppr);
			statement(cppr, RDF.TYPE, VOID.DATASET);
			statement(cppr, VOID_EXT.DATATYPE, datatype);
			if (dtpr.getTripleCount() > 0) {
				statement(cppr, VOID.TRIPLES, vf.createLiteral(dtpr.getTripleCount()));
			}
		}
	}

	private void generateObjectPartitions(IRI namedGraph, PredicatePartition pp, IRI ppr, String voidLocation) {
		for (ObjectPartition spr : pp.getSubjectPartitions()) {
			final IRI datatype = spr.getObject();
			IRI cppr = getResourceForSubPartition(namedGraph, pp.getPredicate(), datatype, voidLocation);
			statement(ppr, VOID_EXT.OBJECT_PARTITION, cppr);
			statement(cppr, VOID_EXT.OBJECT, datatype);
			if (spr.getTripleCount() > 0) {
				statement(cppr, VOID.TRIPLES, vf.createLiteral(spr.getTripleCount()));
			}
		}
	}

	private IRI getResourceForSubPartition(IRI namedGraph, IRI clazz, IRI predicate, String voidLocation) {
		return vf.createIRI(voidLocation,
				namedGraph.getLocalName() + '!'
						+ hash(namedGraph, clazz, predicate) + '!' + '-'
						+ clazz.getLocalName() + '-' + predicate.getLocalName());
	}

	private IRI getResourceForSubPartition(IRI namedGraph, IRI sourceClass, IRI predicate, IRI targetClass,
			String voidLocation) {
		return vf.createIRI(voidLocation,
				namedGraph.getLocalName() + '!'
						+ hash(namedGraph, sourceClass, predicate, targetClass)
						+ '!' + '-' + sourceClass.getLocalName() + '-' + predicate.getLocalName() + '-'
						+ targetClass.getLocalName());
	}

	protected IRI getResourceForPartition(final IRI namedGraph, final IRI rt, String voidLocation) {
		String md5 = hash(rt.stringValue());
		if (voidLocation.endsWith("#")) {
			return vf.createIRI(voidLocation, namedGraph.getLocalName() + '!' + md5 + '!' + rt.getLocalName());
		} else {
			return vf.createIRI(voidLocation + "#", namedGraph.getLocalName() + '!' + md5 + '!' + rt.getLocalName());
		}
	}

	protected IRI getIRI(final String rawGraphName) {
		if (rawGraphName.endsWith("/")) {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 2),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1, rawGraphName.length() - 1);
			return vf.createIRI(namespace, name);
		} else {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 1),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1);
			return vf.createIRI(namespace, name);
		}
	}

	protected IRI getIRI(final Resource graphName) {
		String rawGraphName = graphName.toString();
		if (rawGraphName.endsWith("/")) {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 2),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1, rawGraphName.length() - 1);
			return vf.createIRI(namespace, name);
		} else {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 1),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1);
			return vf.createIRI(namespace, name);
		}
	}

	protected void supportedFormats(Resource subject) {
		statement(subject, SD.RESULT_FORMAT, FORMATS.CSV);
		statement(subject, SD.RESULT_FORMAT, FORMATS.JSON);
		statement(subject, SD.RESULT_FORMAT, FORMATS.NTRIPLES);
		statement(subject, SD.RESULT_FORMAT, FORMATS.RDF_XML);
		statement(subject, SD.RESULT_FORMAT, FORMATS.TSV);
		statement(subject, SD.RESULT_FORMAT, FORMATS.TURTLE);
		statement(subject, SD.RESULT_FORMAT, FORMATS.XML);
	}

	private void statement(Resource s, IRI p, Value o) {
		handler.handleStatement(vf.createStatement(s, p, o));

	}
}
