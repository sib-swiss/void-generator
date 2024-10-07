# Tutorial: void-generator

In this example we use a trivial dataset with two graphs.

The first graph descibes, a few fruits. The second a few fruit trees.
You can see the raw data in src/test/resources/fruits.ttl and src/test/resources/trees.ttl
or merged into one file src/test/resources/tutorial.trig in the [trig](https://www.w3.org/TR/trig/) format.

 

```sparql
mvn clean package
java -jar target/void-generator-0.*-SNAPSHOT.jar \
    --from-test-file=src/test/resources/tutorial.trig \
    --void-file=test-out.ttl \
    --iri-of-void="https://example.org/test/.well-known/void" \
    --repository="https://example.org/sparql"
```

This command generates a void/service description for this test dataset.

# Describing the SPARQL service

The output file `test-out.ttl` will start describing itself as a sparql endpoint.
This is not that important for this tutorial but it gives the normal set of result types and
supportedLanguage. If your SPARQL endpoint is special you might want to add or remove some details 
here.

```turtle
<https://example.org/sparql> a :Service;
  :defaultDataset <https://example.org/sparql#sparql-default-dataset>;
  :endpoint <https://example.org/sparql>;
  :resultFormat formats:SPARQL_Results_CSV, formats:SPARQL_Results_JSON, formats:N-Triples,
    formats:RDF_XML, formats:SPARQL_Results_TSV, formats:Turtle, formats:SPARQL_Results_XML;
  :supportedLanguage :SPARQL11Query;
  :feature :UnionDefaultGraph, :BasicFederatedQuery .
```

# From Service Description to Datasets

The endpoint has a defaultDataset, in the majority of the cases this is an union of all other
graph contents. This is not required.

```turtle
<https://example.org/sparql>
  :defaultDataset <https://example.org/sparql#sparql-default-dataset> .
```

The default dataset a in this case it is an UNION graph has namedGraphs,
it shows the two different named graphs.

```turtle
<https://example.org/sparql#sparql-default-dataset> a :Dataset;
  :defaultGraph <https://example.org/sparql#sparql-default-graph>;
  :namedGraph <https://example.org/void-generator/test/fruits#fruits>, 
              <https://example.org/void-generator/test/trees#trees> .
```

From this point on you follow the specific `:namedGraph` links to the two named graphs that are 
in use in this project.
 1. `<https://example.org/void-generator/test/fruits#fruits>` 
 2. `<https://example.org/void-generator/test/trees#trees>`

We will look at the fruits graph first. Funily enough in the [Service Description spec](https://www.w3.org/TR/sparql11-service-description/) named graph have an independant description of the graph.
Finaly these descriptions are where we need to focus for most uses.

# Graph description

```turtle
<https://example.org/test/.well-known/void#_graph_fruits> a :Graph;
  void:entities "15"^^xsd:long;
  void:classes "3"^^xsd:long;

  ### For code generation start looking from here
  void:classPartition <https://example.org/test/.well-known/void#fruits!9ef2c005d9ebdc38dd416ee6c9777fa5!Apple>,
    <https://example.org/test/.well-known/void#fruits!60286502596782a072b293ff2c6891ab!Fruit>,
    <https://example.org/test/.well-known/void#fruits!0fecbe1bedf023c93ebca1f2c7cb7116!Pear>;

  ###
  void:propertyPartition <https://example.org/test/.well-known/void#fruits!c74e2b735dd8dc85ad0ee3510c33925f!type>,
    <https://example.org/test/.well-known/void#fruits!0df7bcd6e74cc08385b34a9b72ff69a1!color>,
    <https://example.org/test/.well-known/void#fruits!592e9955a2ce92d566d2fc4bdf89896b!growns_on>,
    <https://example.org/test/.well-known/void#fruits!9b56f03bd983e200da0e3a943d44d8a1!grows_on>;
  void:distinctObjects "3"^^xsd:long;
  void_ext:distinctLiterals "3"^^xsd:long .
```

In the Graph description there are links to the `void:classPartion`s for many usecases this is what you would like to look at.

The SPARQL query to get to this is:

```sparql
prefix sd: <http://www.w3.org/ns/sparql-service-description#>
prefix void: <http://rdfs.org/ns/void#>

SELECT ?graph ?classPartitionOfGraph  
WHERE {
  ?graph a sd:Graph ;
         void:classPartion ?classPartitionOfGraph .
}

```

# Class Partitions in a Graph.

## Simple literal properties.

```turtle
<https://example.org/test/.well-known/void#fruits!0df7bcd6e74cc08385b34a9b72ff69a1!color>
  void:property <https://example.org/void-generator/test/schema#color>;
  void:triples "4"^^xsd:long;
  void:distinctSubjects "4"^^xsd:long;
  void:distinctObjects "3"^^xsd:long .
```

