# A Detailed Statistics generator for a VoID file

This CLI tool will automatically generates [VoID description](https://www.w3.org/TR/void/) as RDF for a public endpoint given its URL.

## Build

Building uses java 17 and maven 3.6+

```sh
mvn package
```

Or you can use one of the pre build [releases](https://github.com/JervenBolleman/void-generator/releases).

### Integration tests against a real Virtuoso (optional, needs Docker)

`mvn test` only ever runs against an in-memory RDF4J store, even for the
`--optimize-for virtuoso` code path -- it has its own, separate JDBC/SQL
counting pipeline that is otherwise never exercised by the test suite. A
`virtuoso-it` Maven profile runs that pipeline against a real Virtuoso
started in a Docker container via [Testcontainers](https://testcontainers.com/):

```sh
mvn verify -Pvirtuoso-it
```

This needs a reachable Docker daemon; it is never activated by plain
`mvn test`/`package`/`install`, and the integration test's sources live
under `src/it/java` (not `src/test/java`) so the profile's dependencies
never have to be resolved by the default build either.

Use the `verify` lifecycle phase, not a direct `mvn -Pvirtuoso-it
failsafe:integration-test` goal invocation -- Maven only runs the exact
goals named on the command line that way, skipping the earlier
`generate-test-sources`/`test-compile` phases that put the integration
test's sources on the classpath in the first place, so nothing gets
compiled or run (silent "no tests to run" success).

## How to use

1. Always have permission from the endpoint hoster to run this program
2. Always run this locally and directly on the endpoint, without cache/proxy servers in between
3. If using virtuoso connecting via jdbc is much faster than connecting via http sparql protocol
4. Check the help option for exact options.

Runs on a java 17+ jdk, see the help for how to use
```sh
java -jar target/void-generator-*uber.jar --help
```


## Running against a local Virtuoso endpoint

```sh
java -jar target/void-generator-*uber.jar \
    --user dba \
    --password dba \
    --virtuoso-jdbc=jdbc:virtuoso://localhost:1111/charset=UTF-8 \ # note the localhost and "isql-t" port
    -r "https://YOUR_SPARQL_ENDPOINT/sparql" \
    -s void-file-locally-stored.ttl \
    -i "https://YOUR_SPARQL_ENDPOINT/.well-known/void"
```

The IRI is supposed to be the URL of the published location of the void file/service description.

### Running against a regular SPARQL endpoint

For any endpoint supporting the SPARQL protocol.

Example given here for the WikiPathways SPARQL endpoint:

```sh
java -jar target/void-generator-*-uber.jar -r https://sparql.wikipathways.org/sparql \
   -p https://sparql.wikipathways.org/sparql \
   --void-file void-wikipathway.ttl \
   --iri-of-void 'https://rdf.wikipathway.org/.well-known/void#' \
   -g http://rdf.wikipathways.org/
```

## What about all the options?

The command line options are there to turn off certain queries/void features that may not run in a reasonable time on specific endpoints. The default should be not to use any options.


## Structure of a void file

### General advice

When looking to generate shapes or code from a VoID file the main thing to look for are the `void:classPartition`s. For most shape and programming languages when generating code you would want one `shape` or `object oriented class` for each of them.

These `void:classPartitions`s will have `void:predicatePartition`s and `void:datatypePartition`s. The predicatePartions will lead to other resources (objects that are denoted by an IRI or BNode) while the datatypePartitions will lead to literal values.

The `void:predicatePartition`s are objects of a triple where a `void:subjectTarget` will be that triples predicate. The subject of that triple will also be a type `void:LinkSet` and the `void:objectTarget` will point to a different object of a `void:classPartition` triple.

## Logging for debugging

`-Dorg.slf4j.simpleLogger.defaultLogLevel=debug -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.log.org.apache=info`

## SPARQL queries to retrieve VoID descriptions

Once the VoID description turtle file has been generated you can upload it to your endpoint and retrieve its information with the SPARQL queries below.

Without subject/objects count:

```sparql
PREFIX up: <http://purl.uniprot.org/core/>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
SELECT DISTINCT ?subjectClass ?prop ?objectClass ?objectDatatype
WHERE {
  {
    ?cp void:class ?subjectClass ;
        void:propertyPartition ?pp .
    ?pp void:property ?prop .
    OPTIONAL {
        {
            ?pp  void:classPartition [ void:class ?objectClass ] .
        	
        } UNION {
            ?pp void-ext:datatypePartition [ void-ext:datatype ?objectDatatype ] .
        }
    }
  } UNION {
    ?linkset void:subjectsTarget [ void:class ?subjectClass ] ;
      void:linkPredicate ?prop ;
      void:objectsTarget [ void:class ?objectClass ] .
  }
}
```

With subject/objects count:

```sparql
PREFIX up: <http://purl.uniprot.org/core/>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX void-ext: <http://ldf.fi/void-ext#>
SELECT DISTINCT ?subjectsCount ?subjectClass ?prop ?objectClass ?objectsCount ?objectDatatype
WHERE {
  {
    ?cp void:class ?subjectClass ;
        void:entities ?subjectsCount ;
        void:propertyPartition ?pp .
    ?pp void:property ?prop .
    OPTIONAL {
        {
            ?pp  void:classPartition [ void:class ?objectClass ; void:triples ?objectsCount ] .
        	
        } UNION {
            ?pp void-ext:datatypePartition [ void-ext:datatype ?objectDatatype ] .
        }
    }
  } UNION {
    ?linkset void:subjectsTarget [ void:class ?subjectClass ; void:entities ?subjectsCount ] ;
      void:linkPredicate ?prop ;
      void:objectsTarget [ void:class ?objectClass ; void:entities ?objectsCount ] .
  }
}
```

###

Development local install without signing the jar, usefull if working on a machine where
the gpg toolchain is not configured for public deployment to maven central.

```
mvn install -Dgpg.skip
``
