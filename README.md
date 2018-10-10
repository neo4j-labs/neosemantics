# neosemantics

## Installation
 
You can either download a prebuilt jar from the [releases area](https://github.com/jbarrasa/neosemantics/releases) or build it from the source. If you prefer to build, check the note below.

1. Copy the  the jar(s) in the <NEO_HOME>/plugins directory of your Neo4j instance. (**note:** If you're going to use the JSON-LD serialisation format for RDF, you'll need to include also [APOC](https://neo4j-contrib.github.io/neo4j-apoc-procedures/))
2. Add the following line to your <NEO_HOME>/conf/neo4j.conf

  ```
  dbms.unmanaged_extension_classes=semantics.extension=/rdf
  ```
  
3. Restart the server. 
4. Check that the installation went well by running `call dbms.procedures()`. The list of procedures should include the ones documented below.
You can check that the extension is mounted by running `:GET /rdf/ping`



**Note on build**

When you run
  ```
  mvn clean package
  ```
This will produce two jars :
  1. A neosemantics-[...].jar This jar bundles all the dependencies.
  2. An original-neosemantics-[...].jar This jar is just the neosemantics bit. So go this way if you want to keep the third party jars separate. In this case you will have to add all third party dependencies (look at the pom.xml). 
  

## What's in this repository
This repository contains a set of stored procedures, user definded functions and extensions to integrate with RDF from Neo4j.

### Stored Procedures and UDFs for RDF Parsing/Previewing/Ingesting 

| Stored Proc Name        | params           | Description and example usage  |
|:------------- |:-------------|:-----|
| semantics.importRDF      | <ul><li>URL of the dataset</li><li>serialization format(*)</li><li>map with zero or more params (see table below)</li></ul> | Imports into Neo4j all the triples in the data set according to [the mapping defined in this post] (https://jesusbarrasa.wordpress.com/2016/06/07/importing-rdf-data-into-neo4j/). <br> **Note** that before running the import procedure an index needs to be created on property uri of Resource nodes. Just run `CREATE INDEX ON :Resource(uri)` on your Neo4j DB. <br>**Examples:**<br>CALL semantics.importRDF("file:///.../myfile.ttl","Turtle", { shortenUrls: false, typesToLabels: true, commitSize: 9000 }) <br> CALL semantics.importRDF("http:///.../donnees.rdf","RDF/XML", { languageFilter: 'fr', commitSize: 5000 , nodeCacheSize: 250000}) |
| semantics.previewRDF      | <ul><li>URL of the dataset</li><li>serialization format(*)</li><li>map with zero or more params (see table below)</li></ul> | Parses some RDF and produces a preview in Neo4j browser. Same parameters as data import except for periodic commit, since there is no data written to the DB.<br> Notice that this is adequate for a preliminary visual analysis of a **SMALL dataset**. Think how many nodes you want rendered in your browser.<br> **Examples:**<br>CALL semantics.previewRDF("[https://.../clapton.n3](https://raw.githubusercontent.com/motools/musicontology/master/examples/clapton_perf/clapton.n3)","Turtle", {}) |
| semantics.streamRDF      | <ul><li>URL of the dataset</li><li>serialization format(*)</li><li>map with zero or more params (see table below)</li></ul> | Parses some RDF and streams the triples as records of the form subject, predicate, object plus three additional fields: <ul><li>a boolean indicating whether the object of the statement is a literal: `isLiteral`</li><li>The datatype of the literal value if available `literalType`</li><li>The language if available `literalLang`</li></ul> This SP is useful when you want to import into your Neo4j graph fragments of an RDF dataset in a custom way.<br> **Examples:**<br>CALL semantics.streamRDF("[https://.../clapton.n3](https://raw.githubusercontent.com/motools/musicontology/master/examples/clapton_perf/clapton.n3)","Turtle", {}) |
| semantics.previewRDFSnippet      | <ul><li>An RDF snippet</li><li>serialization format(*)</li><li>map with zero or more params (see table below)</li></ul> | Identical to previewRDF but takes an RDF snippet instead of the url of the dataset.<br> Again, adequate for a preliminary visual analysis of a SMALL dataset. Think how many nodes you want rendered in your browser :)<br> **Examples:**<br>CALL semantics.previewRDFSnippet('[{"@id": "http://indiv#9132", "@type": ... }]', "JSON-LD", { languageFilter: 'en'}) |
| semantics.liteOntoImport      | <ul><li>URL of the dataset</li><li>serialization(*)</li></ul> | Imports the basic elements of an OWL or RDFS ontology, i.e. Classes, Properties, Domains, Ranges. Extended description [here](https://jesusbarrasa.wordpress.com/2016/04/06/building-a-semantic-graph-in-neo4j/) <br> **Example:**<br>CALL semantics.liteOntoImport("http://.../myonto.trig","TriG")  |
| semantics.getIRILocalName      | **[function]**<ul><li>IRI string</li></ul> | Returns the local part of the IRI (stripping out the namespace) <br> **Example:**<br>RETURN semantics.getIRILocalName('http://schema.org/Person')  |
| semantics.getIRINamespace      | **[function]**<ul><li>IRI string</li></ul> | Returns the namespace part of the IRI (stripping out the local part) <br> **Example:**<br>RETURN semantics.getIRINamespace('http://schema.org/Person')  |


(*) Valid formats: Turtle, N-Triples, JSON-LD, TriG, RDF/XML

| Param        | values(default)           | Description  |
|:------------- |:-------------|:-----|
| shortenUrls      | boolean (true) | when set to true, full urls are shortened using generated prefixes for both property names, relationship names and labels |
| typesToLabels      | boolean (true) | when set to true, rdf:type statements are imported as node labels in Neo4j |
| languageFilter      | ['en','fr','es',...] | when set, only literal properties with this language tag (or untagged ones) are imported  |
| headerParams      | map {} | parameters to be passed in the HTTP GET request. <br> Example: { authorization: 'Basic user:pwd', Accept: 'application/rdf+xml'} |
| commitSize      | integer (25000) | commit a partial transaction every n triples |
| nodeCacheSize      | integer (10000) | keep n nodes in cache to minimize reads from DB |


#### Note on namespace prefixes
If `shortenUrls : true`, you'll have prefixes used to shorten property and relationship names; and labels. You don't need to define your own namespaces prefixes as some of the most popular ones will be predefined for you (rdf, rdfs, owl, skos, sch, org) and for any other used in the imported dataset, the loader will automatically generate prefixes with the format `ns0`, `ns1`, etc. You can also define your own set of prefixes. For that you need to create (or merge, depending on whether it exists already) a `NamesapcePrefixDefinition` node before you perform the load of RDF data and the loader will use it:

    // create the prefix mapping 
    CREATE (:NamespacePrefixDefinition {
      `http://www.example.com/ontology/1.0.0#`: 'ex',
      `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'rdf'})
    
### Stored Procedures for Schema (Ontology) Mapping 

| Stored Proc Name        | params           | Description and example usage  |
|:------------- |:-------------|:-----|
| semantics.mapping.addSchema      | <ul><li>URL of the schema/vocabulary/ontology</li><li>prefix to be used in serialisations</li></ul> | Creates a reference to a vocabulary. Needed to define mappings. <br>**Examples:**<br>call semantics.mapping.addSchema("http://schema.org/","sch") |
| semantics.mapping.dropSchema      | <ul><li>URL of the schema/vocabulary/ontology</li>| Deletes a vocabulary reference and all associated mappings. <br>**Examples:**<br>call semantics.mapping.dropSchema("http://schema.org/") |
| semantics.mapping.listSchemas      | <ul><li>[optional] search string to list only schemas containing the search string in their uri or in the associated prefix</li></ul> | Returns all vocabulary references. <br>**Examples:**<br>call semantics.mapping.listSchemas() <br> call semantics.mapping.listSchemas('schema') <br> Combining list and drop to delet a set of schemas by name: <br> CALL semantics.mapping.listSchemas("fibo") YIELD node AS schemaDef WITH schemaDef, schemaDef._ns AS schname CALL semantics.mapping.dropSchema(schemaDef._ns) YIELD output RETURN schname, output |
| semantics.mapping.addCommonSchemas      | | Creates a references to a number of popular vocabularies including schema.org, Dublin Core, SKOS, OWL, etc. <br>**Examples:**<br>call semantics.mapping.addCommonSchemas() |
| semantics.mapping.addMappingToSchema      | <ul><li>The mapping reference node (can be retrieved by addSchema or listSchemas)</li><li>Neo4j DB schema element. It can be either a Label, property key or relationship type </li><li>Local name of the element in the selected schema (Class name, DataTypeProperty name or ObjectProperty name)</li></ul> | Creates a mapping for an element in the Neo4j DB schema to a vocabulary element. <br>**Examples:**<br> Getting a schema reference using listSchemas and creating a mapping for it: <br>call semantics.mapping.listSchemas("http://schema.org") yield node as sch <br> call semantics.mapping.addMappingToSchema(sch,"Movie","Movie") yield node as mapping return mapping |
| semantics.mapping.dropMapping      | <ul><li>mapped DB element name to remove the mapping for</li></ul> | Returns an output text message indicating success/failure of the deletion. <br>**Examples:**<br>call semantics.mapping.dropMapping("Person") |
| semantics.mapping.listMappings      | <ul><li>[optional]search string to list only mappings containing the search string in the DB element name</li></ul> | Returns a list with all the mappings. <br>**Examples:**<br>call semantics.mapping.listMappings() |




### Extensions

| Extension        | params           | Description and example usage  |
|:------------- |:-------------|:-----|
| /rdf/describe/id      | <ul><li><b>nodeid:</b>the id of a node</li><li><b>excludeContext:</b>(optional) if present output will not include connected nodes, just selected one.</li></ul> | Produces an RDF serialization of the selected node. The format will be determined by the **accept** parameter in the header. Default is JSON-LD <br> **Example:**<br>:GET /rdf/describe/id?nodeid=0&excludeContext |
| /rdf/describe/uri      | <ul><li><b>nodeuri:</b>the uri of a node</li><li><b>excludeContext:</b>(optional) if present output will not include connected nodes, just selected one.</li></ul> | Produces an RDF serialization of the selected node. It works on a model either imported from an RDF dataset via **semantics.importRDF** or built in a way that nodes are labeled as :Resource and have an uri. This property is the one used by this extension to lookup a node. [NOTE: URIs should be urlencoded. It's normally not a problem unless there are **hash signs in it** (escape them in the Neo4j browser with %23)] <br> **Example:**<br>:GET /rdf/describe/uri?nodeuri=http://dataset.com#id_1234  |
| /rdf/cypher      | JSON map with the following keys: <ul><li><b>cypher:</b>the cypher query to run</li><li><b>showOnlyMapped:</b>(optional, default is false) if present output will exclude unmapped elements (labels,attributes, relationships)</li></ul> | Produces an RDF serialization of the nodes and relationships returned by the query.<br> **Example:**<br>:POST /rdf/cypher { "cypher" : "MATCH (n:Person { name : 'Keanu Reeves'})-[r]-(m:Movie) RETURN n,r,m " , "showOnlyMapped" : true }  |
| /rdf/cypheronrdf      | JSON map with the following keys: <ul><li><b>cypher:</b>the cypher query to run</li></ul> | Produces an RDF serialization of the nodes and relationships returned by the query. It works on a model either imported from an RDF dataset via **semantics.importRDF** or built in a way that nodes are labeled as :Resource and have an uri.<br> **Example:**<br>:POST /rdf/cypheronrdf { "cypher":"MATCH (a:Resource {uri:'http://dataset/indiv#153'})-[r]-(b) RETURN a, r, b"}  |
