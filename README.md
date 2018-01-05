# neosemantics

## Installation
 
You can either download a prebuilt jar from the [releases area](https://github.com/jbarrasa/neosemantics/releases) or build it from the source. If you prefer to build, check the note below.

1. Copy the  the jar(s) in the <NEO_HOME>/plugins directory of your Neo4j instance.
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
This repository contains a set of stored procedures and extensions to both produce and consume RDF from Neo4j.

### Stored Procedures

| Stored Proc Name        | params           | Description and example usage  |
|:------------- |:-------------|:-----|
| semantics.importRDF      | <ul><li>URL of the dataset</li><li>serialization format(*)</li><li>map with zero or more params (see table below)</li></ul> | Imports into Neo4j all the triples in the data set according to [the mapping defined in this post] (https://jesusbarrasa.wordpress.com/2016/06/07/importing-rdf-data-into-neo4j/). <br> **Note** that before running the import procedure an index needs to be created on property uri of Resource nodes. Just run `CREATE INDEX ON :Resource(uri)` on your Neo4j DB. <br>**Examples:**<br>CALL semantics.importRDF("file:///.../myfile.ttl","Turtle", { shortenUrls: false, typesToLabels: true, commitSize: 9000 }) <br> CALL semantics.importRDF("http:///.../donnees.rdf","RDF/XML", { languageFilter: 'fr', commitSize: 5000 , nodeCacheSize: 250000}) |
| semantics.previewRDF      | <ul><li>URL of the dataset</li><li>serialization format(*)</li><li>map with zero or more params (see table below)</li></ul> | Parses some RDF and produces a preview in Neo4j browser. Same parameters as data import except for periodic commit, since there is no data written to the DB.<br> Notice that this is adequate for a preliminary visual analysis of a **SMALL dataset**. Think how many nodes you want rendered in your browser.<br> **Examples:**<br>CALL semantics.previewRDF("[https://.../clapton.n3](https://raw.githubusercontent.com/motools/musicontology/master/examples/clapton_perf/clapton.n3)","Turtle", {}) |
| semantics.previewRDFSnippet      | <ul><li>An RDF snippet</li><li>serialization format(*)</li><li>map with zero or more params (see table below)</li></ul> | Identical to previewRDF but takes an RDF snippet instead of the url of the dataset.<br> Again, adequate for a preliminary visual analysis of a SMALL dataset. Think how many nodes you want rendered in your browser :)<br> **Examples:**<br>CALL semantics.previewRDFSnippet('[{"@id": "http://indiv#9132", "@type": ... }]', "JSON-LD", { languageFilter: 'en'}) |
| semantics.liteOntoImport      | <ul><li>URL of the dataset</li><li>serialization(*)</li></ul> | Imports the basic elements of an OWL or RDFS ontology, i.e. Classes, Properties, Domains, Ranges. Extended description [here](https://jesusbarrasa.wordpress.com/2016/04/06/building-a-semantic-graph-in-neo4j/) <br> **Example:**<br>CALL semantics.liteOntoImport("http://.../myonto.trig","TriG")  |


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
If `shortenUrls : true`, you'll have prefixes used to shorten property and relationship names; and labels. You don't need to define your own namespaces prefixes as ones will be automatically generated for you with the format `ns0`, `ns1`, etc. If you're coming from the RDF and SPARQL world and you're used to defining your own prefixes, you can do so here too. You need to create (or merge, depending on whether it exists already) a `NamesapcePrefixDefinition` node before you perform the load of RDF data:

    // create the prefix mapping 
    CREATE (:NamespacePrefixDefinition {
      `http://www.example.com/ontology/1.0.0#`: 'ex',
      `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'rdfs'})
      
    // the actual load will use the NamespacePrefixDefinition if it exists already. 
    //Note 'shortenUrls' defaults to true but we're just being explicit
    CALL semantics.importRDF("file:///path/to/some-file.ttl", "Turtle", {shortenUrls: true})


### Extensions

| Extension        | params           | Description and example usage  |
|:------------- |:-------------|:-----|
| /rdf/describe/id      | <ul><li><b>nodeid:</b>the id of a node</li><li><b>excludeContext:</b>(optional) if present output will not include connected nodes, just selected one.</li></ul> | Produces an RDF serialization of the selected node. The format will be determined by the **accept** parameter in the header. Default is JSON-LD <br> **Example:**<br>:GET /rdf/describe/id?nodeid=0&excludeContext |
| /rdf/describe/uri      | <ul><li><b>nodeuri:</b>the uri of a node</li><li><b>excludeContext:</b>(optional) if present output will not include connected nodes, just selected one.</li></ul> | Produces an RDF serialization of the selected node. It works on a model either imported from an RDF dataset via **semantics.importRDF** or built in a way that nodes are labeled as :Resource and have an uri. This property is the one used by this extension to lookup a node.<br> **Example:**<br>:GET /rdf/describe/uri?nodeuri=http://dataset.com#id_1234  |
| /rdf/cypher      | Takes a cypher query in the payload | Produces an RDF serialization of the nodes and relationships returned by the query.<br> **Example:**<br>:POST /rdf/cypher "MATCH (a:Type1)-[r:REL]-(b) RETURN a, r, b"  |
| /rdf/cypheronrdf      | Takes a cypher query in the payload | Produces an RDF serialization of the nodes and relationships returned by the query. It works on a model either imported from an RDF dataset via **semantics.importRDF** or built in a way that nodes are labeled as :Resource and have an uri.<br> **Example:**<br>:POST /rdf/cypheronrdf "MATCH (a:Resource {uri:'http://dataset/indiv#153'})-[r]-(b) RETURN a, r, b"  |
