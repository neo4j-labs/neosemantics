# neosemantics
This repository contains the following stored procedures

| Stored Proc Name        | params           | Description and example usage  |
|:------------- |:-------------|:-----|
| semantics.importRDF      | <ul><li>URL of the dataset</li><li>serialization(*)</li><li>shorten urls [true,false]</li><li>convert types to labels</li><li>periodic commit</li></ul> | Imports into Neo4j all the triples in the data set according to [the mapping defined here] (https://jesusbarrasa.wordpress.com/2016/06/07/importing-rdf-data-into-neo4j/) <br> **Examples:**<br>CALL semantics.importRDF("file:///.../myfile.ttl","Turtle", false, true, 9000)<br>CALL semantics.importRDF("http:///.../data.rdf","RDF/XML", true, true, 9000) |
| semantics.previewRDF      | <ul><li>URL of the dataset</li><li>serialization(*)</li><li>shorten urls [true,false]</li><li>convert types to labels</li></ul> | Parses some RDF and produces a preview in Neo4j browser. Same parameters as data import except for periodic commit, since there is no data written to the DB.<br> Notice that this is adequate for a preliminary visual analysis of a **SMALL dataset**. Think how many nodes you want rendered in your browser.<br> **Examples:**<br>CALL semantics.previewRDF("[https://.../clapton.n3](https://raw.githubusercontent.com/motools/musicontology/master/examples/clapton_perf/clapton.n3)","Turtle", true, true) |
| semantics.previewRDFSnippet      | <ul><li>An RDF snippet</li><li>serialization(*)</li><li>shorten urls [true,false]</li><li>convert types to labels</li></ul> | Identical to previewRDF but takes an RDF snippet instead of the url of the dataset.<br> Again, adequate for a preliminary visual analysis of a SMALL dataset. Think how many nodes you want rendered in your browser :)<br> **Examples:**<br>CALL semantics.previewRDFSnippet('[{"@id": "http://indiv#9132", "@type": ... }]', "JSON-LD", true, true) |
| semantics.LiteOntoImport      | <ul><li>URL of the dataset</li><li>serialization(*)</li></ul> | Imports the basic elements of an OWL or RDFS ontology, i.e. Classes, Properties, Domains, Ranges. Extended description [here](https://jesusbarrasa.wordpress.com/2016/04/06/building-a-semantic-graph-in-neo4j/) <br> **Example:**<br>CALL semantics.LiteOntoImport("http://.../myonto.trig","TriG")  |


(*) Valid formats: Turtle, N-Triples, JSON-LD, TriG, RDF/XML

