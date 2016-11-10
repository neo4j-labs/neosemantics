# neosemantics
This repository contains the following stored procedures

| Stored Proc Name        | params           | Description and example usage  |
|:------------- |:-------------|:-----|
| semantics.importRDF      | <ul><li>URL of the dataset</li><li>serialization(*)</li><li>shorten urls [true,false]</li><li>periodic commit</li></ul> | Imports into Neo4j all the triples in the data set according to [the mapping defined here] (https://jesusbarrasa.wordpress.com/2016/06/07/importing-rdf-data-into-neo4j/) <br> **Examples:**<br>CALL semantics.importRDF("file:///.../myfile.ttl","Turtle", false, 500)<br>CALL semantics.importRDF("http:///.../data.rdf","RDF/XML", false, 500) |
| semantics.LiteOntoImport      | <ul><li>URL of the dataset</li><li>serialization(*)</li></ul> | Imports the basic elements of an OWL or RDFS ontology, i.e. Classes, Properties, Domains, Ranges. Extended description [here](https://jesusbarrasa.wordpress.com/2016/04/06/building-a-semantic-graph-in-neo4j/) <br> **Example:**<br>CALL semantics.LiteOntoImport("http://.../myonto.trig","TriG")  |


(*) Valid formats: Turtle, N-Triples, JSON-LD, TriG, RDF/XML

