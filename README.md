# neosemantics (n10s)
![n10s Logo](https://guides.neo4j.com/rdf/n10s.png) neosemantics is a plugin that enables the **use of RDF in Neo4j**. [RDF is a W3C standard model](https://www.w3.org/RDF/) for data interchange. Some key features of n10s are:

* **Store RDF data in Neo4j** in a
lossless manner (imported RDF can subsequently be exported without losing a single triple in the process).
* On-demand **export property graph data** from Neo4j *as RDF*.
* Model **validation** based on the **W3C SHACL language**
* Impoort of **Ontologies and Taxonomies** in **OWL/RDFS/SKOS/...**

Other features in NSMNTX include *model mapping* and *inferencing* on Neo4j graphs.


## ⇨ User Manual and Blog ⇦ 

⇨ Check out the complete **[user manual](https://neo4j.com/docs/labs/nsmntx/current/)** with examples of use. ⇦

⇨ [Blog on neosemantics](https://jbarrasa.com/category/graph-rdf/) (and more). ⇦

## Installation
 
You can either download a prebuilt jar from the [releases area](https://github.com/neo4j-labs/neosemantics/releases) or build it from the source. If you prefer to build, check the note below.

1. Copy the  the jar(s) in the <NEO_HOME>/plugins directory of your Neo4j instance. (**note:** If you're going to use the JSON-LD serialisation format for RDF, you'll need to include also [APOC](https://neo4j.com/labs/apoc/))
2. Add the following line to your <NEO_HOME>/conf/neo4j.conf

  ```
  dbms.unmanaged_extension_classes=n10s.endpoint=/rdf
  ```
  
3. Restart the server. 
4. Check that the installation went well by:
Running `call dbms.procedures()`. The list of procedures should include a number of them prefixed by **n10s**.
 
Checking that the logs show the following line on startup:
```
YYYY-MM-DD HH:MM:SS.000+0000 INFO  Mounted unmanaged extension [n10s.endpoint] at [/rdf]
```

You can also test the extension is mounted by running `:get http://localhost:7474/rdf/ping` on the neo4j browser and this should return the following message
```
{"ping":"here!"}
```


## Basic flow

####  0. Pre-req: Constraint Creation

``` 
CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE
```

#### 1.  Creating a Graph Configuration
Before any RDF import operation a `GraphConfig` needs to be created. Here we define the way the RDF data is persisted in Neo4j. 
We'll find things like 

| Param        | Values           | Desc  |
| :------------- |:-------------|:-----|
| handleVocabUris     | "SHORTEN","KEEP","SHORTEN_STRICT","MAP"|  how namespaces are  handled |
| handleMultival     | "OVERWRITE","ARRAY"      | how multivalued properties are handled |
| handleRDFTypes | "LABELS","NODES","LABELS_AND_NODES"      |  how RDF datatypes are handled |
| multivalPropList | [ list of predicate uris ] |  |
| ...| ...|...|

Most of them are the same (expect some changes) as in previous versions (see [3.5 manual](https://neo4j.com/docs/labs/nsmntx/current/reference/#_rdf_import_params) for reference). 

You can create a graph config with all the defaults like this:
``` 
call n10s.graphconfig.init()
``` 

Or customize it by passing a map with your options:
``` 
call n10s.graphconfig.init( { handleMultival: "ARRAY", 
                              multivalPropList: ["http://voc1.com#pred1", "http://voc1.com#pred2"],
                              keepLangTag: true })
``` 


#### 2.  Importing RDF data

Once the Graph config is created we can import data from a url using `fetch`:

``` 
call n10s.rdf.import.fetch( "https://raw.githubusercontent.com/jbarrasa/neosemantics/3.5/docs/rdf/nsmntx.ttl",
                            "Turtle")
``` 

Or pass it as a parameter using `inline`:

``` 
with '
@prefix neo4voc: <http://neo4j.org/vocab/sw#> .
@prefix neo4ind: <http://neo4j.org/ind#> .

neo4ind:nsmntx3502 neo4voc:name "NSMNTX" ;
			   a neo4voc:Neo4jPlugin ;
			   neo4voc:runsOn neo4ind:neo4j355 .

neo4ind:apoc3502 neo4voc:name "APOC" ;
			   a neo4voc:Neo4jPlugin ;		   
			   neo4voc:runsOn neo4ind:neo4j355 .

neo4ind:graphql3502 neo4voc:name "Neo4j-GraphQL" ;
			   a neo4voc:Neo4jPlugin ;			   
			   neo4voc:runsOn neo4ind:neo4j355 .			   			   

neo4ind:neo4j355 neo4voc:name "neo4j" ;
			   a neo4voc:GraphPlatform , neo4voc:AwesomePlatform .

' as  payload

call n10s.rdf.import.inline( payload, "Turtle") yield terminationStatus, triplesLoaded, triplesParsed, namespaces
return terminationStatus, triplesLoaded, triplesParsed, namespaces
``` 

It is possible to pass some request specific parameters like headerParams, commitSize, languageFilter...
(also found [in the 3.5 manual](https://neo4j.com/docs/labs/nsmntx/current/reference/#_rdf_import_params))


#### 3.  Importing Ontologies, QuadRDF, etc

Same naming scheme applies...

```
call n10s.onto.import.fetch(...)
```

Use autocompletion to discover the different procedures.

Full documentation will be available soon. In the meantime, please share your feedback in the [Neo4j community portal](https://community.neo4j.com/c/integrations/linked-data-rdf-ontology).

Thanks! 
