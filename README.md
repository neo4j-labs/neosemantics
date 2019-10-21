# NSMNTX
![NSMNTX Logo](http://neo4j-labs.github.io/neosemantics/docs/img/nsmntx-logo.png)
NSMNTX is a plugin that enables the **use of RDF in Neo4j**. [RDF is a W3C standard model](https://www.w3.org/RDF/) for data interchange.
This effectively means that NSMNTX makes it possible to

* **Store RDF data in Neo4j** in a
lossless manner (imported RDF can subsequently be exported without losing a single triple in the process).
* On-demand **export property graph data** from Neo4j *as RDF*.

Other features in NSMNTX include *model mapping* and *inferencing* on Neo4j graphs.

## ⇨ User Manual and Blog ⇦ 

⇨ Check out the complete **[user manual](https://neo4j.com/docs/labs/nsmntx/current/)** with examples of use. ⇦

⇨ [Blog on NSMNTX](https://jbarrasa.com/category/graph-rdf/) (and more). ⇦

## Installation
 
You can either download a prebuilt jar from the [releases area](https://github.com/neo4j-labs/neosemantics/releases) or build it from the source. If you prefer to build, check the note below.

1. Copy the  the jar(s) in the <NEO_HOME>/plugins directory of your Neo4j instance. (**note:** If you're going to use the JSON-LD serialisation format for RDF, you'll need to include also [APOC](https://neo4j.com/labs/apoc/))
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
This repository contains a set of stored procedures, user definded functions and extensions sumarised in [the reference section of the user manual](https://neo4j.com/docs/labs/nsmntx/current/reference/).

## Contributing

neosemantics code formatting follows the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

In order to contribute to this project, it is advisable to install the code formatting configuration in your preferred IDE:
* [Eclipse settings file](https://github.com/google/styleguide/blob/gh-pages/eclipse-java-google-style.xml)
* [IntelliJ IDEA settings file](https://github.com/google/styleguide/blob/gh-pages/eclipse-java-google-style.xml)

Please, make sure you format your code before commiting changes. Thanks!
