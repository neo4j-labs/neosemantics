package n10s.experimental.dimodel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

public class DIModelBuilder {

    private Map<IRI, DIMNodeDef> modelDef;
    private static int MAX_NUM_NODES = 25;
    private static int MAX_NUM_RELS = 250;

    public DIModelBuilder(Transaction tx, Log log) {
         modelDef = new HashMap<>();
    }

    public void buildDIModel(InputStream is, RDFFormat format, Map<String, Object> props)
            throws IOException {

        String urilist ;
        Map<IRI,Set<IRI>> uriMap = new HashMap<>();

        if(props.containsKey("classList")){
            if (props.get("classList") instanceof List){
                if( !((List)props.get("classList")).isEmpty() ){
                    urilist = formatUriList((List<String>)props.get("classList"));
                } else{
                    //UriList is empty. Import the whole* onto.
                    urilist = null;
                }
            } else {
                throw new IllegalArgumentException("classList must contain a list of uris (strings)");
            }
        } else {
            //No urilist parameter. Import the whole* onto.
            urilist = null;
        }

        Repository repo = new SailRepository(new MemoryStore());

        try (RepositoryConnection conn = repo.getConnection()) {

            conn.begin();
            conn.add(new InputStreamReader(is), "http://neo4j.com/base/", format);
            conn.commit();

            TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL,
                    (urilist!=null?String.format(catsQuery, urilist):allCatsQuery));
            TupleQueryResult queryResult = tupleQuery.evaluate();
            Set<IRI> allClasses = new HashSet<>();
            while (queryResult.hasNext()) {
                BindingSet next = queryResult.next();
                allClasses.add((IRI)next.getValue("parent"));
                if(uriMap.containsKey(next.getValue("parent"))){
                    Set<IRI> existingSet = uriMap.get(next.getValue("parent"));
                    existingSet.add((IRI)next.getValue("explicit"));
                } else {
                    Set<IRI> newSet = new HashSet<>();
                    newSet.add((IRI)next.getValue("explicit"));
                    uriMap.put((IRI)next.getValue("parent"), newSet);
                }
            }

            StringBuilder sb = new StringBuilder();
            allClasses.forEach( c -> sb.append(", <").append(c.stringValue()).append(">"));

            urilist = sb.length()>0?sb.substring(1):sb.toString();

            //initialise the modelDef with all explicit classes
            HashSet<IRI> explicitClassSet = new HashSet<>();
            uriMap.values().forEach( s -> explicitClassSet.addAll(s));

            if(explicitClassSet.size() > MAX_NUM_NODES){
                //TODO: check exception type
                throw new RuntimeException("The ontology contains a large number of classes (" + explicitClassSet.size() + " leaf classes) that would generate an" +
                        " unusable model. Please select a subset using the 'classList' parameter. ");
            }

            explicitClassSet.forEach( c -> modelDef.put(c, new DIMNodeDef(c)));

            addRels(String.format(relsQuery, urilist, urilist), uriMap, conn);

            if(modelDef.values().stream().map(md->md.getRelCount()).reduce(0, Integer::sum) > MAX_NUM_RELS){
                //TODO: check exception type
                throw new RuntimeException("The ontology contains a large number of classes and relationships ( " +
                        modelDef.values().stream().map(md->md.getRelCount()).reduce(0, Integer::sum) +
                        " ) that would generate an" +
                        " unusable model. Please select a subset using the 'classList' parameter. ");
            }

            addProps(String.format(propsQuery, urilist, urilist), uriMap, conn);

        }
    }


    private String printModelDefSummary() {
        StringBuilder sb = new StringBuilder();
        for( DIMNodeDef d:modelDef.values()){
            sb.append(d);
        }
        return sb.toString();
    }

    public Map<String, Object> getModelAsSerialisableObject() {

        assignPositionsToNodes();

        Map<String, Object> map = new HashMap<>();
        map.put("version", "0.1.1-beta.0");

        Map<String, Object> graph = new HashMap<>();
        map.put("graph", graph);
        List<Object> graphNodes = new ArrayList<>();
        graph.put("nodes", graphNodes);
        modelDef.forEach((k,v) -> graphNodes.add(v.getGraphNodeAsJsonObject()));
        List<Object> graphRels = new ArrayList<>();
        graph.put("relationships", graphRels);
        modelDef.forEach((k,v) -> graphRels.addAll(v.getGraphRelsAsJsonObject()));

        Map<String, Object> datamodel = new HashMap<>();
        map.put("dataModel", datamodel);

        Map<String, Object> filemodel = new HashMap<>();
        datamodel.put("fileModel", filemodel);
        filemodel.put("fileSchemas", Collections.EMPTY_MAP);


        Map<String, Object> graphmodel = new HashMap<>();
        datamodel.put("graphModel", graphmodel);
        Map<String, Object> nodeSchemas = new HashMap<>();
        graphmodel.put("nodeSchemas", nodeSchemas);
        modelDef.forEach((k,v) -> nodeSchemas.put(k.stringValue(), v.getNodeSchemasAsJsonObject()));

        Map<String, Object> relSchemas = new HashMap<>();
        graphmodel.put("relationshipSchemas", relSchemas);
        modelDef.forEach((k,v) -> relSchemas.putAll(v.getRelSchemasAsJsonObject()));

        Map<String, Object> mappingModel = new HashMap<>();
        datamodel.put("mappingModel", mappingModel);

        Map<String, Object> nodeMappings = new HashMap<>();
        mappingModel.put("nodeMappings",nodeMappings);
        modelDef.forEach((k,v) -> nodeMappings.put(k.stringValue(), v.getNodeMappingsAsJsonObject()));
        Map<String, Object> relMappings = new HashMap<>();
        mappingModel.put("relationshipMappings",relMappings);
        modelDef.forEach((k,v) -> relMappings.putAll(v.getRelsMappingsAsJsonObject()));

        return map;
    }

    private void assignPositionsToNodes() {

        long rows = Math.round(Math.sqrt(modelDef.size() / 2 ));
        if(rows==0) rows = 1; //avoid div by zero
        long cols = 2 * rows;
        long step_h = 2400 / cols;
        long step_v = 1200 / rows;
        long x_start = -1000L + step_h / 2 ;
        long y_start = step_v / 2 ;
        int i = 0;
        for (IRI n:modelDef.keySet()) {
            modelDef.get(n).setPos(x_start + ((i%cols)*step_h),y_start + ((i/cols)*step_v)) ;
            i++;
        }
    }

    private void addProps(String theQuery, Map<IRI, Set<IRI>> uriMap, RepositoryConnection conn) {
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, theQuery);
        TupleQueryResult queryResult = tupleQuery.evaluate();
        while (queryResult.hasNext()) {
            BindingSet next = queryResult.next();
            Set<IRI> domains = uriMap.get((IRI) next.getValue("domain"));
            for(IRI domain:domains) {
                DIMNodeDef nd;
                nd = modelDef.get(domain);
                nd.addProp((IRI) next.getValue("prop"), (IRI) next.getValue("range"));
            }
        }
    }

    private void addRels(String theQuery, Map<IRI, Set<IRI>> uriMap, RepositoryConnection conn) {
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, theQuery);
        TupleQueryResult queryResult = tupleQuery.evaluate();
        while (queryResult.hasNext()) {
            BindingSet next = queryResult.next();
            Set<IRI> domains = uriMap.get((IRI)next.getValue("domain"));
            for(IRI domain:domains) {
                DIMNodeDef nd;
                nd = modelDef.get(domain);
                Set<IRI> ranges = uriMap.get((IRI) next.getValue("range"));
                for(IRI range:ranges) {
                    nd.addRel((IRI) next.getValue("prop"), range);
                }
            }
        }
    }


    private String formatUriList(List urilist) {
        StringBuilder sb = new StringBuilder();
        urilist.forEach(s -> sb.append(", <").append(s).append(">"));
        return sb.substring(1);
    }

    public static String catsQuery = "" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "select distinct ?explicit ?parent\n" +
            "where {\n" +
            "  ?explicit rdfs:subClassOf* ?parent\n" +
            "    filter( ?explicit in ( %s ) && isIRI(?parent))\n" +
            "  }\n";

    public static String allCatsQuery = "" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "select distinct ?explicit ?parent\n" +
            "where {\n" +
            "  ?explicit a ?classtype    \n" +
            "    filter( (?classtype in ( owl:Class, rdfs:Class )) && not exists { ?x rdfs:subClassOf ?explicit })\n" +
            "   \n" +
            "  ?explicit rdfs:subClassOf* ?parent\n" +
            "    filter( isIRI(?parent) )\n" +
            "  }";

    public static String relsQuery = "" +
            "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "prefix sch: <https://schema.org/>\n" +
            "select distinct ?prop ?domain ?range\n" +
            "where {\n" +
            "\n" +
            "   filter(?domain in ( %s )\n" +
            "    && ?range in ( %s ))\n" +
            "  \n" +
            "  ?prop a ?propertyClass .\n" +
            "  filter(?propertyClass in (rdf:Property, owl:ObjectProperty, owl:FunctionalProperty, owl:AsymmetricProperty, " +
            " owl:InverseFunctionalProperty, owl:IrreflexiveProperty, owl:ReflexiveProperty, owl:SymmetricProperty, owl:TransitiveProperty))\n" +
            "\n" +
            "  {\n" +
            "    ?prop ?domainPred ?domain ; ?rangePred ?range .\n" +
            "    \tfilter(?domainPred in (sch:domainIncludes, rdfs:domain) && ?rangePred in (sch:rangeIncludes, rdfs:range))\n" +
            "  } union {\n" +
            "    ?prop ?domainPred [ owl:unionOf/rdf:rest*/rdf:first  ?domain ]\n" +
            "          filter(?domainPred in (sch:domainIncludes, rdfs:domain) )\n" +
            "  } union {\n" +
            "    ?domain rdfs:subClassOf [ a                   owl:Restriction ;\n" +
            "                            owl:onProperty      ?prop ;\n" +
            "                            ?restrictionPred  ?range\n" +
            "                          ] ;\n" +
            "          filter(?restrictionPred in (owl:someValuesFrom, owl:allValuesFrom ))\n" +
            "  } union {\n" +
            "    ?domain rdfs:subClassOf [ a                   owl:Restriction ;\n" +
            "                            owl:onProperty      ?prop ;\n" +
            "                            ?cardinalityRestriction  ?card ;\n" +
            "        \t\t\t\t\towl:onClass ?range \n" +
            "                          ] ;\n" +
            "          filter(?cardinalityRestriction in (owl:qualifiedCardinality, owl:minQualifiedCardinality, " +
            "  owl:maxQualifiedCardinality ))\n" +
            "  }\n" +
            "\n" +
            "}";

    public static String propsQuery = "" +
            "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
            "prefix sch: <https://schema.org/>\n" +
            "select distinct ?prop ?domain ?range\n" +
            "where {\n" +
            "\n" +
            "  ?prop a ?propertyClass .\n" +
            "  filter(?propertyClass in (rdf:Property, owl:DatatypeProperty, owl:FunctionalProperty ))\n" +
            "\n" +
            "  {\n" +
            "    ?prop ?domainPred ?domain\n" +
            "    \tfilter(?domainPred in (sch:domainIncludes, rdfs:domain) &&\n" +
            "        \t?domain in ( %s ))\n" +
            "  } union {\n" +
            "    ?prop ?domainPred [ owl:unionOf/rdf:rest*/rdf:first  ?domain ]\n" +
            "          filter(?domainPred in (sch:domainIncludes, rdfs:domain) &&\n" +
            "             ?domain in ( %s ))\n" +
            "  }\n" +
            "\n" +
            "  optional {\n" +
            "    ?prop ?rangePred ?range\n" +
            "    filter(?rangePred in (sch:rangeIncludes, rdfs:range) && " +
            "   (?range in ( sch:Text ) || regex(str(?range),\"^http://www.w3.org/2001/XMLSchema#.*\")))\n" +
            "  }\n" +
            "\n" +
            "}";

    public Stream<DIModelSummary> exportDIModelToFile(Map<String,Object> map, String mappings) throws IOException {

        File configFile = new File(Stream.of(System.getProperty("sun.java.command").split("--")).map(String::trim)
                .filter(s -> s.startsWith("config-dir="))
                .map(s -> s.substring("config-dir=".length()))
                .findFirst()
                .orElse(".").concat(File.separator).concat("neo4j.conf"));
        Properties p = new Properties();
        p.load(new FileReader(configFile));
        String importFolderPath = p.contains("dbms.directories.import") ? p.get("dbms.directories.import").toString() : "import";
        File diModelFileName = new File(Stream.of(System.getProperty("sun.java.command").split("--")).map(String::trim)
                .filter(s -> s.startsWith("home-dir="))
                .map(s -> s.substring("home-dir=".length()))
                .findFirst()
                .orElse(".").concat(File.separator).concat(importFolderPath).concat(File.separator).concat("diModel.json"));
        OutputStream os = new BufferedOutputStream(new FileOutputStream(diModelFileName));

        ObjectMapper mapper = new ObjectMapper();

        mapper.writerWithDefaultPrettyPrinter().writeValue(os, map);

        //serialiseModelDef(os);
        return Stream.of(new DIModelSummary(diModelFileName.getAbsolutePath(),mappings, printModelDefSummary()));
    }

    public Stream<DIModelSummary> exportDIModelAsString(Map<String,Object> map, String mappings) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return Stream.of(new DIModelSummary(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map),
                mappings, printModelDefSummary()));
    }

    public String getModelMappings() {

        Map<String, String> standardns = new HashMap<>();
        standardns.put("http://schema.org/", "sch");
        standardns.put("http://www.w3.org/2004/02/skos/core#", "skos");
        standardns.put("http://www.w3.org/2008/05/skos-xl#", "skosxl");
        standardns.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs");
        standardns.put("http://www.w3.org/2002/07/owl#", "owl");
        standardns.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
        standardns.put("http://www.w3.org/ns/shacl#", "sh");
        standardns.put("http://www.w3.org/2001/XMLSchema#", "xsd");

        Set <String> namespaces = new HashSet<>();
        Set <IRI> schemaElements = new HashSet<>();
        modelDef.forEach((className,ndef) ->
            { namespaces.add(className.getNamespace());
              schemaElements.add(className);
              ndef.props.forEach((propname,type) ->
                { if (!propname.stringValue().equals("neo4j://graph.schema#uri")) {
                    namespaces.add(propname.getNamespace());
                    schemaElements.add(propname);
                    }
                });
              ndef.rels.forEach((relname,range) ->
                { namespaces.add(relname.getNamespace());
                  schemaElements.add(relname);
            });
            });

        int nscount = 1;
        StringBuilder sb = new StringBuilder();
        for(String ns: namespaces) {
            sb.append("CALL n10s.nsprefixes.add(\"");
            if (standardns.containsKey(ns)) {
                sb.append(standardns.get(ns));
            } else {
                sb.append("ns").append(nscount++);
            }
            sb.append("\",\"" + ns + "\");").append("\n");
        }
        sb.append("\n\n");
        schemaElements.forEach(se ->
                sb.append("CALL n10s.mapping.add(\"" + se + "\", \"" + se.getLocalName() + "\");").append("\n"));

        return sb.toString();
    }


}
