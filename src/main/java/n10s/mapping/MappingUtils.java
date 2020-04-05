package n10s.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import n10s.result.NamespacePrefixesResult;

public class MappingUtils {

  @Context
  public GraphDatabaseService db;
  
  @Context
  public Transaction tx;
  
  @Context
  public Log log;

  @Procedure(mode = Mode.WRITE)
  public Stream<NamespacePrefixesResult> addSchema(@Name("schemaUri") String uri,
      @Name("prefix") String prefix) throws MappingDefinitionException {
    //what should the logic be??? no two prefixes for the same ns and no more than one prefix for a voc
    Map<String, Object> params = new HashMap<>();
    params.put("schema", uri);
    params.put("prefix", prefix);
    String checkIfSchemaOrPrefixExist = "MATCH (mns:_MapNs { _ns: $schema}) RETURN mns "
        + "UNION MATCH (mns:_MapNs { _prefix : $prefix }) RETURN mns ";
    if (tx.execute(checkIfSchemaOrPrefixExist, params).hasNext()) {
      throw new MappingDefinitionException("The schema URI or the prefix are already in use. "
          + "Drop existing ones before reusing.");
    } else {
      String createNewSchema = "CREATE (mns:_MapNs { _ns: $schema, _prefix : $prefix }) "
          + "RETURN  mns._ns AS namespace, mns._prefix AS prefix ";
      return tx.execute(createNewSchema, params).stream().map(
          n -> new NamespacePrefixesResult((String) n.get("prefix"),
              (String) n.get("namespace")));
    }
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<NamespacePrefixesResult> addCommonSchemas() {
    String cypher = "CALL n10s.mapping.listSchemas() YIELD prefix, namespace "
        + "WITH collect(prefix) AS prefixes, collect(namespace) AS namespaces "
        + "WITH prefixes, namespaces, [ { namespace: 'http://schema.org/', prefix: 'sch' },\n" +
        "{ namespace: 'http://purl.org/dc/elements/1.1/', prefix: 'dc' },\n" +
        "{ namespace: 'http://purl.org/dc/terms/', prefix: 'dct' },\n" +
        "{ namespace: 'http://www.w3.org/2004/02/skos/core#', prefix: 'skos' },\n" +
        "{ namespace: 'http://www.w3.org/2000/01/rdf-schema#', prefix: 'rdfs' },\n" +
        "{ namespace: 'http://www.w3.org/2002/07/owl#', prefix: 'owl' },\n" +
        "{ namespace: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', prefix: 'rdf' },\n" +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/', prefix: 'fibo-be-corp-corp' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/', prefix: 'fibo-be-le-cb' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/FormalBusinessOrganizations/', prefix: 'fibo-be-le-fbo' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/', prefix: 'fibo-be-le-lp' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/AgentsAndPeople/Agents/', prefix: 'fibo-fnd-aap-agt' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Accounting/CurrencyAmount/', prefix: 'fibo-fnd-acc-cur' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/', prefix: 'fibo-fnd-dt-fd' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Law/Jurisdiction/', prefix: 'fibo-fnd-law-jur' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Organizations/FormalOrganizations/', prefix: 'fibo-fnd-org-fm' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/', prefix: 'fibo-fnd-rel-rel' },\n"
        +
        "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/', prefix: 'fibo-fnd-utl-av' }\n"
        +
        "] AS new \n" +
        " WITH [x IN new WHERE NOT (x.namespace IN namespaces OR x.prefix IN prefixes)] AS newfiltered "
        + " UNWIND newfiltered AS schemaDef " +
        " CALL n10s.mapping.addSchema(schemaDef.namespace, schemaDef.prefix) YIELD namespace, prefix "
        +
        " RETURN namespace, prefix";

    return tx.execute(cypher).stream().map(
        n -> new NamespacePrefixesResult((String) n.get("prefix"),
            (String) n.get("namespace")));
  }

  @Procedure(mode = Mode.READ)
  public Stream<NamespacePrefixesResult> listSchemas(
      @Name(value = "searchString", defaultValue = "") String searchString) {

    Map<String, Object> params = new HashMap<>();
    params.put("searchString", searchString);

    String cypher = (searchString.trim().equals("") ? "MATCH (mns:_MapNs) "
        + "RETURN mns._ns AS uri, mns._prefix AS prefix  " :
        "MATCH (mns:_MapNs) WHERE mns._ns CONTAINS $searchString OR mns._prefix CONTAINS $searchString "
            + "RETURN mns._ns AS uri, mns._prefix AS prefix ");

    return tx.execute(cypher, params).stream().map(
        n -> new NamespacePrefixesResult((String) n.get("prefix"),
            (String) n.get("uri")));
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<MappingDesc> addMappingToSchema(@Name("schemaUri") String schemaUri,
      @Name("graphElementName") String gElem,
      @Name("schemaElementName") String schElem) throws MappingDefinitionException {

    Node schema = tx.findNode(Label.label("_MapNs"), "_ns", schemaUri);
    if (schema == null) {
      throw new MappingDefinitionException(
          "Schema URI not defined. Define it first with semantics.mapping.addSchema('" +
              schemaUri + "','yourprefix') ");
    }
    Node mapDef;
    Map<String, Object> props = new HashMap<>();
    props.put("_key", gElem);
    ResourceIterator<Node> matchingNodes = tx.findNodes(Label.label("_MapDef"), props);
    // we need to find the schema it links to
    if (matchingNodes.hasNext()) {
      mapDef = matchingNodes.next();
      //if there is a mapping defined already for this element...
      Relationship rel = mapDef
          .getRelationships(Direction.OUTGOING, RelationshipType.withName("_IN")).iterator().next();
      if (rel.getEndNode().equals(schema)) {
        //and it links to the right schema, then just replace the local
        mapDef.setProperty("_local", schElem);
      } else {
        rel.delete();
        mapDef.createRelationshipTo(schema, RelationshipType.withName("_IN"));
      }

    } else {
      mapDef = tx.createNode(Label.label("_MapDef"));
      mapDef.setProperty("_key", gElem);
      mapDef.setProperty("_local", schElem);
      mapDef.createRelationshipTo(schema, RelationshipType.withName("_IN"));
    }

    return Stream
        .of(new MappingDesc(gElem, schElem, schemaUri, schema.getProperty("_prefix").toString()));
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<StringOutput> dropSchema(@Name("schemaUri") String schemaUri) {
    Map<String, Object> props = new HashMap<>();
    props.put("_ns", schemaUri);
    ResourceIterator<Node> schemas = tx.findNodes(Label.label("_MapNs"), props);
    if (!schemas.hasNext()) {
      return Stream.of(new StringOutput("schema not found"));
    } else {
      Node schemaToDelete = schemas.next();
      Iterable<Relationship> inRels = schemaToDelete
          .getRelationships(Direction.INCOMING, RelationshipType.withName("_IN"));
      inRels.forEach(x -> {
        x.getOtherNode(schemaToDelete).delete();
        x.delete();
      });
      schemaToDelete.delete();
      return Stream.of(new StringOutput("successfully deleted schema (and mappings)"));
    }
  }


  @Procedure(mode = Mode.WRITE)
  public Stream<StringOutput> dropMapping(@Name("graphElementName") String gElem) {
    String cypher = "MATCH (elem:_MapDef { _key : $local }) DETACH DELETE elem RETURN count(elem) AS ct";
    Map<String, Object> params = new HashMap<>();
    params.put("local", gElem);
    return Stream.of(new StringOutput(
        ((Long) tx.execute(cypher, params).next().get("ct")).equals(1L)
            ? "successfully deleted mapping" : "mapping not found"));
  }

  @Procedure(mode = Mode.READ)
  public Stream<MappingDesc> listMappings(
      @Name(value = "schemaElem", defaultValue = "") String schemaElem) {

    Map<String, Object> params = new HashMap<>();
    params.put("elemName", schemaElem);

    String cypher = ("MATCH (mns:_MapNs)<-[:_IN]-(elem:_MapDef) WHERE elem._key CONTAINS $elemName "
        + " OR elem._local CONTAINS $elemName"
        +
        " RETURN elem._key AS elemName, elem._local AS schemaElement, mns._prefix AS schemaPrefix, mns._ns AS schemaNs  ");

    return tx.execute(cypher, params).stream().map(MappingDesc::new);
  }

  public static Map<String, String> getExportMappingsFromDB(GraphDatabaseService gds) {
    Map<String, String> mappings = new HashMap<>();
    gds.executeTransactionally(
            "MATCH (mp:_MapDef)-[:_IN]->(mns:_MapNs) RETURN mp._key AS key, mp._local AS local, mns._ns AS ns ", Collections.emptyMap(), new ResultTransformer<Object>() {
              @Override
              public Object apply(Result result) {
                while(result.hasNext()) {
                  Map<String, Object> row = result.next();
                  mappings.put((String) row.get("key"),
                          (String) row.get("ns") + (String) row.get("local"));
                }
                return null;
              }
            });
    return mappings;
  }

  public static Map<String, String> getImportMappingsFromDB(GraphDatabaseService gds) {
    Map<String, String> mappings = new HashMap<>();
    gds.executeTransactionally(
            "MATCH (mp:_MapDef)-[:_IN]->(mns:_MapNs) RETURN mp._key AS key, mp._local AS local, mns._ns AS ns ", Collections.emptyMap(), new ResultTransformer<Object>() {
              @Override
              public Object apply(Result result) {
                while(result.hasNext()) {
                  Map<String, Object> row = result.next();
                  mappings.put((String) row.get("ns") + (String) row.get("local"),
                          (String) row.get("key"));
                }
                return null;
              }
            });

    return mappings;
  }


  public class StringOutput {

    public String output;

    public StringOutput(String output) {
      this.output = output;
    }
  }

  public class MappingDesc {

    public String schemaNs;
    public String schemaPrefix;
    public String schemaElement;
    public String elemName;


    public MappingDesc(Map<String, Object> record) {
      this.elemName = record.get("elemName").toString();
      this.schemaElement = record.get("schemaElement").toString();
      this.schemaNs = record.get("schemaNs").toString();
      this.schemaPrefix = record.get("schemaPrefix").toString();
    }

    public MappingDesc(String elemName, String schemaElement, String schemaNs,
        String schemaPrefix) {
      this.elemName = elemName;
      this.schemaElement = schemaElement;
      this.schemaNs = schemaNs;
      this.schemaPrefix = schemaPrefix;
    }

  }

  private class MappingDefinitionException extends Throwable {

    public MappingDefinitionException(
        String s) {
      super(s);
    }
  }
}
