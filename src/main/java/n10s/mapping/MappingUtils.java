package n10s.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.NsPrefixMap;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class MappingUtils {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  private static final ValueFactory vf = SimpleValueFactory.getInstance();

  @Procedure(mode = Mode.WRITE)
  public Stream<MappingDesc> add(@Name("elementUri") String rdfVocElement,
      @Name("graphElementName") String graphElem)
      throws MappingDefinitionException, InvalidNamespacePrefixDefinitionInDB {

    IRI rdfVocElementIri = vf.createIRI(rdfVocElement);
    NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
    if(!prefixDefs.hasNs(rdfVocElementIri.getNamespace())){
      throw new MappingDefinitionException(
          "No namespace prefix defined for vocabulary " + rdfVocElementIri.getNamespace() + ".  "
              + "Define it first with call n10s.nsprefixes.add('yourprefix','" +
              rdfVocElementIri.getNamespace() + "')");
    }

    String prefix = prefixDefs.getPrefixForNs(rdfVocElementIri.getNamespace());
    Map<String, Object> params = new HashMap<>();
    params.put("namespace", rdfVocElementIri.getNamespace());
    params.put("prefix", prefix);
    params.put("local", rdfVocElementIri.getLocalName());
    params.put("graphElement", graphElem);


    String clearOldOccurences = "MATCH (oldmd:`_MapDef`)-[:`_IN`]->(oldns:`_MapNs`)  \n"
        + "WHERE oldmd._key = $graphElement OR (oldns._ns = $namespace AND oldmd._local = $local)\n"
        + "DETACH DELETE oldmd";

    String cleanOrphansIfAny = "MATCH (oldns:`_MapNs`)\n"
        + "WITH DISTINCT oldns WHERE size((oldns)<-[:_IN]-())=0\n"
        + "DELETE oldns";

    String createNewMapping = "MERGE (newmns:`_MapNs` { _ns: $namespace, _prefix: $prefix }) \n"
        + "MERGE  (newmd:`_MapDef` { _key: $graphElement, _local: $local})\n"
        + "MERGE (newmns)<-[:_IN]-(newmd)";

    tx.execute(clearOldOccurences, params);
    tx.execute(cleanOrphansIfAny);
    tx.execute(createNewMapping, params);

    return Stream
        .of(new MappingDesc(graphElem, rdfVocElementIri.getLocalName(), rdfVocElementIri.getNamespace(), prefix));
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<StringOutput> dropAll(@Name("namespace") String schemaUri) {
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
  public Stream<StringOutput> drop(@Name("graphElementName") String gElem) {
    String cypher = "MATCH (elem:_MapDef { _key : $local }) DETACH DELETE elem RETURN count(elem) AS ct";
    Map<String, Object> params = new HashMap<>();
    params.put("local", gElem);
    return Stream.of(new StringOutput(
        ((Long) tx.execute(cypher, params).next().get("ct")).equals(1L)
            ? "successfully deleted mapping" : "mapping not found"));
  }

  @Procedure(mode = Mode.READ)
  public Stream<MappingDesc> list(
      @Name(value = "schemaElem", defaultValue = "") String schemaElem) {

    Map<String, Object> params = new HashMap<>();
    params.put("elemName", schemaElem);

    String cypher = ("MATCH (mns:_MapNs)<-[:_IN]-(elem:_MapDef) WHERE toLower(elem._key) CONTAINS toLower($elemName) "
        + " OR toLower(elem._local) CONTAINS toLower($elemName) "
        + " RETURN elem._key AS elemName, elem._local AS schemaElement, mns._prefix AS schemaPrefix, mns._ns AS schemaNs  ");

    return tx.execute(cypher, params).stream().map(MappingDesc::new);
  }

  public static Map<String, String> getExportMappingsFromDB(GraphDatabaseService gds) {
    Map<String, String> mappings = new HashMap<>();
    gds.executeTransactionally(
        "MATCH (mp:_MapDef)-[:_IN]->(mns:_MapNs) RETURN mp._key AS key, mp._local AS local, mns._ns AS ns ",
        Collections.emptyMap(), new ResultTransformer<Object>() {
          @Override
          public Object apply(Result result) {
            while (result.hasNext()) {
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
        "MATCH (mp:_MapDef)-[:_IN]->(mns:_MapNs) RETURN mp._key AS key, mp._local AS local, mns._ns AS ns ",
        Collections.emptyMap(), new ResultTransformer<Object>() {
          @Override
          public Object apply(Result result) {
            while (result.hasNext()) {
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
