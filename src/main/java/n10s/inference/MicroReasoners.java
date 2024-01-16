package n10s.inference;

import static org.neo4j.graphdb.RelationshipType.withName;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import n10s.graphconfig.GraphConfig;
import n10s.result.LabelNameResult;
import n10s.result.NodeResult;
import n10s.result.RelAndNodeResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

public class MicroReasoners {

  private static final String sloInferenceFormatReturnClassNames = "CALL db.labels() YIELD label "
          + " WITH collect(label) as labels MATCH path = (:`%1$s` { `%2$s` : $virtLabel } )<-[:`%3$s`*]-(s:`%1$s`) "
          + " WHERE s.`%2$s` in labels "
          + " RETURN COLLECT(DISTINCT s.`%2$s`) + $virtLabel  as l";

  private static final String subcatPathQuery =
      "MATCH (x:`%1$s` { `%2$s`: $oneOfCats } ) MATCH (y:`%1$s` { `%2$s`: $virtLabel } ) "
          + " WHERE  (x)-[:`%3$s`*]->(y) RETURN count(x) > 0 as isTrue ";

  private static final String scoInferenceCypherTopDownQuery = "MATCH (cat)<-[:`%1$s`*0..]-(subcat) WHERE elementid(cat) = $catId RETURN collect(DISTINCT elementid(subcat)) AS catIds";
  private static final String scoInferenceCypherBottomUpQuery = "MATCH (cat)<-[:`%1$s`*0..]-(subcat) WHERE elementid(subcat) = $catId RETURN collect(DISTINCT elementid(cat)) AS catIds";
  private static final String sroInferenceFormatReturnRelNamesQuery = "RETURN $virtRel as r UNION MATCH (:`%1$s` { `%2$s`: $virtRel})<-[:`%3$s`*]-(sr:`%1$s`) RETURN DISTINCT sr.`%2$s` as r";
  //TODO: come up with a well defined approach for class and rel name properties
  private static final boolean DEFAULT_SEARCH_TOP_DOWN = false;

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  /* get nodes with a given label explicit or inferred.
   * semantics (cat:Cat { name: 'xyz'})-[:SCO]->(parent:Cat { name: ''}) */

  @Procedure(mode = Mode.READ)
  @Description("n10s.inference.nodesLabelled('label') - returns all nodes with label 'label' or its sublabels.")
  public Stream<NodeResult> nodesLabelled(@Name("label") String virtLabel,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {
    final GraphConfig gc = getGraphConfig();

    if(gc == null && missingParams(props, "catLabel", "catNameProp", "subCatRel")){
      throw new MicroReasonerException("No GraphConfig or in-procedure params (catLabel,catNameProp,subCatRel). Method cannot be run.");
    }

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("virtLabel", virtLabel);
    Result results = tx.execute(String.format(sloInferenceFormatReturnClassNames,
        (props.containsKey("catLabel") ? (String) props.get("catLabel") : gc.getClassLabelName()),
        (props.containsKey("catNameProp") ? (String) props.get("catNameProp")
            : gc.getClassNamePropName()),
        (props.containsKey("subCatRel") ? (String) props.get("subCatRel") : gc.getSubClassOfRelName())),
        params);

    List<String> labelList = (List<String>) results.next().get("l");

    StringBuilder sb = new StringBuilder();
    sb.append("cypher runtime=slotted ");
    sb.append("unwind [] as result return result ");
    labelList
        .forEach(x -> sb.append(" UNION MATCH (x:`").append(x).append("`) RETURN x as result "));

    return tx.execute(sb.toString()).stream().map(n -> (Node) n.get("result"))
        .map(NodeResult::new);

  }

  private GraphConfig getGraphConfig() {
    try {
      return new GraphConfig(tx);
    } catch (GraphConfig.GraphConfigNotFound graphConfigNotFound) {
      //no graph config
      return null;
    }
  }

  /* in this case the node representing the category exist in the graph and is explicitly linked to the instances of the category
   *  hence the use of a node as param */
  @Procedure(mode = Mode.READ)
  @Description("n10s.inference.nodesInCategory('category') - returns all nodes connected to Node 'catNode' or its subcategories.")
  public Stream<NodeResult> nodesInCategory(@Name("category") Node catNode,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    final GraphConfig gc = getGraphConfig();

    //if no graphconfig (or ontoconfig) and no required in-function params, funcion cannot be invoked
    if(gc == null && missingParams(props, "subCatRel")){
      throw new MicroReasonerException("No GraphConfig or in-procedure params (subCatRel). Method cannot be run.");
    }

    final String inCatRelName = (props.containsKey("inCatRel") ? (String) props.get("inCatRel")
        : getDefaultIncatRel(gc));
    final String subCatRelName = (props.containsKey("subCatRel") ? (String) props.get("subCatRel")
        : gc.getSubClassOfRelName());

    Map<String, Object> params = new HashMap<>();
    params.put("catId", catNode.getElementId());

    String cypher = "MATCH (rootCategory)<-[:`" + subCatRelName + "`*0..]-()<-[:`" +
        inCatRelName + "`]-(individual) WHERE elementid(rootCategory) = $catId RETURN individual ";

    return tx.execute(cypher, params).stream().map(n -> (Node) n.get("individual"))
        .map(NodeResult::new);
  }

  private String getDefaultIncatRel(GraphConfig gc) {

    if(gc.getHandleRDFTypes() == GraphConfig.GRAPHCONF_RDFTYPES_AS_NODES ||
            gc.getHandleRDFTypes() == GraphConfig.GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES){
      if(gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_IGNORE ||
              gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_MAP){
        return "type";
      } else if (gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_SHORTEN ||
              gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_SHORTEN_STRICT){
        return "rdf__type";
      } else if (gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_KEEP){
        return "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
      }
    }
    return "IN_CATEGORY";

  }

  private List<Long> getSubcatIds(Node catNode, String subCatRelName, GraphConfig gc) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("catId", catNode.getElementId());
    return (List<Long>) tx.execute( String.format(scoInferenceCypherTopDownQuery,
            (subCatRelName == null ? gc.getSubClassOfRelName():subCatRelName)), params).next().get("catIds");
            //scoInferenceCypherTopDown
        //: scoInferenceCypherTopDown.replace("SCO", subCatRelName)), params).next().get("catIds");
  }

  private List<Long> getSuperCatIds(String catNodeId, String subCatRelName, GraphConfig gc) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("catId", catNodeId);
    return (List<Long>) tx.execute( String.format(scoInferenceCypherBottomUpQuery,
            (subCatRelName == null ? gc.getSubClassOfRelName():subCatRelName)), params).next().get("catIds");
  }

  @Procedure(mode = Mode.READ)
  @Description(
      "n10s.inference.getRels(node,'rel', { relDir: '>'} ) - returns all relationships "
          + "of type 'rel' or its subtypes along with the target nodes.")
  public Stream<RelAndNodeResult> getRels(@Name("node") Node node, @Name("rel") String virtRel,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    final GraphConfig gc = getGraphConfig();

    //if no graphconfig (or ontoconfig) and no required in-function params, funcion cannot be invoked
    if(gc == null && missingParams(props, "relLabel","subRelRel","relNameProp")){
      throw new MicroReasonerException("No GraphConfig or in-procedure params (relLabel, subRelRel, relNameProp). Method cannot be run.");
    }

    String directionString = (props.containsKey("relDir") ? (String) props.get("relDir") : "");
    Direction direction = (directionString.equals(">") ? Direction.OUTGOING
        : (directionString.equals("<") ? Direction.INCOMING : Direction.BOTH));

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("virtRel", virtRel);

    //this test could be removed, we've tested already that the props are there (!!)
    Result results = tx.execute(String.format(sroInferenceFormatReturnRelNamesQuery,
        (props.containsKey("relLabel") ? (String) props.get("relLabel") : gc.getObjectPropertyLabelName()),
        (props.containsKey("relNameProp") ? (String) props.get("relNameProp")
            : gc.getRelNamePropName()),
        (props.containsKey("subRelRel") ? (String) props.get("subRelRel") : gc.getSubPropertyOfRelName())),
        params);
    Set<RelationshipType> rts = new HashSet<RelationshipType>();
    while (results.hasNext()) {
      rts.add(withName((String) results.next().get("r")));
    }

    return StreamSupport.stream(
        node.getRelationships(direction, rts.toArray(new RelationshipType[0])).spliterator(), true)
        .map(n -> new RelAndNodeResult(n, n.getOtherNode(node)));

  }

  /*
  TODO:
      split procs between onto inferences and instance inferences
      use sco instead of mappings to align with public ontologies
      export with an ontology annotation of the type neo:Person rdfs:subClassOf sch:Person
      add a remove-ontology method
      add a get-class-by-name method
      add a datatype label for xsd: elements
      add a  n10s.inference.prop_datatype method
   */


  @Procedure(mode = Mode.READ)
  @Description("n10s.inference.labels() - returns all labels in use in the graph, including inferred ones.")
  public Stream<LabelNameResult> labels(@Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {
    final GraphConfig gc = getGraphConfig();

    if (gc == null && missingParams(props, "catLabel", "catNameProp", "subCatRel", "relLabel", "propLabel")) {
      throw new MicroReasonerException("No GraphConfig or in-procedure params (catLabel, catNameProp, subCatRel, relLabel, propLabel). Method cannot be run.");
    }

    String cypher;

    if (gc == null){
      cypher = getInferredLabelsQuery((String) props.get("catLabel"),
              (String) props.get("subCatRel"),
              (String) props.get("catNameProp"),
              (String) props.get("relLabel"),
              (String) props.get("propLabel"));
    } else {
      cypher = getInferredLabelsQuery((String) props.getOrDefault("catLabel", gc.getClassLabelName()),
              (String) props.getOrDefault("subCatRel", gc.getSubClassOfRelName()),
              (String) props.getOrDefault("catNameProp", gc.getClassNamePropName()),
              (String) props.getOrDefault("relLabel", gc.getObjectPropertyLabelName()),
              (String) props.getOrDefault("propLabel", gc.getDataTypePropertyLabelName()));
    }
    return tx.execute(cypher).stream().map(n -> (String) n.get("inferredlabel")).map(LabelNameResult::new);
  }

  private static final String getInferredLabelsQuery(String catLabel, String subCatRel, String catNameProp,
                                                     String relLabel, String propLabel ) {
    return "call db.labels() yield label " +
    " where not label in ['_GraphConfig', 'Resource', '_NsPrefDef', '" + relLabel + "', '" + propLabel + "', '" + catLabel + "', '_MapNs', '_MapDef'] " +
            " call { " +
            " with label " +
            " return label as inferredlabel " +
            " union " +
            " with label " +
            " match hierarchy = (:`" + catLabel + "` { `" + catNameProp + "`: label })-[:`" + subCatRel + "`*0..]->(p) where size([(p)-[s:`" + subCatRel + "`]->() | s]) = 0 " +
            " unwind [n in nodes(hierarchy) | n.`" + catNameProp + "` ] as inferredlabel " +
            " return distinct inferredlabel " +
            " } " +
            " return distinct inferredlabel " ;
  }

  private static final String getClassRelsFromOntoQuery(boolean outgoing, boolean includeAll, String catLabel,
                                                    String scoRelType, String relLabel, String sroRelType,
                                                    String domainRelLabel, String rangeRelLabel){

    return " match (c) where c=$node with c match (c)-[:`" + scoRelType + "`*0..]->(hook:`" + catLabel + "`)<-[:`" + (outgoing?domainRelLabel:rangeRelLabel) + "`]-(rel:`" + relLabel + "`) " +
                    (!includeAll?" where not (rel)<-[:`" + sroRelType + "`*]-(:`" + relLabel + "`)-[:`" + (outgoing?domainRelLabel:rangeRelLabel) + "`]->(:`" + catLabel + "`)<-[:`" + scoRelType + "`*0..]-(c)  ":"") +
                    " return collect(distinct rel) as rels" ;
  }

  private static final String getRelDomainsOrRangesFromOntoQuery(boolean outgoing, boolean includeAll, String catLabel,
                                                        String scoRelType, String relLabel, String sroRelType,
                                                        String domainRelLabel, String rangeRelLabel){
    return " match (rel) where rel=$node with rel " +
                    " match (rel)-[:`" + sroRelType + "`*0..]->(hook:`" + relLabel + "`)-[:`" + (outgoing?rangeRelLabel:domainRelLabel) + "`]->(target) " +
                    (!includeAll?" where not (target)<-[:`" + scoRelType + "`*]-(:`" + catLabel + "`)<-[:`" + (outgoing?rangeRelLabel:domainRelLabel) + "`]-(:`" + relLabel + "`)<-[:`" + sroRelType + "`*0..]-(rel) ":"") +
                    " return collect(distinct target) as others " ;
  }

  private static final String getClassPropsFromOntoQuery(boolean includeAll, String catLabel,
                                                        String scoRelType, String propLabel, String sroRelType,
                                                        String domainRelLabel){

    return " match (c) where c=$node with c match (c)-[:`" + scoRelType + "`*0..]->(hook:`" + catLabel + "`)<-[:`" + domainRelLabel + "`]-(prop:`" + propLabel + "`) " +
            (!includeAll?" where not (prop)<-[:`" + sroRelType + "`*]-(:`" + propLabel + "`)-[:`" + domainRelLabel + "`]->(:`" + catLabel + "`)<-[:`" + scoRelType + "`*0..]-(c)  ":"") +
            " return collect(distinct prop) as props" ;
  }

  @UserFunction
  @Description(
          "n10s.inference.class_out_rels(class, { includeAll: true, catLabel:''...} ) " +
                  "- returns inferred outgoing relationships for a given class")
  public List<Node> class_outgoing_rels(@Name("class") Node node,
                @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    return getInferredRelsForClass(node, true, props);
  }

  @UserFunction
  @Description(
          "n10s.inference.class_in_rels(class, { includeAll: true, catLabel:''...} ) " +
                  "- returns inferred incoming relationships for a given class")
  public List<Node> class_incoming_rels(@Name("class") Node node,
                                   @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    return getInferredRelsForClass(node, false, props);
  }

  @UserFunction
  @Description(
          "n10s.inference.rel_targets(rel, { includeAll: true, catLabel:''...} ) " +
                  "- returns inferred targets (ranges) for a given relationship")
  public List<Node> rel_target_classes(@Name("rel") Node node,
                                  @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    return getInferredClassForRels(node, true, props);
  }

  @UserFunction
  @Description(
          "n10s.inference.rel_targets(rel, { includeAll: true, catLabel:''...} ) " +
                  "- returns inferred sources (domains) for a given relationship")
  public List<Node> rel_source_classes(@Name("rel") Node node,
                                @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    return getInferredClassForRels(node, false, props);
  }

  @UserFunction
  @Description(
          "n10s.inference.class_props(class, { catLabel:''...} ) " +
                  "- returns inferred properties for a given class")
  public List<Node> class_props(@Name("class") Node node,
                                   @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    return getInferredPropsForClass(node, props);
  }

  private List<Node> getInferredRelsForClass(Node node, boolean outgoing, Map<String, Object> props) throws MicroReasonerException {
    final GraphConfig gc = getGraphConfig();

    //if no graphconfig (or ontoconfig) function cannot be invoked (maybe defaults?)
    if(gc == null){ // && missingParams(props, "catLabel", "subCatRel","relLabel","subRelRel", "domainRel", "rangeRel")
      throw new MicroReasonerException("No GraphConfig. Method cannot be run.");
    }

    Result results = tx.execute(getClassRelsFromOntoQuery(outgoing,
                    (Boolean) props.getOrDefault("includeAll",false),
                    (String) props.getOrDefault("catLabel",gc.getClassLabelName()),
                    (String) props.getOrDefault("subCatRel",gc.getSubClassOfRelName()),
                    (String) props.getOrDefault("relLabel",gc.getObjectPropertyLabelName()),
                    (String) props.getOrDefault("subRelRel",gc.getSubPropertyOfRelName()),
                    (String) props.getOrDefault("domainRel",gc.getDomainRelName()),
                    (String) props.getOrDefault("rangeRel",gc.getRangeRelName())),
            Map.of("node", node));

      return (results.hasNext()?(List<Node>) results.next().get("rels"):Collections.emptyList());

  }

  private List<Node> getInferredClassForRels(Node node, boolean outgoing, Map<String, Object> props) throws MicroReasonerException {
    final GraphConfig gc = getGraphConfig();

    //if no graphconfig (or ontoconfig) function cannot be invoked (maybe defaults?)
    if(gc == null){ // && missingParams(props, "catLabel", "subCatRel","relLabel","subRelRel", "domainRel", "rangeRel")
      throw new MicroReasonerException("No GraphConfig. Method cannot be run.");
    }

    Result results = tx.execute(getRelDomainsOrRangesFromOntoQuery(outgoing,
                    (Boolean) props.getOrDefault("includeAll",false),
                    (String) props.getOrDefault("catLabel",gc.getClassLabelName()),
                    (String) props.getOrDefault("subCatRel",gc.getSubClassOfRelName()),
                    (String) props.getOrDefault("relLabel",gc.getObjectPropertyLabelName()),
                    (String) props.getOrDefault("subRelRel",gc.getSubPropertyOfRelName()),
                    (String) props.getOrDefault("domainRel",gc.getDomainRelName()),
                    (String) props.getOrDefault("rangeRel",gc.getRangeRelName())),
            Map.of("node", node));

    return (results.hasNext()?(List<Node>) results.next().get("others"):Collections.emptyList());
  }

  private List<Node> getInferredPropsForClass(Node node, Map<String, Object> props) throws MicroReasonerException {
    final GraphConfig gc = getGraphConfig();

    //if no graphconfig (or ontoconfig) function cannot be invoked (maybe defaults?)
    if(gc == null){ // && missingParams(props, "catLabel", "subCatRel","relLabel","subRelRel", "domainRel", "rangeRel")
      throw new MicroReasonerException("No GraphConfig. Method cannot be run.");
    }

    Result results = tx.execute(getClassPropsFromOntoQuery(
                    (Boolean) props.getOrDefault("includeAll",false),
                    (String) props.getOrDefault("catLabel",gc.getClassLabelName()),
                    (String) props.getOrDefault("subCatRel",gc.getSubClassOfRelName()),
                    (String) props.getOrDefault("propLabel",gc.getDataTypePropertyLabelName()),
                    (String) props.getOrDefault("subRelRel",gc.getSubPropertyOfRelName()),
                    (String) props.getOrDefault("domainRel",gc.getDomainRelName())),
            Map.of("node", node));

    return (results.hasNext()?(List<Node>) results.next().get("props"):Collections.emptyList());
  }

  @UserFunction
  @Description(
      "n10s.inference.hasLabel(node,'label',{}) - checks whether node is explicitly or "
          + "implicitly labeled as 'label'.")
  public boolean hasLabel(
      @Name("node") Node individual,
      @Name("label") String label,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws MicroReasonerException {

    final GraphConfig gc = getGraphConfig();

    //if no graphconfig (or ontoconfig) and no required in-function params, funcion cannot be invoked
    if(gc == null && missingParams(props, "catLabel", "subCatRel","catNameProp")){
      throw new MicroReasonerException("No GraphConfig or in-function params (catLabel, subCatRel, catNameProp). Method cannot be run.");
    }

    String queryString = String.format(subcatPathQuery,
        (props.containsKey("catLabel") ? (String) props.get("catLabel") : gc.getClassLabelName()),
        (props.containsKey("catNameProp") ? (String) props.get("catNameProp")
            : gc.getClassNamePropName()),
        (props.containsKey("subCatRel") ? (String) props.get("subCatRel") : gc.getSubClassOfRelName()));

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("virtLabel", label);

    Iterable<Label> labels = individual.getLabels();
    boolean is = false;
    for (Label l : labels) {
      params.put("oneOfCats", l.name());
      is |= (l.name().equals(label) ? true
          : tx.execute(queryString, params).next().get("isTrue").equals(true));
    }

    return is;
  }

  private boolean missingParams(Map<String, Object> props, String... paramNames) {
    boolean missing = false;
    for (String param:paramNames) {
      missing |= !props.containsKey(param);
    }
    return  missing;
  }


  @UserFunction
  @Description("n10s.inference.inCategory(node, category, {}) - checks whether node is explicitly or implicitly in a category.")
  public boolean inCategory(
      @Name("node") Node individual, @Name("category") Node category,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    final GraphConfig gc = getGraphConfig();

    final String inCatRelName = (props.containsKey("inCatRel") ? (String) props.get("inCatRel")
        : getDefaultIncatRel(gc));
    final String subCatRelName = (props.containsKey("subCatRel") ? (String) props.get("subCatRel")
        : gc.getSubClassOfRelName());
    final boolean searchTopDown = (props.containsKey("searchTopDown") ? (boolean) props
        .get("searchTopDown")
        : DEFAULT_SEARCH_TOP_DOWN);

    Iterator<Relationship> relIterator = individual
        .getRelationships(Direction.OUTGOING, RelationshipType.withName(inCatRelName)).iterator();

    if (searchTopDown) {
      List<Long> catIds = getSubcatIds(category, subCatRelName, gc);
      boolean is = false;
      while (!is && relIterator.hasNext()) {
        is |= catIds.contains(relIterator.next().getEndNode().getElementId());
      }
      return is;

    } else {
      boolean is = false;
      while (!is && relIterator.hasNext()) {
        List<Long> catIds = getSuperCatIds(relIterator.next().getEndNode().getElementId(), subCatRelName, gc);
        is |= catIds.contains(category.getElementId());
      }
      return is;

    }


  }

}
