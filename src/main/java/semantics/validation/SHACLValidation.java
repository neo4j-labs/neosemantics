package semantics.validation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class SHACLValidation {

  private static final String CYPHER_TX_INFIX = " focus in $touchedNodes AND ";

  private static final String CYPHER_DATATYPE_V_PREF = "MATCH (focus:%s) WHERE ";
  private static final String CYPHER_DATATYPE_V_SUFF =
      " NOT all(x in [] +  focus.`%s` where %s x %s = x) RETURN id(focus) as nodeId, " +
          "'%s' as nodeType, '%s' as shapeId, '" + SHACL.DATATYPE_CONSTRAINT_COMPONENT + "' as propertyShape, focus.`%s` as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_RANGETYPE1_V_PREF = "MATCH (focus:%s)-[r:`%s`]->(x) WHERE ";
  private static final String CYPHER_RANGETYPE1_V_SUFF = "NOT x:`%s` RETURN id(focus) as nodeId, " +
      "'%s' as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT + "' as propertyShape, 'nodeid: ' + id(x) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_RANGETYPE2_V_PREF = "MATCH (focus:%s) WHERE ";
  private static final String CYPHER_RANGETYPE2_V_SUFF =
      "exists(focus.`%s`) RETURN id(focus) as nodeId, " +
          "'%s' as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT + "' as propertyShape, focus.`%s` as offendingValue, '%s' as propertyName, '%s' as severity";
  private static final String CYPHER_REGEX_V_PREF = "WITH $`%s` as params MATCH (focus:`%s`) WHERE ";
  private static final String CYPHER_REGEX_V_SUFF =
      "NOT all(x in [] +  focus.`%s` where toString(x) =~ params.theRegex )  RETURN id(focus) as nodeId, "
          + "'%s' as nodeType, '%s' as shapeId, '"+ SHACL.PATTERN_CONSTRAINT_COMPONENT.stringValue() +"' as propertyShape, focus.`%s` as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_VALRANGE_V_PREF = "WITH $`%s` as params MATCH (focus:%s) WHERE ";
  private static final String CYPHER_VALRANGE_V_SUFF =
      "NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN id(focus) as nodeId, "
          + "'%s' as nodeType, '%s' as shapeId, '" + SHACL.MIN_EXCLUSIVE_CONSTRAINT_COMPONENT.stringValue() + "' as propertyShape, focus.`%s` as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_CARDINALITY1_V_PREF = "WITH $`%s` as params MATCH (focus:%s) WHERE ";
//  private static final String CYPHER_CARDINALITY1_V_SUFF =
//      "NOT %s size((focus)-[:`%s`]->()) %s RETURN id(focus) as nodeId, "
//          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.NAMESPACE + "MinMaxCountConstraintComponent" + "' as propertyShape,  'value count: ' + size((focus)-[:`%s`]->()) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_MIN_CARDINALITY1_V_SUFF =
      "NOT %s size((focus)-[:`%s`]->()) RETURN id(focus) as nodeId, "
          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT + "' as propertyShape,  'value count: ' + size((focus)-[:`%s`]->()) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_MAX_CARDINALITY1_V_SUFF =
      "NOT size((focus)-[:`%s`]->()) %s  RETURN id(focus) as nodeId, "
          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT + "' as propertyShape,  'value count: ' + size((focus)-[:`%s`]->()) as offendingValue, '%s' as propertyName, '%s' as severity ";
//  private static final String CYPHER_CARDINALITY1_INVERSE_V_SUFF =
//      "NOT %s size((focus)<-[:`%s`]-()) %s RETURN id(focus) as nodeId, "
//          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.NAMESPACE + "MinMaxCountConstraintComponent" + "' as propertyShape,  'value count: ' + size((focus)<-[:`%s`]-()) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_CARDINALITY2_V_PREF = " WITH $`%s` as params MATCH (focus:%s) WHERE ";
  private static final String CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF =
      "NOT %s size((focus)<-[:`%s`]-()) RETURN id(focus) as nodeId, "
          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT + "' as propertyShape,  'value count: ' + size((focus)<-[:`%s`]-()) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF =
      "NOT size((focus)<-[:`%s`]-()) %s RETURN id(focus) as nodeId, "
          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT + "' as propertyShape,  'value count: ' + size((focus)<-[:`%s`]-()) as offendingValue, '%s' as propertyName, '%s' as severity ";
//  private static final String CYPHER_CARDINALITY2_V_SUFF =
//      " NOT %s size([] + focus.`%s`) %s RETURN id(focus) as nodeId, "
//          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.NAMESPACE + "MinMaxCountConstraintComponent" + "' as propertyShape, 'value count: ' + size([] + focus.`%s`) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_MIN_CARDINALITY2_V_SUFF =
      " NOT %s size([] + focus.`%s`) RETURN id(focus) as nodeId, "
          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT + "' as propertyShape, 'value count: ' + size([] + focus.`%s`) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_MAX_CARDINALITY2_V_SUFF =
      " NOT size([] + focus.`%s`) %s RETURN id(focus) as nodeId, "
          + " '%s' as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT + "' as propertyShape, 'value count: ' + size([] + focus.`%s`) as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_STRLEN_V_PREF = "WITH $`%s` as params MATCH (focus:%s) WHERE ";
  private static final String CYPHER_STRLEN_V_SUFF =
      "NOT all(x in [] +  focus.`%s` where %s size(toString(x)) %s ) RETURN id(focus) as nodeId, "
          + "'%s' as nodeType, '%s' as shapeId, 'stringLength' as propertyShape, focus.`%s` as offendingValue, '%s' as propertyName, '%s' as severity ";
  private static final String CYPHER_NODE_STRUCTURE_V_PREF = "WITH $`%s` as params MATCH (focus:`%s`) WHERE ";
  private static final String CYPHER_NODE_STRUCTURE_V_SUFF = " true \n" +
      "UNWIND [ x in [(focus)-[r]->()| type(r)] where not x in params.allAllowedProps] + [ x in keys(focus)  where not x in params.allAllowedProps] as noProp\n"
      +
      "WITH focus, collect(distinct noProp) as noProps\n" +
      "RETURN  id(focus) as nodeId , '%s' as nodeType, '%s' as shapeId, '" + SHACL.CLOSED_CONSTRAINT_COMPONENT.stringValue() + "' as propertyShape, 'exists' as offendingValue, "
      +
      "reduce(result='', x in noProps | result + ' ' + x ) as propertyName, '%s' as severity";

  //A property shape is a shape in the shapes graph that is the subject of a triple that has sh:path as its predicate.
  private static final String CYPHER_PROP_VALIDATIONS =
      "MATCH (ps:Resource)-[:sh__path]->()-[inv:sh__inversePath*0..1]->(rel) "
      + "WHERE NOT (rel)-->() "
      + "OPTIONAL MATCH (ps)-[:sh__class]->(rangeClass) "
      + "OPTIONAL MATCH (ps)-[:sh__nodeKind]->(rangeKind) "
      + "OPTIONAL MATCH (ps)-[:sh__datatype]->(datatype) "
      + "OPTIONAL MATCH (ps)-[:sh__severity]->(severity) "
      + "OPTIONAL MATCH (targetClass)<-[:sh__targetClass]-(ns)-[:sh__property]->(ps) "
      + "OPTIONAL MATCH (nsAsTarget:rdfs__Class)-[:sh__property]->(ps) "
      + "RETURN semantics.getIRILocalName(rel.uri) AS item, inv <> []  AS inverse, "
      + "       semantics.getIRILocalName(coalesce(rangeClass.uri,'#')) AS rangeType, "
      + "       semantics.getIRILocalName(coalesce(rangeKind.uri,'#')) AS rangeKind, "
      + "       datatype.uri AS dataType, "
      + "       semantics.getIRILocalName(coalesce(coalesce(targetClass.uri, nsAsTarget.uri),'#'))  AS appliesToCat,"
      + "       ps.sh__pattern AS pattern,"
      + "       ps.sh__maxCount AS maxCount,"
      + "       ps.sh__minCount AS minCount, ps.sh__minInclusive AS minInc, "
      + "       ps.sh__maxInclusive AS maxInc, ps.sh__minExclusive AS minExc, ps.sh__maxExclusive AS maxExc,"
      + "       ps.sh__minLength AS minStrLen, ps.sh__maxLength AS maxStrLen , ps.uri AS propShapeUid ,"
      + "       coalesce(severity.uri,'http://www.w3.org/ns/shacl#Violation') AS severity";

  private static final String CYPHER_NODE_VALIDATIONS =
      "MATCH (ns:Resource { sh__closed : true })\n" +
          "WITH ns, (CASE WHEN 'rdfs__Class' IN labels(ns) THEN  [semantics.getIRILocalName(ns.uri)] ELSE [] END  + "
          + " [(ns)-[:sh__targetClass]-(target) | semantics.getIRILocalName(target.uri)])[0] AS target "
          + " RETURN target, [ (ns)-[:sh__property]->()-[:sh__path]->(x)  WHERE NOT (x)-->() | semantics.getIRILocalName(x.uri)] + "
          + " [(ns)-[:sh__ignoredProperties]->()-[:rdf__first|rdf__rest*0..]->(prop) "
          + " WHERE ()-[:rdf__first]->(prop) | semantics.getIRILocalName(prop.uri) ] AS allAllowedProps "
          + " , ns.uri AS nodeShapeUid ";


  @Context
  public GraphDatabaseService db;
  @Context
  public Log log;

  @Procedure(mode = Mode.READ)
  @Description("semantics.validation.shaclValidateTx() - runs SHACL validation on selected nodes")
  public Stream<ValidationResult> shaclValidateTx(@Name("nodeList") List<Node> touchedNodes,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    //TODO: check if passing ids is any better
    return runValidations(touchedNodes);
  }


  @Procedure(mode = Mode.READ)
  @Description("semantics.validation.shaclValidate() - runs SHACL validation on the whole graph.")
  public Stream<ValidationResult> shaclValidate(
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return runValidations(null);
  }

//  @Procedure(mode = Mode.READ)
//  @Description("semantics.validation.listActiveShaclShapes() - lists SHACL shapes.")
//  public Stream<SHACLShape> listShaclShapes(
//      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
//
//    return listShapes();
//  }
//
//  private Stream<SHACLShape> listShapes() {
//    return Stream.empty();
//  }

  @Procedure(mode = Mode.READ)
  @Description("semantics.validation.triggerSHACLValidateTx() - runs SHACL validation in trigger context.")
  public Stream<ValidationResult> shaclValidateTxForTrigger(
      @Name("createdNodes") Object createdNodes,
      @Name("createdRelationships") Object createdRelationships,
      @Name("assignedLabels") Object assignedLabels, @Name("removedLabels") Object removedLabels,
      @Name("assignedNodeProperties") Object assignedNodeProperties,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    Map<String, Object> params = new HashMap<>();
    params.put("createdNodes", createdNodes);
    params.put("createdRelationships", createdRelationships);
    params.put("assignedLabels", assignedLabels);
    params.put("removedLabels", removedLabels);
    params.put("assignedNodeProperties", assignedNodeProperties);
    Result validationResults = db.execute(
        "UNWIND reduce(nodes = [], x IN keys($removedLabels) | nodes + $removedLabels[x]) AS rln MATCH (rln)<--(x) WITH collect(DISTINCT x) AS sn UNWIND sn + $createdNodes + [x IN $createdRelationships | startNode(x)] + reduce( nodes = [] , x IN keys($assignedLabels) | nodes + $assignedLabels[x]) + reduce( nodes = [] , x IN keys($assignedNodeProperties) | nodes + [ item IN $assignedNodeProperties[x] | item.node] ) AS nd WITH collect( DISTINCT nd) AS touchedNodes\n"
            + "CALL semantics.validation.shaclValidateTx(touchedNodes) YIELD nodeId, nodeType, shapeId, propertyShape, offendingValue, propertyName\n"
            + "RETURN {nodeId: nodeId, nodeType: nodeType, shapeId: shapeId, propertyShape: propertyShape, offendingValue: offendingValue, propertyName:propertyName} AS validationResult ",
        params);
    if(validationResults.hasNext()){
      throw new SHACLValidationException(validationResults.next().toString());
    }

    return Stream.empty();
  }


  private Stream<ValidationResult> runValidations(List<Node> nodeList) {

    Result propertyValidations = db.execute(CYPHER_PROP_VALIDATIONS);

    Map<String, Object> allParams = new HashMap<>();
    allParams.put("touchedNodes", nodeList);

    StringBuilder cypherUnion = new StringBuilder("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity ");

    while (propertyValidations.hasNext()) {

      Map<String, Object> validation = propertyValidations.next();
      String focusLabel = (String) validation.get("appliesToCat");
      String propOrRel = (String) validation.get("item");
      String severity = (String) validation.get("severity");

      if (validation.get("dataType") != null) {
        addDataTypeValidations(allParams, cypherUnion, focusLabel, propOrRel, severity, validation,
            nodeList != null);
      }
      if (validation.get("rangeType") != null && !validation.get("rangeType").equals("")) {
        addRangeTypeValidations(allParams, cypherUnion, focusLabel, propOrRel, severity, validation,
            nodeList != null);
      }
      if (validation.get("pattern") != null) {
        addRegexValidations(allParams, cypherUnion, focusLabel, propOrRel, severity, validation,
            nodeList != null);
      }
      if (validation.get("minCount") != null) {
        addMinCardinalityValidations(allParams, cypherUnion, focusLabel, propOrRel, severity, validation,
            nodeList != null);
      }
      if (validation.get("maxCount") != null) {
        addMaxCardinalityValidations(allParams, cypherUnion, focusLabel, propOrRel, severity, validation,
            nodeList != null);
      }
      if (validation.get("minStrLen") != null || validation.get("maxStrLen") != null) {
        addStrLenValidations(allParams, cypherUnion, focusLabel, propOrRel, severity, validation,
            nodeList != null);
      }
      if (validation.get("minInc") != null || validation.get("maxInc") != null
          || validation.get("minExc") != null || validation.get("maxExc") != null) {
        addValueRangeValidations(allParams, cypherUnion, focusLabel, propOrRel, severity, validation,
            nodeList != null);
      }

    }

    Result nodeValidations = db.execute(CYPHER_NODE_VALIDATIONS);


    while (nodeValidations.hasNext()) {
      Map<String, Object> validation = nodeValidations.next();
      String focusLabel = (String) validation.get("target");
      //String severity = (String) validation.get("severity");
      //AFAIK there's no way to set severity on these type of constraints??? TODO: confirm
      addNodeStructureValidations(allParams, cypherUnion, focusLabel, "http://www.w3.org/ns/shacl#Violation", validation, nodeList != null);
    }

    return db.execute(cypherUnion.toString(), allParams).stream().map(ValidationResult::new);
  }

  private void addNodeStructureValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String severity, Map<String, Object> validation, boolean tx) {
    String paramSetId = validation.get("nodeShapeUid") + "_" + SHACL.CLOSED.stringValue();
    Map<String, Object> params = addParams(allParams, paramSetId);
    params.put("allAllowedProps", validation.get("allAllowedProps"));

    addCypher(cypherUnion, getNodeStructureViolationQuery(tx), paramSetId, appliesToCat,
        appliesToCat,
        (String) validation.get("nodeShapeUid"), severity);
  }

  private Map<String, Object> addParams(Map<String, Object> allParams, String id) {
    Map<String, Object> params = new HashMap<>();
    allParams.put(id, params);
    return params;
  }

  private void addValueRangeValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {

    String paramSetId = validation.get("propShapeUid") + "_" + SHACL.MIN_EXCLUSIVE.stringValue();
    Map<String, Object> params = addParams(allParams, paramSetId);
    params.put("min",
        validation.get("minInc") != null ? validation.get("minInc") : validation.get("minExc"));
    params.put("max",
        validation.get("maxInc") != null ? validation.get("maxInc") : validation.get("maxExc"));

    addCypher(cypherUnion, getValueRangeViolationQuery(tx), paramSetId, appliesToCat, item,
        validation.get("minInc") != null ? " params.min <="
            : (validation.get("minExc") != null ? " params.min < " : ""),
        validation.get("maxInc") != null ? " <= params.max "
            : (validation.get("maxExc") != null ? " < params.max " : ""),
        appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
  }

  private void addStrLenValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {

    String paramSetId = validation.get("propShapeUid") + "_" + SHACL.MIN_LENGTH.stringValue();
    Map<String, Object> params = addParams(allParams, paramSetId);
    params.put("minStrLen", validation.get("minStrLen"));
    params.put("maxStrLen", validation.get("maxStrLen"));

    addCypher(cypherUnion, getStrLenViolationQuery(tx), paramSetId, appliesToCat,
        item,
        validation.get("minStrLen") != null ? " params.minStrLen <= " : "",
        validation.get("maxStrLen") != null ? " <= params.maxStrLen " : "",
        appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
  }

//  private void addCardinalityValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
//      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {
//
//    String paramSetId = validation.get("propShapeUid") + "_" + SHACL.MIN_COUNT.stringValue();
//    Map<String, Object> params = addParams(allParams, paramSetId);
//    params.put("minCount", validation.get("minCount"));
//    params.put("maxCount", validation.get("maxCount"));
//
//    if(!(boolean)validation.get("inverse")) {
//
//      //Old version combining min and max
//      addCypher(cypherUnion, getCardinality1ViolationQuery(tx), paramSetId, appliesToCat,
//          validation.get("minCount") != null ? " params.minCount <= " : "",
//          item,
//          validation.get("maxCount") != null ? " <= params.maxCount " : "",
//          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
//      addCypher(cypherUnion, getCardinality2ViolationQuery(tx), paramSetId, appliesToCat,
//          validation.get("minCount") != null ? " params.minCount <= " : "",
//          item,
//          validation.get("maxCount") != null ? " <= params.maxCount " : "",
//          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
//    } else {
//      // multivalued attributes not checked for cardinality in the case of inverse??
//      // does not make sense in an LPG
//      addCypher(cypherUnion, getCardinality1InverseViolationQuery(tx), paramSetId, appliesToCat,
//          validation.get("minCount") != null ? " params.minCount <= " : "",
//          item,
//          validation.get("maxCount") != null ? " <= params.maxCount " : "",
//          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
//    }
//  }

  private void addMinCardinalityValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {

    String paramSetId = validation.get("propShapeUid") + "_" + SHACL.MIN_COUNT.stringValue();
    Map<String, Object> params = addParams(allParams, paramSetId);
    params.put("minCount", validation.get("minCount"));

    if(!(boolean)validation.get("inverse")) {

      addCypher(cypherUnion, getMinCardinality1ViolationQuery(tx), paramSetId, appliesToCat,
          " params.minCount <= " ,
          item,
          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
      addCypher(cypherUnion, getMinCardinality2ViolationQuery(tx), paramSetId, appliesToCat,
          " params.minCount <= ",
          item,
          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
    } else {
      // multivalued attributes not checked for cardinality in the case of inverse??
      // does not make sense in an LPG
      addCypher(cypherUnion, getMinCardinality1InverseViolationQuery(tx), paramSetId, appliesToCat,
          " params.minCount <= ",
          item,
          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
    }
  }

  private void addMaxCardinalityValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {

    String paramSetId = validation.get("propShapeUid") + "_" + SHACL.MAX_COUNT.stringValue();
    Map<String, Object> params = addParams(allParams, paramSetId);
    params.put("maxCount", validation.get("maxCount"));

    if(!(boolean)validation.get("inverse")) {

      addCypher(cypherUnion, getMaxCardinality1ViolationQuery(tx), paramSetId, appliesToCat,
          item,
          " <= params.maxCount ",
          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
      addCypher(cypherUnion, getMaxCardinality2ViolationQuery(tx), paramSetId, appliesToCat,
          item,
          " <= params.maxCount ",
          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
    } else {
      // multivalued attributes not checked for cardinality in the case of inverse??
      // does not make sense in an LPG
      addCypher(cypherUnion, getMaxCardinality1InverseViolationQuery(tx), paramSetId, appliesToCat,
          item,
          " <= params.maxCount ",
          appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
    }
  }

  private void addRegexValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {
    String paramSetId = validation.get("propShapeUid") + "_" + SHACL.PATTERN.stringValue();
    Map<String, Object> params = addParams(allParams, paramSetId);
    params.put("theRegex", validation.get("pattern"));
    addCypher(cypherUnion, getRegexViolationQuery(tx), paramSetId, appliesToCat, item, appliesToCat,
        (String) validation.get("propShapeUid"), item, item, severity);
  }

  private void addRangeTypeValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {

    addCypher(cypherUnion, getRangeType1ViolationQuery(tx), appliesToCat, item,
        (String) validation.get("rangeType"),
        appliesToCat, (String) validation.get("propShapeUid"), item, severity);
    addCypher(cypherUnion, getRangeType2ViolationQuery(tx), appliesToCat, item,
        appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);
  }

  private void addDataTypeValidations(Map<String, Object> allParams, StringBuilder cypherUnion,
      String appliesToCat, String item, String severity, Map<String, Object> validation, boolean tx) {

    //TODO: this will be safer via APOC? maybe exclude some of them? and log the ignored ones?
    addCypher(cypherUnion, getDataTypeViolationQuery(tx), appliesToCat, item,
        getDatatypeCastExpressionPref((String) validation.get("dataType")),
        getDatatypeCastExpressionSuff((String) validation.get("dataType")),
        appliesToCat, (String) validation.get("propShapeUid"), item, item, severity);

    //TODO: Complete all datatypes: spatial type Point, Temporal types: Date, Time, LocalTime, DateTime, LocalDateTime and Duration

  }

  private String getDatatypeCastExpressionPref(String dataType) {
    if (dataType.equals(XMLSchema.BOOLEAN.stringValue())) {
      return "toBoolean(toString(";
    } else if (dataType.equals(XMLSchema.STRING.stringValue())) {
      return "toString(";
    } else if (dataType.equals(XMLSchema.INTEGER.stringValue())) {
      return "toInteger(";
    } else if (dataType.equals(XMLSchema.FLOAT.stringValue())) {
      return "toFloat(";
    } else if (dataType.equals(XMLSchema.DATE.stringValue())) {
      return "date(";
    } else if (dataType.equals(XMLSchema.DATETIME.stringValue())) {
      return "datetime(";
    } else {
      return "";
    }
  }

  private String getDatatypeCastExpressionSuff(String dataType) {
    if (dataType.equals(XMLSchema.BOOLEAN.stringValue())) {
      return "))";
    } else if (dataType.equals(XMLSchema.STRING.stringValue())) {
      return ")";
    } else if (dataType.equals(XMLSchema.INTEGER.stringValue())) {
      return ")";
    } else if (dataType.equals(XMLSchema.FLOAT.stringValue())) {
      return ")";
    } else if (dataType.equals(XMLSchema.DATE.stringValue())) {
      return ")";
    } else if (dataType.equals(XMLSchema.DATETIME.stringValue())) {
      return ")";
    } else {
      return "";
    }
  }

  private void addCypher(StringBuilder cypherUnion, String querystr, String... args) {
    cypherUnion.append("\n UNION \n").append(String.format(querystr, args));
  }

  private String getDataTypeViolationQuery(boolean tx) {
    return getQuery(CYPHER_DATATYPE_V_PREF, tx, CYPHER_DATATYPE_V_SUFF);
  }

  private String getRangeType1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_RANGETYPE1_V_PREF, tx, CYPHER_RANGETYPE1_V_SUFF);
  }

  private String getRangeType2ViolationQuery(boolean tx) {
    return getQuery(CYPHER_RANGETYPE2_V_PREF, tx, CYPHER_RANGETYPE2_V_SUFF);
  }

  private String getRegexViolationQuery(boolean tx) {
    return getQuery(CYPHER_REGEX_V_PREF, tx, CYPHER_REGEX_V_SUFF);
  }

//  private String getCardinality1ViolationQuery(boolean tx) {
//    return getQuery(CYPHER_CARDINALITY1_V_PREF, tx, CYPHER_CARDINALITY1_V_SUFF);
//  }

//  private String getCardinality1InverseViolationQuery(boolean tx) {
//    return getQuery(CYPHER_CARDINALITY1_V_PREF, tx, CYPHER_CARDINALITY1_INVERSE_V_SUFF);
//  }

//  private String getCardinality2ViolationQuery(boolean tx) {
//    return getQuery(CYPHER_CARDINALITY2_V_PREF, tx, CYPHER_CARDINALITY2_V_SUFF);
//  }

  private String getMinCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_CARDINALITY1_V_PREF, tx, CYPHER_MIN_CARDINALITY1_V_SUFF);
  }

  private String getMinCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_CARDINALITY1_V_PREF, tx, CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF);
  }

  private String getMinCardinality2ViolationQuery(boolean tx) {
    return getQuery(CYPHER_CARDINALITY2_V_PREF, tx, CYPHER_MIN_CARDINALITY2_V_SUFF);
  }

  private String getMaxCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_CARDINALITY1_V_PREF, tx, CYPHER_MAX_CARDINALITY1_V_SUFF);
  }

  private String getMaxCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_CARDINALITY1_V_PREF, tx, CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF);
  }

  private String getMaxCardinality2ViolationQuery(boolean tx) {
    return getQuery(CYPHER_CARDINALITY2_V_PREF, tx, CYPHER_MAX_CARDINALITY2_V_SUFF);
  }

  private String getStrLenViolationQuery(boolean tx) {
    return getQuery(CYPHER_STRLEN_V_PREF, tx, CYPHER_STRLEN_V_SUFF);
  }

  private String getValueRangeViolationQuery(boolean tx) {
    return getQuery(CYPHER_VALRANGE_V_PREF, tx, CYPHER_VALRANGE_V_SUFF);
  }

  private String getNodeStructureViolationQuery(boolean tx) {
    return getQuery(CYPHER_NODE_STRUCTURE_V_PREF, tx, CYPHER_NODE_STRUCTURE_V_SUFF);
  }

  private String getQuery(String pref, boolean tx, String suff) {
    return pref + (tx ? CYPHER_TX_INFIX : "") + suff;
  }

}
