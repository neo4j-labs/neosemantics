package n10s.validation;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_MODE_LPG;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_KEEP;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_MAP;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN_STRICT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

public class SHACLValidator {

  private static final String CYPHER_TX_INFIX = " focus in $touchedNodes AND ";

  private static final String CYPHER_MATCH_WHERE = "MATCH (focus:`%s`) WHERE ";
  private static final String CYPHER_MATCH_REL_WHERE =  "MATCH (focus:`%s`)-[r:`%s`]->(x) WHERE ";
  private static final String CYPHER_WITH_PARAMS_MATCH_WHERE = "WITH $`%s` as params MATCH (focus:`%s`) WHERE ";

  //A property shape is a shape in the shapes graph that is the subject of a triple that has sh:path as its predicate.
  private static final String CYPHER_PROP_CONSTRAINTS =
      "MATCH (ns:sh__NodeShape)-[:sh__property]->(ps)-[:sh__path]->()-[inv:sh__inversePath*0..1]->(rel)\n"
          + "          WHERE NOT (rel)-->() \n"
          + "          MATCH (ps)-[rcr:sh__class*0..1]->(rangeClass) \n"
          + "          MATCH (ps)-[nkr:sh__nodeKind*0..1]->(rangeKind) \n"
          + "          MATCH (ps)-[dtr:sh__datatype*0..1]->(datatype) \n"
          + "          MATCH (ps)-[sr:sh__severity*0..1]->(severity) \n"
          + "          OPTIONAL MATCH (targetClass)<-[:sh__targetClass]-(ns)\n"
          + "          OPTIONAL MATCH (nsAsTarget:rdfs__Class)-[:sh__property]->(ps) \n"
          + "          \n"
          + "with  rel.uri AS item, inv <> []  AS inverse, \n"
          + "      case when rcr=[] then null else rangeClass.uri end AS rangeType, \n"
          + "      case when nkr=[] then null else rangeKind.uri end AS rangeKind, \n"
          + "      case when dtr=[] then null else datatype.uri end AS dataType, \n"
          + "      coalesce(targetClass.uri, nsAsTarget.uri) AS appliesToCat,\n"
          + "      ps.sh__pattern AS pattern,\n"
          + "      ps.sh__maxCount AS maxCount,\n"
          + "      ps.sh__minCount AS minCount, ps.sh__minInclusive AS minInc, \n"
          + "      ps.sh__maxInclusive AS maxInc, ps.sh__minExclusive AS minExc, ps.sh__maxExclusive AS maxExc,\n"
          + "      ps.sh__minLength AS minStrLen, ps.sh__maxLength AS maxStrLen , ps.uri AS propShapeUid, \n"
          + "      case when sr=[] then null else severity.uri end AS severity\n"
          + "                 \n"
          + "RETURN  item, collect(distinct appliesToCat)[0] as appliesToCat,\n"
          + "        collect(distinct inverse)[0]  as inverse,\n"
          + "        [x in collect(distinct rangeType) ][0] as rangeType,\n"
          + "        [x in collect(distinct rangeKind) ][0] as rangeKind,\n"
          + "        [x in collect(distinct dataType) ][0] as dataType,\n"
          + "        collect(distinct pattern)[0] as pattern,\n"
          + "        collect(distinct maxCount)[0] as maxCount,\n"
          + "        collect(distinct minCount)[0] as minCount,\n"
          + "        collect(distinct minInc)[0] as minInc,\n"
          + "        collect(distinct minExc)[0] as minExc,\n"
          + "        collect(distinct maxExc)[0] as maxExc,\n"
          + "        collect(distinct maxInc)[0] as maxInc,\n"
          + "        collect(distinct minStrLen)[0] as minStrLen,\n"
          + "        collect(distinct maxStrLen)[0] as maxStrLen,\n"
          + "        collect(distinct propShapeUid)[0] as propShapeUid,\n"
          + "        coalesce(collect(distinct severity)[0],'http://www.w3.org/ns/shacl#Violation') as severity";

  private static final String CYPHER_NODE_VALIDATIONS =
      "MATCH (ns:sh__NodeShape { sh__closed : true }) \n"
          + "WITH ns, (CASE WHEN 'rdfs__Class' IN labels(ns) THEN  [ns.uri] ELSE [] END  + \n"
          + "  [(ns)-[:sh__targetClass]-(target) | target.uri])[0] AS target \n"
          + "  RETURN target, [ (ns)-[:sh__property]->()-[:sh__path]->(x)  WHERE NOT (x)-->() | x.uri] + \n"
          + " [(ns)-[:sh__ignoredProperties]->()-[:rdf__first|rdf__rest*0..]->(prop) \n"
          + " WHERE ()-[:rdf__first]->(prop) | prop.uri ] AS allAllowedProps \n"
          + " , ns.uri AS nodeShapeUid";


  private Transaction tx;
  private Log log;
  private GraphConfig gc;
  private StringBuilder cypherU;
  private Map<String, Object> paramsU;

  public SHACLValidator(Transaction transaction,Log l) {
    this.tx = transaction;
    this.log = l;
    try {
      this.gc = new GraphConfig(tx);
    } catch (GraphConfigNotFound graphConfigNotFound) {
      //valid when it's a pure LPG
      this.gc  =null;
    }
    cypherU = new StringBuilder("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity , '' as message ");
    paramsU = new HashMap<>();

  }

  protected Stream<ValidationResult> runValidations(List<Node> nodeList) {

    paramsU.put("touchedNodes", nodeList);

    Result propertyConstraints = tx.execute(CYPHER_PROP_CONSTRAINTS);

    while (propertyConstraints.hasNext()) {

      Map<String, Object> propConstraint = propertyConstraints.next();
      if (propConstraint.get("appliesToCat") == null) {

        log.info("Only class-based targets (sh:targetClass) and implicit class targets are validated.");

      } else {

        String focusLabel = translateUri((String) propConstraint.get("appliesToCat"));
        String propOrRel = translateUri((String) propConstraint.get("item"));
        String severity = (String) propConstraint.get("severity");

        if (propConstraint.get("dataType") != null) {
          //TODO: this will be safer via APOC? maybe exclude some of them? and log the ignored ones?
          addCypherToValidationScript(getDataTypeViolationQuery(nodeList != null), focusLabel,
              propOrRel,
              getDatatypeCastExpressionPref((String) propConstraint.get("dataType")),
              getDatatypeCastExpressionSuff((String) propConstraint.get("dataType")),
              focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
              severity);

          //TODO: Complete all datatypes: spatial type Point, Temporal types: Date, Time, LocalTime, DateTime, LocalDateTime and Duration
        }

        if (propConstraint.get("rangeType") != null && !propConstraint.get("rangeType")
            .equals("")) {
          addCypherToValidationScript(getRangeType1ViolationQuery(nodeList != null), focusLabel,
              propOrRel,
              translateUri((String) propConstraint.get("rangeType")),
              focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, severity,
              translateUri((String) propConstraint.get("rangeType")));
          addCypherToValidationScript(getRangeType2ViolationQuery(nodeList != null), focusLabel,
              propOrRel,
              focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, severity,
              propOrRel);
        }

        if (propConstraint.get("pattern") != null) {
          String paramSetId =
              propConstraint.get("propShapeUid") + "_" + SHACL.PATTERN.stringValue();
          Map<String, Object> params = createNewSetOfParams(paramSetId);
          params.put("theRegex", (String) propConstraint.get("pattern"));
          addCypherToValidationScript(getRegexViolationQuery(nodeList != null), paramSetId,
              focusLabel, propOrRel, propOrRel, focusLabel,
              (String) propConstraint.get("propShapeUid"), propOrRel, severity);

        }

        if (propConstraint.get("minCount") != null) {
          String paramSetId =
              propConstraint.get("propShapeUid") + "_" + SHACL.MIN_COUNT.stringValue();
          Map<String, Object> params = createNewSetOfParams(paramSetId);
          params.put("minCount", propConstraint.get("minCount"));

          if (!(boolean) propConstraint.get("inverse")) {

            addCypherToValidationScript(getMinCardinality1ViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                " params.minCount <= ",
                propOrRel,
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
                severity);
            addCypherToValidationScript(getMinCardinality2ViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                " params.minCount <= ",
                propOrRel,
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
                severity);
          } else {
            // multivalued attributes not checked for cardinality in the case of inverse??
            // does not make sense in an LPG
            addCypherToValidationScript(getMinCardinality1InverseViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                " params.minCount <= ",
                propOrRel,
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
                severity);
          }
        }

        if (propConstraint.get("maxCount") != null) {
          String paramSetId =
              propConstraint.get("propShapeUid") + "_" + SHACL.MAX_COUNT.stringValue();
          Map<String, Object> params = createNewSetOfParams(paramSetId);
          params.put("maxCount", propConstraint.get("maxCount"));

          if (!(boolean) propConstraint.get("inverse")) {

            addCypherToValidationScript(getMaxCardinality1ViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                propOrRel,
                " <= params.maxCount ",
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
                severity);
            addCypherToValidationScript(getMaxCardinality2ViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                propOrRel,
                " <= params.maxCount ",
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
                severity);
          } else {
            // multivalued attributes not checked for cardinality in the case of inverse??
            // does not make sense in an LPG
            addCypherToValidationScript(getMaxCardinality1InverseViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                propOrRel,
                " <= params.maxCount ",
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
                severity);
          }
        }

        if (propConstraint.get("minStrLen") != null || propConstraint.get("maxStrLen") != null) {

          String paramSetId =
              propConstraint.get("propShapeUid") + "_" + SHACL.MIN_LENGTH.stringValue();
          Map<String, Object> params = createNewSetOfParams(paramSetId);
          params.put("minStrLen", propConstraint.get("minStrLen"));
          params.put("maxStrLen", propConstraint.get("maxStrLen"));

          addCypherToValidationScript(getStrLenViolationQuery(nodeList != null), paramSetId,
              focusLabel,
              propOrRel,
              propConstraint.get("minStrLen") != null ? " params.minStrLen <= " : "",
              propConstraint.get("maxStrLen") != null ? " <= params.maxStrLen " : "",
              focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
              severity);

        }

        if (propConstraint.get("minInc") != null || propConstraint.get("maxInc") != null
            || propConstraint.get("minExc") != null || propConstraint.get("maxExc") != null) {

          String paramSetId =
              propConstraint.get("propShapeUid") + "_" + SHACL.MIN_EXCLUSIVE.stringValue();
          Map<String, Object> params = createNewSetOfParams(paramSetId);
          params.put("min",
              propConstraint.get("minInc") != null ? propConstraint.get("minInc")
                  : propConstraint.get("minExc"));
          params.put("max",
              propConstraint.get("maxInc") != null ? propConstraint.get("maxInc")
                  : propConstraint.get("maxExc"));

          addCypherToValidationScript(getValueRangeViolationQuery(nodeList != null), paramSetId,
              focusLabel, propOrRel,
              propConstraint.get("minInc") != null ? " params.min <="
                  : (propConstraint.get("minExc") != null ? " params.min < " : ""),
              propConstraint.get("maxInc") != null ? " <= params.max "
                  : (propConstraint.get("maxExc") != null ? " < params.max " : ""),
              focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,
              severity);
        }

      }
    }

    Result nodeConstraints = tx.execute(CYPHER_NODE_VALIDATIONS);

    while (nodeConstraints.hasNext()) {
      Map<String, Object> nodeConstraint = nodeConstraints.next();
      String focusLabel = translateUri((String) nodeConstraint.get("target"));

      //String severity = (String) validation.get("severity");
      //AFAIK there's no way to set severity on these type of constraints??? TODO: confirm
      String paramSetId = nodeConstraint.get("nodeShapeUid") + "_" + SHACL.CLOSED.stringValue();
      Map<String, Object> params = createNewSetOfParams(paramSetId);
      List<String> allowedPropsTranslated = new ArrayList<>();
      if(nodeConstraint.get("allAllowedProps") != null){
        for (String uri:(List<String>)nodeConstraint.get("allAllowedProps")) {
          allowedPropsTranslated.add(translateUri(uri));
        }
      }
      params.put("allAllowedProps", allowedPropsTranslated);

      addCypherToValidationScript(getNodeStructureViolationQuery(nodeList != null), paramSetId, focusLabel,
          focusLabel,
          (String) nodeConstraint.get("nodeShapeUid"), "http://www.w3.org/ns/shacl#Violation");

    }

    //TODO: Remove  this
    //log.info(cypherU.toString());
    //log.info(paramsU.toString());

    return tx.execute(cypherU.toString(), paramsU).stream().map(ValidationResult::new);
  }

  private String translateUri(String uri) {
    if( gc == null || gc.getGraphMode()==GRAPHCONF_MODE_LPG){
      return uri.substring(URIUtil.getLocalNameIndex(uri));
    } else if(gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN_STRICT||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_MAP){
        Map<String, Object> params = new HashMap<>();
        params.put("uri",uri);
        return (String)tx.execute("return n10s.rdf.shortFormFromFullUri($uri) as shortenedUri", params).next().get("shortenedUri");
    } else {
      //it's GRAPHCONF_VOC_URI_KEEP
      return uri;
    }
  }

  private Map<String, Object> createNewSetOfParams(String id) {
    Map<String, Object> params = new HashMap<>();
    paramsU.put(id, params);
    return params;
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

  private void addCypherToValidationScript(String querystr, String... args) {
    cypherU.append("\n UNION \n").append(String.format(querystr, args));
  }

  private String getDataTypeViolationQuery(boolean tx ) {
    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_DATATYPE_V_SUFF());
  }

  private String getRangeType1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_REL_WHERE, tx, CYPHER_RANGETYPE1_V_SUFF());
  }

  private String getRangeType2ViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_RANGETYPE2_V_SUFF());
  }

  private String getRegexViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_REGEX_V_SUFF());
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
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MIN_CARDINALITY1_V_SUFF());
  }

  private String getMinCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF());
  }

  private String getMinCardinality2ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MIN_CARDINALITY2_V_SUFF());
  }

  private String getMaxCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY1_V_SUFF());
  }

  private String getMaxCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF());
  }

  private String getMaxCardinality2ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY2_V_SUFF());
  }

  private String getStrLenViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_STRLEN_V_SUFF());
  }

  private String getValueRangeViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_VALRANGE_V_SUFF());
  }

  private String getNodeStructureViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_NODE_STRUCTURE_V_SUFF());
  }


  private boolean shallIUseUriInsteadOfId() {
    return gc!=null &&  (gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN_STRICT||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_MAP ||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_KEEP) ;
  }

  private boolean shallIShorten() {
    return gc!=null &&  (gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN_STRICT||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_MAP) ;
  }

  private String getQuery(String pref, boolean tx, String suff) {
    return pref + (tx ? CYPHER_TX_INFIX : "") + suff;
  }

  private String CYPHER_DATATYPE_V_SUFF() {
    return " NOT all(x in [] +  focus.`%s` where %s x %s = x) RETURN " +
          (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
          + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
          " as nodeType, '%s' as shapeId, '" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
          + "' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity,"
        + " '' as message ";
  }

  private String CYPHER_RANGETYPE1_V_SUFF() {
    return "NOT x:`%s` RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT
        + "' as propertyShape, " + (shallIUseUriInsteadOfId()?" x.uri ":" id(x) ") +" as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity,"
        + " 'value should be of type ' + " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as message  ";
  }

  private String CYPHER_RANGETYPE2_V_SUFF() {
    return "exists(focus.`%s`) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'%s should be a relationship but it is a property' as message  ";
  }

  private String CYPHER_REGEX_V_SUFF() {
    return "NOT all(x in [] +  coalesce(focus.`%s`,[]) where toString(x) =~ params.theRegex )  "
        + " UNWIND [x in [] +  coalesce(focus.`%s`,[]) where not toString(x) =~ params.theRegex ]  as offval "
        + "RETURN "
        + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.PATTERN_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, offval as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'' as message  ";
  }

  private String CYPHER_VALRANGE_V_SUFF() {
    return "NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
        " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_EXCLUSIVE_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'' as message  ";
  }

  private String CYPHER_MIN_CARDINALITY1_V_SUFF() {
    return "NOT %s size((focus)-[:`%s`]->()) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable value count: ' + size((focus)-[:`%s`]->()) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MAX_CARDINALITY1_V_SUFF() {
    return "NOT size((focus)-[:`%s`]->()) %s  RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, "  + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable  value count: ' + size((focus)-[:`%s`]->()) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF() {
    return "NOT %s size((focus)<-[:`%s`]-()) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable value count: ' + size((focus)<-[:`%s`]-()) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF() {
    return "NOT size((focus)<-[:`%s`]-()) %s RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
     " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unacceptable value count: ' + size((focus)<-[:`%s`]-()) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MIN_CARDINALITY2_V_SUFF() {
    return " NOT %s size([] + focus.`%s`) RETURN "  + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    "as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape, 'unacceptable value count: ' + size([] + focus.`%s`) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }
  private String CYPHER_MAX_CARDINALITY2_V_SUFF() {
    return " NOT size([] + focus.`%s`) %s RETURN "  + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape, 'unacceptable value count: ' + size([] + focus.`%s`) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_STRLEN_V_SUFF() {
    return "NOT all(x in [] +  focus.`%s` where %s size(toString(x)) %s ) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
        " as nodeType, '%s' as shapeId, 'stringLength' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'' as message  ";
  }

  private String CYPHER_NODE_STRUCTURE_V_SUFF() {
    return " true \n" +
        "UNWIND [ x in [(focus)-[r]->()| type(r)] where not x in params.allAllowedProps] + [ x in keys(focus) where " +
        (shallIUseUriInsteadOfId()?" x <> 'uri' and ":"")  +" not x in params.allAllowedProps] as noProp\n"
        + "RETURN  " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
        " as nodeId , " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '"
        + SHACL.CLOSED_CONSTRAINT_COMPONENT.stringValue()
        + "' as propertyShape, substring(reduce(result='', x in [] + coalesce(focus[noProp],[(focus)-[r]-(x) where type(r)=noProp | " + (shallIUseUriInsteadOfId()?" x.uri ":" id(x) ") + "]) | result + ', ' + x ),2) as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm(noProp)": " noProp ") +
        " as propertyName, '%s' as severity, "
        + "'Closed type does not include this property/relationship' as message  ";
  }

  public String getListConstraintsQuery() {

     return "call {\n"
          + "MATCH  (ns:sh__NodeShape)-[:sh__property]->(ps)-[:sh__path]->(path)-[inv:sh__inversePath*0..1]->(rel:Resource)\n"
          + "WHERE NOT (rel)-->() // no multihop rel (what happens with alternatives? -> //TODO)\n"
          + "WITH coalesce(([(targetClass)<-[:sh__targetClass]-(ns)| targetClass] + [(nsAsTarget:rdfs__Class)-[:sh__property]->(ps) | nsAsTarget])[0].uri,'#') AS category, rel.uri AS propertyOrRelationshipPath, ps, inv<>[] as inverse\n"
          + "MATCH (ps)-[r]->(val) where (type(r)<>\"sh__path\"  and type(r) <> \"sh__severity\")  \n"
          + "WITH category, propertyOrRelationshipPath, ps, inverse, collect({ p: type(r), v: val.uri}) as set1\n"
          + "UNWIND keys(ps) as key \n"
          + "WITH category, propertyOrRelationshipPath, inverse, set1, collect ({p: key, v: ps[key]}) as set2raw\n"
          + "WITH category, propertyOrRelationshipPath, inverse, set1, [ x in set2raw where x.p <> \"uri\"] as set2\n"
          + "UNWIND set1+set2 as pair\n"
          + "RETURN " + (shallIUseUriInsteadOfId()?" category ":" n10s.rdf.getIRILocalName(category) ")  + " as category , "
          + (shallIUseUriInsteadOfId()?" propertyOrRelationshipPath ":" n10s.rdf.getIRILocalName(propertyOrRelationshipPath) ")  +
          " as propertyOrRelationshipPath , " + (shallIShorten()?" n10s.rdf.fullUriFromShortForm(pair.p) ":" n10s.rdf.getIRILocalName(n10s.rdf.fullUriFromShortForm(pair.p)) ")
          + " as param, " +  (shallIUseUriInsteadOfId()?" pair.v ":" case when tostring(pair.v) =~ '^\\\\w+://.*' then n10s.rdf.getIRILocalName(toString(pair.v)) else pair.v end ") + "  as value \n"
          + "\n"
          + "UNION\n"
          + "\n"
          + "MATCH  (ns:sh__NodeShape)\n"
          + "OPTIONAL MATCH (targetClass)<-[:sh__targetClass]-(ns)\n"
          + "OPTIONAL MATCH (nsAsTarget:rdfs__Class)-[:sh__property]->(ps) \n"
          + "WITH ns, coalesce(coalesce(targetClass.uri, nsAsTarget.uri),'#') AS category\n"
          + "WITH  category, null as propertyOrRelationshipPath, [{ p: \"closed\" , v: coalesce(ns.sh__closed,false)}, { p:\"ignoredProperties\",v:\n"
          + " [(ns)-[:sh__ignoredProperties]->()-[:rdf__first|rdf__rest*0..]->(prop) \n"
          + " WHERE ()-[:rdf__first]->(prop) | " + (shallIUseUriInsteadOfId()?" prop.uri ":" n10s.rdf.getIRILocalName(prop.uri) ") + " ] }] as pairs\n"
          + "UNWIND pairs as pair\n"
          + "RETURN " + (shallIUseUriInsteadOfId()?" category ":" n10s.rdf.getIRILocalName(category) ")  + " as category , "
          + " propertyOrRelationshipPath , pair.p as param, pair.v as value \n"
          + "\n"
          + "\n"
          + "} \n"
          + "\n"
          + "RETURN category, propertyOrRelationshipPath, param, value\n"
          + "\n"
          + "ORDER BY category, propertyOrRelationshipPath, param";
  }
}
