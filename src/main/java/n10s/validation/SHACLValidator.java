package n10s.validation;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_MODE_LPG;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_KEEP;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_MAP;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN_STRICT;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import n10s.CommonProcedures.InvalidShortenedName;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.NsPrefixMap;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

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
          + "      ps.sh__minCount AS minCount, ps.sh__minInclusive AS minInc, "
          + "      [ (ps)-[:sh__hasValue]->(x) | x.uri ] as hasValueUri, [] + ps.sh__hasValue AS hasValueLiteral,\n"
          + "      [(ps)-[:sh__in]->()-[:rdf__rest*0..]->(value) where exists(value.rdf__first)| value.rdf__first] as inLiterals, "
          + "      [(ps)-[:sh__in]->()-[:rdf__first|rdf__rest*0..]->(prop) WHERE ()-[:rdf__first]->(prop) | prop.uri ] as inUris, "
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
          + "        collect(distinct hasValueUri)[0] as hasValueUri,\n"
          + "        collect(distinct hasValueLiteral)[0] as hasValueLiteral,\n"
          + "        collect(distinct inLiterals)[0] as inLiterals,\n"
          + "        collect(distinct inUris)[0] as inUris,\n"
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

  protected Stream<ValidationResult> runValidations(List<Node> nodeList)
      throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

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

          addCypherToValidationScript(getDataTypeViolationQuery2(nodeList != null), focusLabel,
              propOrRel, focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel,
              severity,propOrRel);
        }

        //this type of constraint only makes sense RDF graphs.
        if (shallIUseUriInsteadOfId() && propConstraint.get("hasValueUri") != null) {
          List<String> valueUriList = (List<String>) propConstraint.get("hasValueUri");
          if (!valueUriList.isEmpty()) {
            String paramSetId =
                propConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
            Map<String, Object> params = createNewSetOfParams(paramSetId);
            params.put("theHasValueUri", valueUriList);

            addCypherToValidationScript(getHasValueUriViolationQuery(nodeList != null), paramSetId,
                focusLabel,
                propOrRel, focusLabel, (String) propConstraint.get("propShapeUid"),
                propOrRel, severity, propOrRel);
          }
        }

        if (propConstraint.get("hasValueLiteral") != null) {
          List<String> valueLiteralList = (List<String>) propConstraint.get("hasValueLiteral");
          if(!valueLiteralList.isEmpty()) {
            String paramSetId =
                propConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
            Map<String, Object> params = createNewSetOfParams(paramSetId);
            params.put("theHasValueLiteral", valueLiteralList);

            addCypherToValidationScript(getHasValueLiteralViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                propOrRel, focusLabel, (String) propConstraint.get("propShapeUid"),
                propOrRel, severity, propOrRel);
          }
        }

        if (propConstraint.get("rangeKind") != null) {
          if (propConstraint.get("rangeKind").equals(SHACL.LITERAL.stringValue())) {
            addCypherToValidationScript(getRangeIRIKindViolationQuery(nodeList != null), focusLabel,
                propOrRel,
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, severity, propOrRel);
          } else if (propConstraint.get("rangeKind").equals(SHACL.BLANK_NODE_OR_IRI.stringValue())){
            addCypherToValidationScript(getRangeLiteralKindViolationQuery(nodeList != null), focusLabel,
                propOrRel,
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, severity, propOrRel);
          }
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

        if (propConstraint.get("inLiterals") != null) {
          List<String> valueLiteralList = (List<String>) propConstraint.get("inLiterals");
          if(!valueLiteralList.isEmpty()) {
            String paramSetId =
                propConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
            Map<String, Object> params = createNewSetOfParams(paramSetId);
            params.put("theInLiterals", valueLiteralList);

            addCypherToValidationScript(getInLiteralsViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                propOrRel, focusLabel, (String) propConstraint.get("propShapeUid"),
                propOrRel, severity, propOrRel);
          }
        }

        if (propConstraint.get("inUris") != null) {
          List<String> valueLiteralList = (List<String>) propConstraint.get("inUris");
          if(!valueLiteralList.isEmpty()) {
            String paramSetId =
                propConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
            Map<String, Object> params = createNewSetOfParams(paramSetId);
            params.put("theInUris", valueLiteralList);

            addCypherToValidationScript(getInUrisViolationQuery(nodeList != null),
                paramSetId, focusLabel,
                propOrRel, focusLabel, (String) propConstraint.get("propShapeUid"),
                propOrRel, severity, propOrRel);
          }
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
                propOrRel,propOrRel,
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel, propOrRel,
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
                propOrRel,propOrRel,
                " <= params.maxCount ",
                focusLabel, (String) propConstraint.get("propShapeUid"), propOrRel, propOrRel,propOrRel,
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



  protected ValidatorConfig compileValidations(Iterator<Map<String,Object>> constraints)
      throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

    ValidatorConfig vc = new ValidatorConfig();

    while (constraints.hasNext()) {

      Map<String, Object> propConstraint = constraints.next();
      if (propConstraint.get("appliesToCat") == null) {
        log.info("Only class-based targets (sh:targetClass) and implicit class targets are validated.");
      } else if (propConstraint.containsKey("item")&&propConstraint.get("item").equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
        log.info("Constraints on rdf:type are ignored  (temporary solution until we figure out how can they be used).");
      } else{
        processConstraint(propConstraint, vc);
        addPropertyConstraintsToList(propConstraint, vc);
      }
    }

    return vc;
  }

  protected void processConstraint(Map<String,Object> theConstraint, ValidatorConfig vc)
      throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

    String focusLabel = translateUri((String) theConstraint.get("appliesToCat"));
    String propOrRel = theConstraint.containsKey("item")?translateUri((String) theConstraint.get("item")):null;
    String severity = theConstraint.containsKey("severity")? (String) theConstraint.get("severity")
        :SHACL.VIOLATION.stringValue();

    if (theConstraint.get("dataType") != null) {
      //TODO: this will be safer via APOC? maybe exclude some of them? and log the ignored ones?
      addCypherToValidationScripts(vc,getDataTypeViolationQuery(false), getDataTypeViolationQuery(true), focusLabel,
          propOrRel,
          getDatatypeCastExpressionPref((String) theConstraint.get("dataType")),
          getDatatypeCastExpressionSuff((String) theConstraint.get("dataType")),
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
          severity);

      //TODO: Complete all datatypes: spatial type Point, Temporal types: Date, Time, LocalTime, DateTime, LocalDateTime and Duration

      addCypherToValidationScripts(vc,getDataTypeViolationQuery2(false),getDataTypeViolationQuery2(true), focusLabel,
          propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel,
          severity,propOrRel);
    }

    //this type of constraint only makes sense RDF graphs.
    if (shallIUseUriInsteadOfId() && theConstraint.get("hasValueUri") != null) {
      List<String> valueUriList = (List<String>) theConstraint.get("hasValueUri");
      if (!valueUriList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theHasValueUri", valueUriList);

        addCypherToValidationScripts(vc,getHasValueUriViolationQuery(false), getHasValueUriViolationQuery(true), paramSetId,
            focusLabel,
            propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
            propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("hasValueLiteral") != null) {
      List<String> valueLiteralList = (List<String>) theConstraint.get("hasValueLiteral");
      if(!valueLiteralList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theHasValueLiteral", valueLiteralList);

        addCypherToValidationScripts(vc,getHasValueLiteralViolationQuery(false),getHasValueLiteralViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
            propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("rangeKind") != null) {
      if (theConstraint.get("rangeKind").equals(SHACL.LITERAL.stringValue())) {
        addCypherToValidationScripts(vc,getRangeIRIKindViolationQuery(false), getRangeIRIKindViolationQuery(true), focusLabel,
            propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel);
      } else if (theConstraint.get("rangeKind").equals(SHACL.BLANK_NODE_OR_IRI.stringValue())){
        addCypherToValidationScripts(vc,getRangeLiteralKindViolationQuery(false),getRangeLiteralKindViolationQuery(true), focusLabel,
            propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("rangeType") != null && !theConstraint.get("rangeType")
        .equals("")) {
      addCypherToValidationScripts(vc,getRangeType1ViolationQuery(false), getRangeType1ViolationQuery(true), focusLabel,
          propOrRel,
          translateUri((String) theConstraint.get("rangeType")),
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity,
          translateUri((String) theConstraint.get("rangeType")));
      addCypherToValidationScripts(vc,getRangeType2ViolationQuery(false),getRangeType2ViolationQuery(true), focusLabel,
          propOrRel,
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity,
          propOrRel);
    }

    if (theConstraint.get("inLiterals") != null) {
      List<String> valueLiteralList = (List<String>) theConstraint.get("inLiterals");
      if(!valueLiteralList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theInLiterals", valueLiteralList);

        addCypherToValidationScripts(vc,getInLiteralsViolationQuery(false),getInLiteralsViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
            propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("inUris") != null) {
      List<String> valueLiteralList = (List<String>) theConstraint.get("inUris");
      if(!valueLiteralList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theInUris", valueLiteralList);

        addCypherToValidationScripts(vc,getInUrisViolationQuery(false),getInUrisViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
            propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("pattern") != null) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.PATTERN.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("theRegex", (String) theConstraint.get("pattern"));
      addCypherToValidationScripts(vc,getRegexViolationQuery(false), getRegexViolationQuery(true), paramSetId,
          focusLabel, propOrRel, propOrRel, focusLabel,
          (String) theConstraint.get("propShapeUid"), propOrRel, severity);

    }

    if (theConstraint.get("minCount") != null) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_COUNT.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("minCount", theConstraint.get("minCount"));

      if (!(boolean) theConstraint.get("inverse")) {

        addCypherToValidationScripts(vc,getMinCardinality1ViolationQuery(false),getMinCardinality1ViolationQuery(true),
            paramSetId, focusLabel,
            " params.minCount <= ",
            propOrRel,propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel, propOrRel,
            severity);
      } else {
        // multivalued attributes not checked for cardinality in the case of inverse??
        // does not make sense in an LPG
        addCypherToValidationScripts(vc,getMinCardinality1InverseViolationQuery(false),getMinCardinality1InverseViolationQuery(true),
            paramSetId, focusLabel,
            " params.minCount <= ",
            propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
            severity);
      }
    }

    if (theConstraint.get("maxCount") != null) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MAX_COUNT.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("maxCount", theConstraint.get("maxCount"));

      if (!(boolean) theConstraint.get("inverse")) {

        addCypherToValidationScripts(vc,getMaxCardinality1ViolationQuery(false),getMaxCardinality1ViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel,propOrRel,
            " <= params.maxCount ",
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,propOrRel,
            severity);
      } else {
        // multivalued attributes not checked for cardinality in the case of inverse??
        // does not make sense in an LPG
        addCypherToValidationScripts(vc,getMaxCardinality1InverseViolationQuery(false),getMaxCardinality1InverseViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel,
            " <= params.maxCount ",
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
            severity);
      }
    }

    if (theConstraint.get("minStrLen") != null || theConstraint.get("maxStrLen") != null) {

      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_LENGTH.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("minStrLen", theConstraint.get("minStrLen"));
      params.put("maxStrLen", theConstraint.get("maxStrLen"));

      addCypherToValidationScripts(vc,getStrLenViolationQuery(false), getStrLenViolationQuery(true), paramSetId,
          focusLabel,
          propOrRel,
          theConstraint.get("minStrLen") != null ? " params.minStrLen <= " : "",
          theConstraint.get("maxStrLen") != null ? " <= params.maxStrLen " : "",
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
          severity);

    }

    if (theConstraint.get("minInc") != null || theConstraint.get("maxInc") != null
        || theConstraint.get("minExc") != null || theConstraint.get("maxExc") != null) {

      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_EXCLUSIVE.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("min",
          theConstraint.get("minInc") != null ? theConstraint.get("minInc")
              : theConstraint.get("minExc"));
      params.put("max",
          theConstraint.get("maxInc") != null ? theConstraint.get("maxInc")
              : theConstraint.get("maxExc"));

      addCypherToValidationScripts(vc,getValueRangeViolationQuery(false), getValueRangeViolationQuery(true),paramSetId,
          focusLabel, propOrRel,
          theConstraint.get("minInc") != null ? " params.min <="
              : (theConstraint.get("minExc") != null ? " params.min < " : ""),
          theConstraint.get("maxInc") != null ? " <= params.max "
              : (theConstraint.get("maxExc") != null ? " < params.max " : ""),
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
          severity);
    }

    if (theConstraint.get("ignoredProps") != null) {

      String paramSetId = theConstraint.get("nodeShapeUid") + "_" + SHACL.CLOSED.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      List<String> allowedPropsTranslated = new ArrayList<>();
      for (String uri:(List<String>)theConstraint.get("ignoredProps")) {
        if(!uri.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
          allowedPropsTranslated.add(translateUri(uri));
        }
      }
      if(theConstraint.get("definedProps") != null) {
        for (String uri:(List<String>)theConstraint.get("definedProps")) {
          if(!uri.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
            allowedPropsTranslated.add(translateUri(uri));
          }
        }
      }
      params.put("allAllowedProps", allowedPropsTranslated);

      addCypherToValidationScripts(vc,getNodeStructureViolationQuery(false), getNodeStructureViolationQuery(true),paramSetId, focusLabel,
          focusLabel,
          (String) theConstraint.get("nodeShapeUid"), "http://www.w3.org/ns/shacl#Violation");


    }

  }

void addPropertyConstraintsToList(Map<String, Object> propConstraint,
    ValidatorConfig vc)
    throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

    String focusLabel = translateUri((String) propConstraint.get("appliesToCat"));
    String propOrRel = propConstraint.containsKey("item")?translateUri((String) propConstraint.get("item")):null;
    //TODO: add severity and inverse
    //String severity = (String) propConstraint.get("severity");
    //INVERSE?

    if (propConstraint.get("dataType") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.DATATYPE.getLocalName():SHACL.DATATYPE.getLocalName(),
          shallIUseUriInsteadOfId()?propConstraint.get("dataType"):((String)propConstraint.get("dataType"))
              .substring(URIUtil.getLocalNameIndex((String)propConstraint.get("dataType")))));
    }

    if (propConstraint.get("hasValueUri") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.HAS_VALUE.getLocalName():SHACL.HAS_VALUE.getLocalName(),
          (List<String>) propConstraint.get("hasValueUri"))); //TODO: there  should be a translate here??
    }

    if (propConstraint.get("hasValueLiteral") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.HAS_VALUE.getLocalName():SHACL.HAS_VALUE.getLocalName(),
          (List<String>) propConstraint.get("hasValueLiteral")));
    }

    if (propConstraint.get("rangeKind") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.NODE_KIND.getLocalName():SHACL.NODE_KIND.getLocalName(),
          shallIUseUriInsteadOfId()?propConstraint.get("rangeKind"):
              ((String)propConstraint.get("rangeKind"))
                  .substring(URIUtil.getLocalNameIndex((String)propConstraint.get("rangeKind")))));
    }

    if (propConstraint.get("rangeType") != null && !propConstraint.get("rangeType")
        .equals("")) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.CLASS.getLocalName():SHACL.CLASS.getLocalName(),
          translateUri((String)propConstraint.get("rangeType"))));
    }

    if (propConstraint.get("inLiterals") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.IN.getLocalName():SHACL.IN.getLocalName(),
          (List<String>)propConstraint.get("inLiterals")));
    }

    if (propConstraint.get("inUris") != null) {
      List<String> inUrisRaw = (List<String>) propConstraint.get("inUris");
      List<String> inUrisLocal = new ArrayList<>();
      inUrisRaw.forEach(x ->  inUrisLocal.add(x.substring(URIUtil.getLocalNameIndex(x))));
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.IN.getLocalName():SHACL.IN.getLocalName(),
          shallIUseUriInsteadOfId()?inUrisRaw:inUrisLocal));
    }

    if (propConstraint.get("pattern") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.PATTERN.getLocalName():SHACL.PATTERN.getLocalName(),
          propConstraint.get("pattern")));
    }

    if (propConstraint.get("minCount") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.MIN_COUNT.getLocalName():SHACL.MIN_COUNT.getLocalName(),
          propConstraint.get("minCount")));
    }

    if (propConstraint.get("maxCount") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.MAX_COUNT.getLocalName():SHACL.MAX_COUNT.getLocalName(),
          propConstraint.get("maxCount")));
    }

    if (propConstraint.get("minStrLen") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ?"sh:"+SHACL.MIN_LENGTH.getLocalName()
              : SHACL.MIN_LENGTH.getLocalName(),
          propConstraint.get("minStrLen")));
    }

    if(propConstraint.get("maxStrLen") != null){
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.MAX_LENGTH.getLocalName():SHACL.MAX_LENGTH.getLocalName(),
          propConstraint.get("maxStrLen")));
    }

    if (propConstraint.get("minInc") != null){
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
        shallIUseUriInsteadOfId() ? "sh:"+ SHACL.MIN_INCLUSIVE.getLocalName()
            : SHACL.MIN_INCLUSIVE.getLocalName(),
        propConstraint.get("minInc")));
    }

    if (propConstraint.get("maxInc") != null){
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.MAX_INCLUSIVE.getLocalName():SHACL.MAX_INCLUSIVE.getLocalName(),
          propConstraint.get("maxInc")));
    }

    if (propConstraint.get("minExc") != null){
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.MIN_EXCLUSIVE.getLocalName():SHACL.MIN_EXCLUSIVE.getLocalName(),
          propConstraint.get("minExc")));
    }

    if (propConstraint.get("maxExc") != null){
    vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
        shallIUseUriInsteadOfId()?"sh:"+SHACL.MAX_EXCLUSIVE.getLocalName():SHACL.MAX_EXCLUSIVE.getLocalName(),
        propConstraint.get("maxExc")));
    }

    if (propConstraint.get("ignoredProps") != null) {
      List<String> ignoredUrisRaw = (List<String>) propConstraint.get("ignoredProps");
      List<String> ignoredUrisTranslated = new ArrayList<>();
      for (String x:ignoredUrisRaw) {
        if(!x.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
          ignoredUrisTranslated.add(translateUri(x));
        }
      }
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId()?"sh:"+SHACL.IGNORED_PROPERTIES.getLocalName():SHACL.IGNORED_PROPERTIES.getLocalName(),
          ignoredUrisTranslated));
    }

  }

  private String translateUri(String uri)
      throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {
    if( gc == null || gc.getGraphMode()==GRAPHCONF_MODE_LPG){
      return uri.substring(URIUtil.getLocalNameIndex(uri));
    } else if(gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_SHORTEN_STRICT||
        gc.getHandleVocabUris()==GRAPHCONF_VOC_URI_MAP){
        return getShortForm(uri);
    } else {
      //it's GRAPHCONF_VOC_URI_KEEP
      return uri;
    }
  }

  private String getShortForm(String str)
      throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {
    IRI iri = SimpleValueFactory.getInstance().createIRI(str);
    NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
    if (!prefixDefs.hasNs(iri.getNamespace())) {
      throw new ShapesUsingNamespaceWithUndefinedPrefix(
          "Prefix Undefined: No prefix defined for namespace <" + str + ">. Use n10s.nsprefixes.add(...) procedure.");
    }
    return prefixDefs.getPrefixForNs(iri.getNamespace()) + PREFIX_SEPARATOR + iri.getLocalName();
  }

  private Map<String, Object> createNewSetOfParams(String id) {
    Map<String, Object> params = new HashMap<>();
    paramsU.put(id, params);
    return params;
  }

  private Map<String, Object> createNewSetOfParams(Map<String, Object> allParams, String id) {
    Map<String, Object> params = new HashMap<>();
    allParams.put(id, params);
    return params;
  }

  protected Iterator<Map<String,Object>> parseConstraints(InputStream is, RDFFormat format) {
    Repository repo = new SailRepository(new MemoryStore());

    List<Map<String,Object>> constraints = new ArrayList<>();
    try (RepositoryConnection conn = repo.getConnection()) {
      conn.begin();
      conn.add(new InputStreamReader(is), "http://neo4j.com/base/", format);
      conn.commit();
      String sparqlQueryPropertyConstraints= "prefix sh: <http://www.w3.org/ns/shacl#>  \n"
          + "SELECT distinct ?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity "
          + "?targetClass ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc ?minStrLen "
          + "?maxStrLen (GROUP_CONCAT (distinct ?hasValueUri; separator=\"---\") AS ?hasValueUris) "
          + "(GROUP_CONCAT (distinct ?hasValueLiteral; separator=\"---\") AS ?hasValueLiterals) "
          + "(GROUP_CONCAT (distinct ?in; separator=\"---\") AS ?ins) "
          + "(isLiteral(?inFirst) as ?isliteralIns)\n"
          + "{ ?ns a sh:NodeShape ;\n"
          + "     sh:property ?ps .\n"
          + "\n"
          + "  optional { ?ps sh:path/sh:inversePath ?invPath }\n"
          + "  optional { ?ps sh:path  ?path }\n"
          + "  optional { ?ps sh:class  ?rangeClass }\n"
          + "  optional { ?ps sh:nodeKind  ?rangeKind }  \n"
          + "  optional { ?ps sh:datatype  ?datatype }\n"
          + "  optional { ?ps sh:severity  ?severity }\n"
          + "  optional { \n"
          + "    { ?ns sh:targetClass  ?targetClass }\n"
          + "    union\n"
          + "    { ?targetClass sh:property ?ps;\n"
          + "          a rdfs:Class . }\n"
          + "  }\n"
          + "  optional { ?ps sh:pattern  ?pattern }\n"
          + "  optional { ?ps sh:maxCount  ?maxCount }\n"
          + "  \n"
          + "    optional { ?ps sh:minCount  ?minCount }\n"
          + "    optional { ?ps sh:minInclusive  ?minInc }\n"
          + "  \n"
          + "    optional { ?ps sh:maxInclusive  ?maxInc }\n"
          + "    optional { ?ps sh:minExclusive  ?minExc }\n"
          + "    optional { ?ps sh:maxExclusive  ?maxExc }  \n"
          + "  optional { ?ps sh:minLength  ?minStrLen }\n"
          + "  \n"
          + "    optional { ?ps sh:minLength  ?minStrLen }\n"
          + "    optional { ?ps sh:maxLength  ?maxStrLen }\n"
          + "  \n"
          + "   optional { ?ps sh:hasValue  ?hasValueUri . filter(isIRI(?hasValueUri)) } \n"
          + "    optional { ?ps sh:hasValue  ?hasValueLiteral . filter(isLiteral(?hasValueLiteral)) } \n"
          + "  \n"
          + "    optional { ?ps sh:in/rdf:rest*/rdf:first ?in } \n"
          + "    optional { ?ps sh:in/rdf:first ?inFirst }\n"
          + "    optional { ?ps sh:minLength  ?minStrLen }\n"
          + "  \n"
          + "} group by \n"
          + "?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity ?targetClass ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc ?minStrLen ?maxStrLen ?inFirst";

      String sparqlQueryNodeConstraints = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
          + "prefix sh: <http://www.w3.org/ns/shacl#>  \n"
          + "SELECT ?ns ?targetClass (GROUP_CONCAT (distinct ?definedProp; separator=\"---\") AS ?definedProps)\n"
          + "(GROUP_CONCAT (distinct ?ignored; separator=\"---\") AS ?ignoredProps)\n"
          + "{ ?ns a sh:NodeShape ;\n"
          + "    sh:closed true .\n"
          + "  \n"
          + "   optional { \n"
          + "     ?ns sh:targetClass  ?targetClass \n"
          + "   }\n"
          + "   \n"
          + "   optional { \n"
          + "     ?targetClass a rdfs:Class . filter(?targetClass = ?ns)\n"
          + "   }\n"
          + "  \n"
          + "  optional { ?ns sh:property [ sh:path ?definedProp ].  filter(isIRI(?definedProp)) }\n"
          + "   optional { ?ns sh:ignoredProperties/rdf:rest*/rdf:first ?ignored }\n"
          + "   \n"
          + "} group by ?ns ?targetClass";

      TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQueryPropertyConstraints);
      TupleQueryResult queryResult = tupleQuery.evaluate();
      while(queryResult.hasNext()){
        Map<String,Object> record = new HashMap<>();
        BindingSet next = queryResult.next();
        record.put("item", next.hasBinding("invPath")?next.getValue("invPath").stringValue():next.getValue("path").stringValue());
        record.put("inverse", next.hasBinding("invPath"));
        record.put("appliesToCat", next.hasBinding("targetClass")?next.getValue("targetClass").stringValue():null);
        record.put("rangeType", next.hasBinding("rangeClass")?next.getValue("rangeClass").stringValue():null);
        record.put("rangeKind", next.hasBinding("rangeKind")?next.getValue("rangeKind").stringValue():null);
        record.put("dataType", next.hasBinding("datatype")?next.getValue("datatype").stringValue():null);
        record.put("pattern", next.hasBinding("pattern")?next.getValue("pattern").stringValue():null);
        record.put("maxCount", next.hasBinding("maxCount")?((Literal)next.getValue("maxCount")).intValue():null);
        record.put("minCount", next.hasBinding("minCount")?((Literal)next.getValue("minCount")).intValue():null);
        record.put("minInc", next.hasBinding("minInc")?((Literal)next.getValue("minInc")).intValue():null);
        record.put("minExc", next.hasBinding("minExc")?((Literal)next.getValue("minExc")).intValue():null);
        record.put("maxInc", next.hasBinding("maxInc")?((Literal)next.getValue("maxInc")).intValue():null);
        record.put("maxExc", next.hasBinding("maxExc")?((Literal)next.getValue("maxExc")).intValue():null);

        if(next.hasBinding("hasValueLiterals") && !next.getValue("hasValueLiterals").stringValue().equals("")) {
          List<String> hasValueLiterals = Arrays.asList(next.getValue("hasValueLiterals").stringValue().split("---"));
          record.put("hasValueLiteral", hasValueLiterals);
        }
        if(next.hasBinding("hasValueUris") && !next.getValue("hasValueUris").stringValue().equals("")) {
          List<String> hasValueUris = Arrays.asList(next.getValue("hasValueUris").stringValue().split("---"));
          record.put("hasValueUri", hasValueUris);
        }

        if(next.hasBinding("isliteralIns")) {
          List<String> inVals = Arrays.asList(next.getValue("ins").stringValue().split("---"));
          Literal val = (Literal)next.getValue("isliteralIns");
          if(val.booleanValue()) {
            record.put("inLiterals", inVals);
          } else {
            record.put("inUris", inVals);
          }
        }

        record.put("minStrLen", next.hasBinding("minStrLen")?((Literal)next.getValue("minStrLen")).intValue():null);
        record.put("maxStrLen", next.hasBinding("maxStrLen")?((Literal)next.getValue("maxStrLen")).intValue():null);
        record.put("propShapeUid", next.hasBinding("ps")?next.getValue("ps").stringValue():null);
        record.put("severity", next.hasBinding("severity")?next.getValue("severity").stringValue():"http://www.w3.org/ns/shacl#Violation");

        constraints.add(record);

      }


      tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQueryNodeConstraints);
      queryResult = tupleQuery.evaluate();
      while(queryResult.hasNext()){
        Map<String,Object> record = new HashMap<>();
        BindingSet next = queryResult.next();
        record.put("appliesToCat", next.getValue("targetClass").stringValue());
        record.put("nodeShapeUid", next.hasBinding("ns")?next.getValue("ns").stringValue():null);
        if(next.hasBinding("definedProps")) {
          record.put("definedProps", Arrays.asList(next.getValue("definedProps").stringValue().split("---")));
        }
        if(next.hasBinding("ignoredProps")) {
          record.put("ignoredProps", Arrays.asList(next.getValue("ignoredProps").stringValue().split("---")));
        }
        constraints.add(record);
      }

    } catch (IOException e) {
      //TODO: deal  with this properly
      e.printStackTrace();
    }

    return constraints.iterator();
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

  private void addCypherToValidationScripts(ValidatorConfig vc, String querystrGlobal, String querystrOnNodeset, String... args) {
    vc.getEngineGlobal().append("\n UNION \n").append(String.format(querystrGlobal, args));
    vc.getEngineForNodeSet().append("\n UNION \n").append(String.format(querystrOnNodeset, args));
  }

  private void addCypherToValidationScript(String querystr, String... args) {
    cypherU.append("\n UNION \n").append(String.format(querystr, args));
  }

  private String getDataTypeViolationQuery(boolean tx ) {
    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_DATATYPE_V_SUFF());
  }

  private String getDataTypeViolationQuery2(boolean tx ) {
    return getQuery(CYPHER_MATCH_REL_WHERE, tx, CYPHER_DATATYPE2_V_SUFF());
  }

  private String getRangeIRIKindViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_IRI_KIND_V_SUFF());
  }

  private String getRangeLiteralKindViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_LITERAL_KIND_V_SUFF());
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

  private String getHasValueUriViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_HAS_VALUE_URI_V_SUFF());
  }


  private String getHasValueLiteralViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_HAS_VALUE_LITERAL_V_SUFF());
  }

  private String getInLiteralsViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_IN_LITERAL_V_SUFF());
  }

  private String getInUrisViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_IN_URI_V_SUFF());
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

//  private String getMinCardinality2ViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MIN_CARDINALITY2_V_SUFF());
//  }

  private String getMaxCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY1_V_SUFF());
  }

  private String getMaxCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF());
  }

//  private String getMaxCardinality2ViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY2_V_SUFF());
//  }

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

  private String CYPHER_DATATYPE2_V_SUFF() {
    return " true RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
        + "' as propertyShape, " + (shallIUseUriInsteadOfId()?" x.uri ":" 'node id: ' + id(x) ") + "as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " + ' should be a property, instead it  is a relationship' as message ";
  }

  private String CYPHER_IRI_KIND_V_SUFF(){
    return " (focus)-[:`%s`]->() RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity,"
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " + ' should be a property ' as message  ";
  }

  private String CYPHER_LITERAL_KIND_V_SUFF(){
    return " exists(focus.`%s`) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity,"
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " + ' should be a relationship ' as message  ";
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


  private String CYPHER_HAS_VALUE_URI_V_SUFF() {
    return " true with params, focus unwind params.theHasValueUri as reqVal with focus, reqVal where not reqVal in [(focus)-[:`%s`]->(v) | v.uri ] "
        + "RETURN "
        + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'The required value ' + reqVal  + ' could not be found as value of relationship ' + " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s') ": " '%s' ") + " as message  ";
  }

  private String CYPHER_HAS_VALUE_LITERAL_V_SUFF() {
    return " true with params, focus unwind params.theHasValueLiteral as  reqVal with focus, reqVal where not reqVal in [] + focus.`%s` "
        + "RETURN "
        + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'The required value \"'+ reqVal + '\" was not found in property ' + " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s') ": " '%s' ") + " as message  ";
  }

  private String CYPHER_IN_LITERAL_V_SUFF() {
    return " true with params, focus unwind [] + focus.`%s` as val with focus, val where not val in params.theInLiterals "
        + "RETURN "
        + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, val as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'The value \"'+ val + '\" in property ' + " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s') ": " '%s'") + "+ 'is not in  the accepted list' as message  ";
  }

  private String CYPHER_IN_URI_V_SUFF() {
    return " true with params, focus unwind [(focus)-[:`%s`]->(x) | x ] as val with focus, val where not val.uri in params.theInUris "
        + "RETURN "
        + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") + " as nodeId, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, " +  (shallIUseUriInsteadOfId()?"val.uri":"id(val)") +" as offendingValue, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "'The value \"'+ " + (shallIUseUriInsteadOfId()?" val.uri ":" 'node id: '  + id(val) ") +" + '\" in property ' + " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s') ": " '%s'") + "+ ' is not in  the accepted list' as message  ";
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
    return "NOT %s ( size((focus)-[:`%s`]->()) +  size([] + coalesce(focus.`%s`, [])) )  RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable cardinality: ' + ( size((focus)-[:`%s`]->()) +  size([] + focus.`%s`))  as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MAX_CARDINALITY1_V_SUFF() {
    return "NOT (size((focus)-[:`%s`]->()) + size([] + coalesce(focus.`%s`,[]))) %s  RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, "  + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable  cardinality: ' + (size((focus)-[:`%s`]->()) + size([] + focus.`%s`)) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF() {   //This will need fixing, the coalesce in first line + the changes to cardinality
    return "NOT %s size((focus)<-[:`%s`]-()) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable cardinality: ' + size((focus)<-[:`%s`]-()) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF() {   //Same as previous
    return "NOT size((focus)<-[:`%s`]-()) %s RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
     " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unacceptable cardinality: ' + size((focus)<-[:`%s`]-()) as message, "
        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

//  private String CYPHER_MIN_CARDINALITY2_V_SUFF() {
//    return " NOT %s size([] + focus.`%s`) RETURN "  + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
//    "as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
//        + "' as propertyShape, 'unacceptable cardinality: ' + size([] + focus.`%s`) as message, "
//        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
//        + "null as offendingValue  ";
//  }
//  private String CYPHER_MAX_CARDINALITY2_V_SUFF() {
//    return " NOT size([] + focus.`%s`) %s RETURN "  + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
//    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
//        + "' as propertyShape, 'unacceptable cardinality: ' + size([] + focus.`%s`) as message, "
//        + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") + " as propertyName, '%s' as severity, "
//        + "null as offendingValue  ";
//  }

  private String CYPHER_STRLEN_V_SUFF() {
    return "NOT all(x in [] +  focus.`%s` where %s size(toString(x)) %s ) RETURN " + (shallIUseUriInsteadOfId()?" focus.uri ":" id(focus) ") +
    " as nodeId, " + (shallIShorten()?"n10s.rdf.fullUriFromShortForm('%s')": " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.MAX_LENGTH_CONSTRAINT_COMPONENT + "' as propertyShape, focus.`%s` as offendingValue, "
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
        + "'Closed type definition does not include this property/relationship' as message  ";
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

  protected class ShapesUsingNamespaceWithUndefinedPrefix extends Exception {

    public ShapesUsingNamespaceWithUndefinedPrefix(String message) {
      super(message);
    }
  }
}
