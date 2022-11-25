package n10s.validation;

import com.google.common.collect.Lists;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.*;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.UriUtils.UriNamespaceHasNoAssociatedPrefix;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static n10s.graphconfig.GraphConfig.*;
import static n10s.graphconfig.Params.WKTLITERAL_URI;
import static n10s.utils.UriUtils.translateUri;

public class SHACLValidator {

  private static final String CYPHER_TX_INFIX = " focus in $touchedNodes AND ";

  private static final String CYPHER_MATCH_WHERE = "MATCH (focus:`%s`) WHERE ";
  private static final String CYPHER_MATCH_ALL_WHERE = "MATCH (focus) WHERE ";
  private static final String CYPHER_MATCH_REL_WHERE = "MATCH (focus:`%s`)-[r:`%s`]->(x) WHERE ";
  private static final String CYPHER_MATCH_ALL_REL_WHERE = "MATCH (focus)-[r:`%s`]->(x) WHERE ";
  private static final String CYPHER_WITH_PARAMS_MATCH_WHERE = "WITH $`%s` as params MATCH (focus:`%s`) WHERE ";
  private static final String CYPHER_WITH_PARAMS_MATCH_ALL_WHERE = "WITH $`%s` as params MATCH (focus) WHERE ";
  private static final String BNODE_PREFIX = "bnode://id/";
  private static final int GLOBAL_CONSTRAINT = 0;
  private static final int CLASS_BASED_CONSTRAINT = 1;
  private static final int QUERY_BASED_CONSTRAINT = 2;
  public static final String SHACL_COUNT_CONSTRAINT_COMPONENT = "http://www.w3.org/ns/shacl#CountConstraintComponent";
  public static final String SHACL_VALUE_RANGE_CONSTRAINT_COMPONENT = "http://www.w3.org/ns/shacl#ValueRangeConstraintComponent";
  public static final String SHACL_LENGTH_CONSTRAINT_COMPONENT = "http://www.w3.org/ns/shacl#LengthConstraintComponent";


  String PROP_CONSTRAINT_QUERY = "PREFIX ex: <http://example/>\n" +
          "prefix sh: <http://www.w3.org/ns/shacl#> \n" +
          "prefix exp: <http://www.nsmntx.org/voc/expectations#>\n" +
          "\n" +
          "SELECT distinct ?ns ?ps ?mostly ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity (coalesce(?pmsg, ?nmsg,\"\") as ?msg)\n" +
          "?targetClass ?targetIsQuery ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc ?minStrLen \n" +
          "?maxStrLen (GROUP_CONCAT (distinct ?disjointProp; separator=\"---\") AS ?disjointProps) \n" +
          "(GROUP_CONCAT (distinct ?hasValueUri; separator=\"---\") AS ?hasValueUris) \n" +
          "(GROUP_CONCAT (distinct ?hasValueLiteral; separator=\"---\") AS ?hasValueLiterals) \n" +
          "(GROUP_CONCAT (distinct ?in; separator=\"---\") AS ?ins) \n" +
          "(GROUP_CONCAT (distinct ?notIn; separator=\"---\") AS ?notins) \n" +
          "(isLiteral(?inFirst) as ?isliteralIns)\n" +
          "(isLiteral(?notInFirst) as ?isliteralNotIns)\n" +
          "{ ?ns a ?shapeOrNodeShape ;\n" +
          "     sh:node?/sh:property ?ps .\n" +
          "  filter ( ?shapeOrNodeShape = sh:Shape || ?shapeOrNodeShape = sh:NodeShape )\n" +
          "\n" +
          "  optional { ?ps sh:path/sh:inversePath ?invPath }\n" +
          "  optional { ?ps sh:path  ?path }\n" +
          "  optional { ?ps sh:class  ?rangeClass }\n" +
          "  optional { ?ps sh:nodeKind  ?rangeKind }  \n" +
          "  optional { ?ps sh:datatype  ?datatype }\n" +
          "  optional { ?ps sh:severity  ?severity }\n" +
          "  optional { ?ps sh:message  ?pmsg }  \n" +
          "  optional { ?ns sh:message  ?nmsg }  \n" +
          "  { \n" +
          "    { ?ns sh:targetClass  ?targetClass .\n" +
          "      bind( false as ?targetIsQuery )}\n" +
          "    union\n" +
          "    { ?targetClass sh:property ?ps;\n" +
          "          a rdfs:Class . \n" +
          "            bind( false as ?targetIsQuery )}\n" +
          "    union \n" +
          "    { ?ns sh:targetQuery ?targetClass .\n" +
          "              bind( true as ?targetIsQuery )}\n" +
          "  }\n" +
          "  optional { ?ps sh:pattern  ?pattern }\n" +
          "  optional { ?ps sh:maxCount  ?maxCount }\n" +
          "  \n" +
          "    optional { ?ps sh:minCount  ?minCount }\n" +
          "    optional { ?ps sh:minInclusive  ?minInc }\n" +
          "  \n" +
          "    optional { ?ps sh:maxInclusive  ?maxInc }\n" +
          "    optional { ?ps sh:minExclusive  ?minExc }\n" +
          "    optional { ?ps sh:maxExclusive  ?maxExc }  \n" +
          "  optional { ?ps sh:minLength  ?minStrLen }\n" +
          "  \n" +
          "    optional { ?ps sh:minLength  ?minStrLen }\n" +
          "    optional { ?ps sh:maxLength  ?maxStrLen }\n" +
          "  \n" +
          "   optional { ?ps sh:hasValue  ?hasValueUri . filter(isIRI(?hasValueUri)) } \n" +
          "    optional { ?ps sh:hasValue  ?hasValueLiteral . filter(isLiteral(?hasValueLiteral)) } \n" +
          "  \n" +
          "    optional { ?ps sh:in/rdf:rest*/rdf:first ?in } \n" +
          "    optional { ?ps sh:in/sh:not/rdf:rest*/rdf:first ?notIn }\n" +
          "    optional { ?ps sh:in/rdf:first ?inFirst }\n" +
          "    optional { ?ps sh:in/sh:not/rdf:first ?notInFirst }\n" +
          "    optional { ?ps sh:minLength  ?minStrLen }\n" +
          "    optional { ?ps sh:disjoint  ?disjointProp }\n" +
          "    optional { ?ps exp:mostly  ?mostly }\n" +
          "   \n" +
          "} group by \n" +
          "?ns ?ps ?path ?mostly ?invPath ?rangeClass  ?rangeKind ?datatype ?severity ?nmsg ?pmsg " +
          "?targetClass ?targetIsQuery ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc " +
          "?minStrLen ?maxStrLen ?inFirst ?notInFirst";

  String NODE_CONSTRAINT_QUERY = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
          "prefix sh: <http://www.w3.org/ns/shacl#>  \n" +
          "SELECT ?ns (coalesce(?nmsg,\"\") as ?msg) ?targetClass ?targetIsQuery (GROUP_CONCAT (distinct ?definedProp; separator=\"---\") AS ?definedProps)\n" +
          "(GROUP_CONCAT (distinct ?ignored; separator=\"---\") AS ?ignoredProps)\n" +
          "{ ?ns a sh:NodeShape ;\n" +
          "    sh:closed true .\n" +
          "  {\n" +
          "   { \n" +
          "     ?ns sh:targetClass  ?targetClass .\n" +
          "      bind( false as ?targetIsQuery )\n" +
          "   }\n" +
          "   union\n" +
          "   { \n" +
          "     ?targetClass a rdfs:Class . filter(?targetClass = ?ns) .\n" +
          "     bind( false as ?targetIsQuery )\n" +
          "   }\n" +
          "   union\n" +
          "   { \n" +
          "     ?ns sh:targetQuery  ?targetClass .\n" +
          "      bind( true as ?targetIsQuery )\n" +
          "   }\n" +
          "  }\n" +
          "  optional { ?ns sh:message  ?nmsg }\n" +
          "  optional { ?ns sh:property [ sh:path ?definedProp ].  filter(isIRI(?definedProp)) }\n" +
          "   optional { ?ns sh:ignoredProperties/rdf:rest*/rdf:first ?ignored }\n" +
          "   \n" +
          "} group by ?ns ?nmsg ?targetClass ?targetIsQuery";

  //the sh:class at the node shape level is not used (consider removing)
  String NODE_ADDITIONAL_CONSTRAINT_QUERY = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
          "prefix sh: <http://www.w3.org/ns/shacl#>  \n" +
          "SELECT ?ns (coalesce(?nmsg,\"\") as ?msg) ?targetClass ?targetIsQuery (GROUP_CONCAT (distinct ?class; separator=\"---\") AS ?class)\n" +
          "(GROUP_CONCAT (distinct ?disjointclass; separator=\"---\") AS ?disjointclass)\n" +
          "{ ?ns a sh:NodeShape .\n" +
          "  \n" +
          "  { \n" +
          "    { \n" +
          "     ?ns sh:targetClass  ?targetClass .\n" +
          "        bind( false as ?targetIsQuery )\n" +
          "    }\n" +
          "    union \n" +
          "    { \n" +
          "     ?targetClass a rdfs:Class . filter(?targetClass = ?ns) .\n" +
          "      bind( false as ?targetIsQuery )\n" +
          "    }\n" +
          "    union\n" +
          "    {\n" +
          "    ?ns sh:targetQuery  ?targetClass .\n" +
          "      bind( true as ?targetIsQuery )\n" +
          "    }\n" +
          "  }\n" +
          "  optional { ?ns sh:message ?nmsg }\n" +
          "  optional { ?ns sh:class [ sh:not ?disjointclass ].  filter(isIRI(?disjointclass)) }\n" +
          "  optional { ?ns sh:class ?class .  filter(isIRI(?class)) }\n" +
          "  filter(bound(?disjointclass) || bound(?class))\n" +
          "} group by ?ns ?nmsg ?targetClass ?targetIsQuery";

  private Transaction tx;
  private GraphDatabaseService db;
  private Log log;
  private GraphConfig gc;

  public SHACLValidator(GraphDatabaseService db, Transaction transaction, Log l) {
    this.tx = transaction;
    this.log = l;
    this.db = db;

    try {
      this.gc = new GraphConfig(tx);
    } catch (GraphConfigNotFound graphConfigNotFound) {
      //valid when it's a pure LPG
      this.gc = null;
    }

  }


  protected ValidatorConfig compileValidations(Iterator<Map<String, Object>> constraints)
          throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {

    ValidatorConfig vc = new ValidatorConfig();

    while (constraints.hasNext()) {

      Map<String, Object> propConstraint = constraints.next();
      if (!propConstraint.containsKey("appliesToCat") && !propConstraint.containsKey("appliesToQueryResult")) {
        log.debug(
            "Only class-based targets (sh:targetClass), implicit class targets and query-based shapes are validated.");
      }
      else {
        processConstraint(propConstraint, vc);
      }
    }

    return vc;
  }

  protected void processConstraint(Map<String, Object> theConstraint, ValidatorConfig vc)
          throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {

    int constraintType;
    String focusLabel = "";
    String whereClause = "";
    if (theConstraint.containsKey("appliesToCat") ) {
      if (((String) theConstraint.get("appliesToCat")).equals("")){
        //it's a global constraint
        constraintType = GLOBAL_CONSTRAINT;
      } else {
        //it's a class based constraint
        focusLabel = translateUri((String) theConstraint.get("appliesToCat"), tx, gc);
        constraintType = CLASS_BASED_CONSTRAINT;
      }
    } else if (theConstraint.containsKey("appliesToQueryResult") ){
      //it's a query based constraint
      whereClause = (String) theConstraint.get("appliesToQueryResult");
      validateWhereClause(whereClause);
      constraintType = QUERY_BASED_CONSTRAINT;
    } else {
      throw new SHACLValidationException("invalid constraint config");
    }

    boolean isConstraintOnType = theConstraint.containsKey("item") &&
            theConstraint.get("item").equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

    String propOrRel =
            theConstraint.containsKey("item") ? translateUri((String) theConstraint.get("item"), tx, gc) : null;

    String severity = theConstraint.containsKey("severity") ? (String) theConstraint.get("severity")
        : SHACL.VIOLATION.stringValue();

    String customMsg = theConstraint.containsKey("msg") ? (String) theConstraint.get("msg")
            : "";

    if (theConstraint.get("dataType") != null && !isConstraintOnType) {

      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "DataType", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(focusLabel, propOrRel,
                              getDatatypeCastExpressionPref((String) theConstraint.get("dataType")),
                              getDatatypeCastExpressionSuff((String) theConstraint.get("dataType")),
                              focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
                              severity, (String) theConstraint.get("dataType"), customMsg) ,
                      Arrays.asList(propOrRel,
                              getDatatypeCastExpressionPref((String) theConstraint.get("dataType")),
                              getDatatypeCastExpressionSuff((String) theConstraint.get("dataType")),
                              (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
                              severity, (String) theConstraint.get("dataType"), customMsg)));

      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "DataType2", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(focusLabel, propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
                          propOrRel,severity, propOrRel, customMsg),
                      Arrays.asList( propOrRel, (String) theConstraint.get("propShapeUid"), propOrRel,
                          severity, propOrRel, customMsg)));

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
              printConstraintType(SHACL.DATATYPE),
              (gc != null && gc.getGraphMode() == GRAPHCONF_MODE_RDF) ? theConstraint.get("dataType")
                      : ((String) theConstraint.get("dataType"))
                      .substring(URIUtil.getLocalNameIndex((String) theConstraint.get("dataType")))));
    }

    // This type of constraint only makes sense RDF graphs or for specifying enumerated types (labels).
    // note semantics are that at least one of the values is the given one (different from sh:in).
    // useful with rdf:type "every PremiumCustomer has to be a Customer".
    if (theConstraint.get("hasValueUri") != null && (isConstraintOnType || nodesAreUriIdentified())) {
      List<String> valueUriList = (List<String>) theConstraint.get("hasValueUri");
      if (!valueUriList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);

        if (isConstraintOnType){

          if(typesAsLabels()) {

            addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    "HasValueOnTypeAsLabel", whereClause, constraintType,
                    buildArgArray(constraintType,
                            Arrays.asList(paramSetId, focusLabel,
                                  focusLabel, (String) theConstraint.get("propShapeUid"), severity, customMsg),
                            Arrays.asList(paramSetId, (String) theConstraint.get("propShapeUid"), severity,
                                    customMsg)));

            params.put("theHasTypeTranslatedUris", translateUriList(valueUriList));
          }
          // not an if/else because both can coexist when NODES_AND_LABELS
          if(typesAsRelToNodes()){

            addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    "HasValueOnTypeAsNode", whereClause, constraintType,
                    buildArgArray(constraintType,
                            Arrays.asList(paramSetId,
                                    focusLabel,
                                    propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
                                    propOrRel, severity, propOrRel, customMsg),
                            Arrays.asList(paramSetId,
                                    propOrRel, (String) theConstraint.get("propShapeUid"),
                                    propOrRel, severity, propOrRel, customMsg)));

            params.put("theHasTypeUris", valueUriList);
          }
        } else {
          addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                  "HasValueUri", whereClause, constraintType,
                  buildArgArray(constraintType,
                          Arrays.asList(paramSetId,
                                  focusLabel,
                                  propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
                                  propOrRel, severity, propOrRel, customMsg),
                          Arrays.asList(paramSetId,
                                  propOrRel, (String) theConstraint.get("propShapeUid"),
                                  propOrRel, severity, propOrRel, customMsg)));

          params.put("theHasValueUri", valueUriList);
        }

        //ADD constraint to the list
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.HAS_VALUE),
                (isConstraintOnType&&typesAsLabels()?translateUriList(valueUriList):valueUriList)));

      }
    }

    if (theConstraint.get("hasValueLiteral") != null && !isConstraintOnType) {
      List<String> valueLiteralList = (List<String>) theConstraint.get("hasValueLiteral");
      if (!valueLiteralList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theHasValueLiteral", valueLiteralList);

        addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "HasValueLiteral", whereClause, constraintType,
                buildArgArray(constraintType,
                        Arrays.asList(paramSetId, focusLabel,
                                propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
                                propOrRel, severity, propOrRel, customMsg) ,
                        Arrays.asList(paramSetId,
                                propOrRel, (String) theConstraint.get("propShapeUid"),
                                propOrRel, severity, propOrRel, customMsg)));

        //ADD constraint to the list
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.HAS_VALUE),
                valueLiteralList));
      }
    }

    if (theConstraint.get("disjointProps") != null && !isConstraintOnType) {
      List<String> disjointPropList = (List<String>) theConstraint.get("disjointProps");
      if (!disjointPropList.isEmpty()) {

        List<String> classBasedList = Arrays.asList(focusLabel,propOrRel,focusLabel,
                (String) theConstraint.get("propShapeUid"),propOrRel, severity, propOrRel, customMsg);

        List<String> queryBasedList = Arrays.asList(propOrRel,(String) theConstraint.get("propShapeUid"),
                propOrRel, severity, propOrRel, customMsg);

        addQueriesForTriggerWithDynamicProps(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "HasOverlappingValuesinProps", whereClause, constraintType, disjointPropList,
                buildArgArray(constraintType, classBasedList, queryBasedList));


        addQueriesForTriggerWithDynamicProps(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "HasOverlappingValuesinRels", whereClause, constraintType, disjointPropList,
                buildArgArray(constraintType, classBasedList, queryBasedList));

        //ADD constraint to the list
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.HAS_VALUE),
                disjointPropList));
      }
    }

    if (theConstraint.get("rangeKind") != null && !isConstraintOnType) {
      if (theConstraint.get("rangeKind").equals(SHACL.LITERAL.stringValue())) {
        addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "GetRangeIRIKind", whereClause, constraintType,
                buildArgArray(constraintType,
                        Arrays.asList(focusLabel, propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
                                propOrRel, severity, propOrRel, customMsg),
                        Arrays.asList(propOrRel, (String) theConstraint.get("propShapeUid"),
                                propOrRel, severity, propOrRel, customMsg)));

      } else if (theConstraint.get("rangeKind").equals(SHACL.BLANK_NODE_OR_IRI.stringValue()) ||
              theConstraint.get("rangeKind").equals(SHACL.IRI.stringValue())) {
        addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "GetRangeLiteralKind", whereClause, constraintType,
                buildArgArray(constraintType,
                        Arrays.asList(focusLabel, propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
                                propOrRel, severity, propOrRel, customMsg),
                        Arrays.asList(propOrRel, (String) theConstraint.get("propShapeUid"),
                                propOrRel, severity, propOrRel, customMsg)));
      }

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
              printConstraintType(SHACL.NODE_KIND),
              (gc != null && gc.getGraphMode() == GRAPHCONF_MODE_RDF) ? theConstraint.get("rangeKind") :
                      ((String) theConstraint.get("rangeKind"))
                              .substring(URIUtil.getLocalNameIndex((String) theConstraint.get("rangeKind")))));
    }

    if (theConstraint.get("rangeType") != null && !theConstraint.get("rangeType")
        .equals("") && !isConstraintOnType) {

      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "GetRangeType1", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(focusLabel,
                              propOrRel,
                              translateUri((String) theConstraint.get("rangeType"), tx, gc),
                              focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity,
                              translateUri((String) theConstraint.get("rangeType"), tx, gc), customMsg),
                      Arrays.asList(propOrRel,
                              translateUri((String) theConstraint.get("rangeType"), tx, gc),
                              (String) theConstraint.get("propShapeUid"), propOrRel, severity,
                              translateUri((String) theConstraint.get("rangeType"), tx, gc), customMsg)));

      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "GetRangeType2", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(focusLabel, propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
                              propOrRel,propOrRel, severity, propOrRel ,customMsg),
                      Arrays.asList(propOrRel, (String) theConstraint.get("propShapeUid"),
                              propOrRel,propOrRel, severity, propOrRel ,customMsg)));

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
              printConstraintType(SHACL.CLASS),
              translateUri((String) theConstraint.get("rangeType"), tx, gc)));
    }

    //Logic: if there's in,  it takes precedence over the not/in but only one will be compiled
    if ((theConstraint.containsKey("inLiterals") || theConstraint.containsKey("notInLiterals")) && !isConstraintOnType) {
      List<String> valueLiteralList = (theConstraint.containsKey("inLiterals")?(List<String>) theConstraint.get("inLiterals"):(List<String>) theConstraint.get("notInLiterals"));
      //If empty ignore constraint (most likely an error)
      if (!valueLiteralList.isEmpty()) {
        String paramSetId =
                theConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theInLiterals", valueLiteralList);

        addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "InLiterals", whereClause, constraintType,
                buildArgArray(constraintType,
                        Arrays.asList(paramSetId, focusLabel,
                                propOrRel, (theConstraint.containsKey("inLiterals")?"not":"") , focusLabel,
                                (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel, customMsg) ,
                        Arrays.asList(paramSetId,
                                propOrRel, (theConstraint.containsKey("inLiterals")?"not":"") ,
                                (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel, customMsg)));

        //ADD constraint to the list
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.IN),
                (theConstraint.containsKey("inLiterals")?valueLiteralList:"not " + valueLiteralList.toString())));
      }
    }

    if (theConstraint.containsKey("inUris") || theConstraint.containsKey("notInUris")) {
      List<String> valueUriList = (theConstraint.containsKey("inUris") ? (List<String>) theConstraint.get("inUris") : (List<String>) theConstraint.get("notInUris"));
      if (!valueUriList.isEmpty()) {
        String paramSetId =
                theConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);

        if (isConstraintOnType){
          if(typesAsLabels()) {

            addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    "TypeAsLabelInUris", whereClause, constraintType,
                    buildArgArray(constraintType,
                            Arrays.asList(paramSetId, focusLabel, (theConstraint.containsKey("inUris")?"not":""), focusLabel,
                                    (String) theConstraint.get("propShapeUid"), severity, customMsg) ,
                            Arrays.asList(paramSetId, (theConstraint.containsKey("inUris")?"not":""),
                                    (String) theConstraint.get("propShapeUid"), severity, customMsg)));

            params.put("theInTypeTranslatedUris", translateUriList(valueUriList));
          }
          //this is not an if/else because both can coexist
          if(typesAsRelToNodes()){

            addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    "TypeAsNodeInUris", whereClause, constraintType,
                    buildArgArray(constraintType,
                            Arrays.asList(paramSetId, focusLabel, propOrRel,
                                    (theConstraint.containsKey("inUris")?"not":""), focusLabel,
                                    (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel, customMsg) ,
                            Arrays.asList(paramSetId, propOrRel,
                                    (theConstraint.containsKey("inUris")?"not":""),
                                    (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel, customMsg)));

            params.put("theInTypeUris", valueUriList);
          }
        } else {
          addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                  "InUris", whereClause, constraintType,
                  buildArgArray(constraintType,
                          Arrays.asList(paramSetId, focusLabel,
                                  propOrRel, (theConstraint.containsKey("inUris")?"not":"") , focusLabel, (String) theConstraint.get("propShapeUid"),
                                  propOrRel, severity, propOrRel, customMsg) ,
                          Arrays.asList(paramSetId, propOrRel, (theConstraint.containsKey("inUris")?"not":"") ,
                                  (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel, customMsg)));

          params.put("theInUris", valueUriList);
        }
      }

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
              printConstraintType(SHACL.IN),
              (theConstraint.containsKey("inUris")?(isConstraintOnType&&typesAsLabels()?translateUriList(valueUriList):valueUriList):
                      "not " + (isConstraintOnType&&typesAsLabels()?translateUriList(valueUriList):valueUriList))));
    }

    if (theConstraint.get("pattern") != null && !isConstraintOnType) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.PATTERN.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("theRegex", (String) theConstraint.get("pattern"));


      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "Regex", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(paramSetId, focusLabel, propOrRel, propOrRel, focusLabel,
                              (String) theConstraint.get("propShapeUid"), propOrRel, severity, customMsg) ,
                      Arrays.asList(paramSetId, propOrRel, propOrRel,
                              (String) theConstraint.get("propShapeUid"), propOrRel, severity, customMsg)));

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
              printConstraintType(SHACL.PATTERN),
              theConstraint.get("pattern")));

    }

    if (theConstraint.get("minCount")!=null || theConstraint.get("maxCount")!=null) {
      String paramSetId =
              theConstraint.get("propShapeUid") + "_" + SHACL.MIN_COUNT.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("minCount", theConstraint.get("minCount"));
      params.put("maxCount", theConstraint.get("maxCount"));

      String constraintSHACLType = (theConstraint.get("minCount")!=null&&theConstraint.get("maxCount")!=null?
              SHACL_COUNT_CONSTRAINT_COMPONENT:
              (theConstraint.get("minCount")!=null?SHACL.MIN_COUNT_CONSTRAINT_COMPONENT.stringValue():
                      SHACL.MAX_COUNT_CONSTRAINT_COMPONENT.stringValue()));

      if (!(boolean) theConstraint.get("inverse")) {
        if (isConstraintOnType){
          if(typesAsLabels()) {
            addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    "TypeAsLabelMinCardinality", whereClause, constraintType,
                    buildArgArray(constraintType,
                            Arrays.asList(paramSetId, focusLabel,
                                    (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                    (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                    focusLabel, (String) theConstraint.get("propShapeUid"),
                                    constraintSHACLType,
                                    propOrRel, severity, customMsg) ,
                            Arrays.asList(paramSetId,
                                    (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                    (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                    (String) theConstraint.get("propShapeUid"),
                                    constraintSHACLType,
                                    propOrRel, severity, customMsg)));

          }
          //this is not an if/else because both can coexist
          if(typesAsRelToNodes()){

            addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    "TypeAsNodeMinCardinality", whereClause, constraintType,
                    buildArgArray(constraintType,
                            Arrays.asList(paramSetId, focusLabel,
                                    (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                    propOrRel,
                                    (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                    focusLabel, (String) theConstraint.get("propShapeUid"),
                                    constraintSHACLType, propOrRel,
                                    propOrRel,
                                    severity, customMsg) ,
                            Arrays.asList(paramSetId,
                                    (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                    propOrRel,
                                    (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                    (String) theConstraint.get("propShapeUid"),
                                    constraintSHACLType, propOrRel,
                                    propOrRel,
                                    severity, customMsg)));

          }
        } else {

          addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                  "MinCardinality1", whereClause, constraintType,
                  buildArgArray(constraintType,
                          Arrays.asList(paramSetId, focusLabel,
                                  (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                  propOrRel, propOrRel,
                                  (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                  focusLabel, (String) theConstraint.get("propShapeUid"),
                                  constraintSHACLType, propOrRel, propOrRel,
                                  propOrRel,
                                  severity, customMsg) ,
                          Arrays.asList(paramSetId,
                                  (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                  propOrRel,propOrRel,
                                  (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                  (String) theConstraint.get("propShapeUid"),
                                  constraintSHACLType, propOrRel, propOrRel,
                                  propOrRel,
                                  severity, customMsg)));

        }
      } else {
        // multivalued attributes not checked for cardinality in the case of inverse??
        // does not make sense in an LPG for properties but yes for rels
        addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "MinCardinality1Inverse", whereClause, constraintType,
                buildArgArray(constraintType,
                        Arrays.asList(paramSetId, focusLabel,
                                (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                propOrRel,
                                (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                focusLabel, (String) theConstraint.get("propShapeUid"),
                                constraintSHACLType,
                                propOrRel, propOrRel, severity, customMsg) ,
                        Arrays.asList(paramSetId,
                                (theConstraint.get("minCount")!=null?" toInteger(params.minCount) <= ":""),
                                propOrRel,
                                (theConstraint.get("maxCount")!=null?" <= toInteger(params.maxCount) ":""),
                                (String) theConstraint.get("propShapeUid"),
                                constraintSHACLType,
                                propOrRel, propOrRel, severity, customMsg)));

      }


      //ADD constraint to the list
      if(theConstraint.get("maxCount")!=null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MAX_COUNT), theConstraint.get("maxCount")));
      }
      if(theConstraint.get("minCount")!=null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MIN_COUNT), theConstraint.get("minCount")));
      }
    }

    if ((theConstraint.get("minStrLen") != null || theConstraint.get("maxStrLen") != null)
            && !isConstraintOnType ) {

      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_LENGTH.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("minStrLen", theConstraint.get("minStrLen"));
      params.put("maxStrLen", theConstraint.get("maxStrLen"));

      String constraintSHACLType = (theConstraint.get("minStrLen")!=null&&theConstraint.get("maxStrLen")!=null?
              SHACL_LENGTH_CONSTRAINT_COMPONENT:
              (theConstraint.get("minStrLen")!=null?SHACL.MIN_LENGTH_CONSTRAINT_COMPONENT.stringValue():
                      SHACL.MAX_LENGTH_CONSTRAINT_COMPONENT.stringValue()));

      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "StrLen", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(paramSetId,
                              focusLabel,
                              propOrRel,
                              theConstraint.get("minStrLen") != null ? " params.minStrLen <= " : "",
                              theConstraint.get("maxStrLen") != null ? " <= params.maxStrLen " : "",
                              focusLabel, (String) theConstraint.get("propShapeUid"),
                              constraintSHACLType, propOrRel, propOrRel, severity, customMsg) ,
                      Arrays.asList(paramSetId,
                              propOrRel,
                              theConstraint.get("minStrLen") != null ? " params.minStrLen <= " : "",
                              theConstraint.get("maxStrLen") != null ? " <= params.maxStrLen " : "",
                              (String) theConstraint.get("propShapeUid"),
                              constraintSHACLType, propOrRel, propOrRel, severity, customMsg)));

      //ADD constraint to the list
      if(theConstraint.get("minStrLen") != null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MIN_LENGTH),
                theConstraint.get("minStrLen")));
      }
      if(theConstraint.get("maxStrLen") != null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MAX_LENGTH),
                theConstraint.get("maxStrLen")));
      }

    }

    if ((theConstraint.get("minInc") != null || theConstraint.get("maxInc") != null
        || theConstraint.get("minExc") != null || theConstraint.get("maxExc") != null) && !isConstraintOnType) {

      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_EXCLUSIVE.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("min",
          theConstraint.get("minInc") != null ? theConstraint.get("minInc")
              : theConstraint.get("minExc"));
      params.put("max",
          theConstraint.get("maxInc") != null ? theConstraint.get("maxInc")
              : theConstraint.get("maxExc"));

      String constraintSHACLType = SHACL_VALUE_RANGE_CONSTRAINT_COMPONENT;
      if ((theConstraint.get("minExc") != null || theConstraint.get("minInc") != null ) &&
              (theConstraint.get("maxExc") != null || theConstraint.get("maxInc") != null )){
        constraintSHACLType = SHACL_VALUE_RANGE_CONSTRAINT_COMPONENT;
      } else if (theConstraint.get("minExc") != null){
        constraintSHACLType = SHACL.MIN_EXCLUSIVE_CONSTRAINT_COMPONENT.stringValue();
      }else if (theConstraint.get("minInc") != null){
        constraintSHACLType = SHACL.MIN_INCLUSIVE_CONSTRAINT_COMPONENT.stringValue();
      }else if (theConstraint.get("maxExc") != null){
        constraintSHACLType = SHACL.MAX_EXCLUSIVE_CONSTRAINT_COMPONENT.stringValue();
      }else if (theConstraint.get("maxInc") != null){
        constraintSHACLType = SHACL.MAX_INCLUSIVE_CONSTRAINT_COMPONENT.stringValue();
      }

      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "ValueRange", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(paramSetId,
                              focusLabel, propOrRel,
                              theConstraint.get("minInc") != null ? " params.min <="
                                      : (theConstraint.get("minExc") != null ? " params.min < " : ""),
                              theConstraint.get("maxInc") != null ? " <= params.max "
                                      : (theConstraint.get("maxExc") != null ? " < params.max " : ""),
                              focusLabel, (String) theConstraint.get("propShapeUid"),
                              constraintSHACLType, propOrRel, propOrRel, severity, customMsg) ,
                      Arrays.asList(paramSetId,
                              propOrRel,
                              theConstraint.get("minInc") != null ? " params.min <="
                                      : (theConstraint.get("minExc") != null ? " params.min < " : ""),
                              theConstraint.get("maxInc") != null ? " <= params.max "
                                      : (theConstraint.get("maxExc") != null ? " < params.max " : ""),
                              (String) theConstraint.get("propShapeUid"),
                              constraintSHACLType, propOrRel, propOrRel, severity, customMsg)));

      //ADD constraint to the list
      if (theConstraint.get("minInc") != null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MIN_INCLUSIVE),
                theConstraint.get("minInc")));
      }
      if (theConstraint.get("maxInc") != null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MAX_INCLUSIVE),
                theConstraint.get("maxInc")));
      }
      if (theConstraint.get("minExc") != null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MIN_EXCLUSIVE),
                theConstraint.get("minExc")));
      }
      if (theConstraint.get("maxExc") != null) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.MAX_EXCLUSIVE),
                theConstraint.get("maxExc")));
      }
    }


    if (theConstraint.containsKey("constraintType") && theConstraint.get("constraintType").equals("closedDefinitionPropList")){

      String paramSetId = theConstraint.get("nodeShapeUid") + "_" + SHACL.CLOSED.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      List<String> allowedPropsTranslated = new ArrayList<>();
      if(typesAsRelToNodes()){
        // if rdf import graph and types stored as rels then type rel is allowed implicitly (ok this?)
        allowedPropsTranslated.add(translateUri(RDF.TYPE.stringValue(), tx, gc));
      }

      if (theConstraint.get("ignoredProps") != null ){
        allowedPropsTranslated.addAll(translateUriList(((List<String>) theConstraint.get("ignoredProps"))));
      }
      if (theConstraint.get("definedProps") != null) {
        allowedPropsTranslated.addAll(translateUriList((List<String>) theConstraint.get("definedProps")));
      }
      params.put("allAllowedProps", allowedPropsTranslated);


      addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
              "NodeStructure", whereClause, constraintType,
              buildArgArray(constraintType,
                      Arrays.asList(paramSetId, focusLabel, focusLabel,
                       (String) theConstraint.get("nodeShapeUid"), "http://www.w3.org/ns/shacl#Violation", customMsg) ,
                      Arrays.asList(paramSetId,
                       (String) theConstraint.get("nodeShapeUid"), "http://www.w3.org/ns/shacl#Violation", customMsg)));

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
              printConstraintType(SHACL.IGNORED_PROPERTIES),
              translateUriList((List<String>) theConstraint.get("ignoredProps"))));

    }

    if (theConstraint.get("disjointClass") != null) {

      for (String uri : (List<String>) theConstraint.get("disjointClass")) {

        addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                "reqAndDisjointClass", whereClause, constraintType,
            buildArgArray(constraintType,
                Arrays.asList(focusLabel, "", translateUri(uri, tx, gc),focusLabel,
                        (String) theConstraint.get("nodeShapeUid"), SHACL.NOT_CONSTRAINT_COMPONENT.stringValue(), translateUri(uri, tx, gc),
                        "http://www.w3.org/ns/shacl#Violation", "not allowed", translateUri(uri, tx, gc), customMsg) ,
                Arrays.asList("", translateUri(uri, tx, gc),
                        (String) theConstraint.get("nodeShapeUid"), SHACL.NOT_CONSTRAINT_COMPONENT.stringValue(), translateUri(uri, tx, gc),
                        "http://www.w3.org/ns/shacl#Violation", "not allowed", translateUri(uri, tx, gc), customMsg)));

      }

      //ADD constraint to the list
      List<String> disjointClassesRaw = (List<String>) theConstraint.get("disjointClass");
      for (String x : disjointClassesRaw) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.NOT),
                translateUri(x, tx, gc)));
      }

    }

    if (theConstraint.get("reqClass") != null) {

      for (String uri : (List<String>) theConstraint.get("disjointProps")) {

//////// HERE <<<<<<<<<<<<<<<
//        addQueriesForTrigger(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
//                "reqAndDisjointClass", whereClause, constraintType,
//           buildArgArray(constraintType,
//                Arrays.asList(focusLabel, "not", translateUri(uri, tx, gc),focusLabel,
//                        (String) theConstraint.get("nodeShapeUid"), SHACL.CLASS_CONSTRAINT_COMPONENT.stringValue(), translateUri(uri, tx, gc),
//                        "http://www.w3.org/ns/shacl#Violation", "missing", translateUri(uri, tx, gc), customMsg) ,
//                Arrays.asList("not", translateUri(uri, tx, gc),
//                        (String) theConstraint.get("nodeShapeUid"), SHACL.CLASS_CONSTRAINT_COMPONENT.stringValue(), translateUri(uri, tx, gc),
//                        "http://www.w3.org/ns/shacl#Violation", "missing", translateUri(uri, tx, gc), customMsg)));

      }

      //ADD constraint to the list
      List<String> reqClassesRaw = (List<String>) theConstraint.get("reqClass");
      for (String x : reqClassesRaw) {
        vc.addConstraintToList(new ConstraintComponent(getTargetForList(constraintType, focusLabel, whereClause), propOrRel,
                printConstraintType(SHACL.CLASS),
                translateUri(x, tx, gc)));
      }

    }

  }

  private void validateWhereClause(String whereClause)  {
    try (Transaction tempTransaction = db.beginTx(50, TimeUnit.MILLISECONDS)) {

      tempTransaction.execute("explain " + "match (focus) where " + whereClause + " return focus limit 1 ");
    } catch (Exception e){
      throw new SHACLValidationException("Invalid cypher expression: \"" + whereClause + "\". The cypher fragment " +
              "in a sh:targetQuery element should form a valid query when embeeded in the following template: " +
              " \"match (focus) where <your cypher> return focus\"");
    }

  }

  private String[] buildArgArray(int constraintType, List<String> classBasedParamList, List<String> queryBasedParamList) {
    List<String> argsAsList = new ArrayList<>();
    argsAsList = constraintType == CLASS_BASED_CONSTRAINT ? classBasedParamList : queryBasedParamList;
    String[] argsAsArray = new String[argsAsList.size()];
    argsAsList.toArray(argsAsArray);
    return argsAsArray;
  }

  private String getTargetForList(int constraintType, String focusLabel, String whereClause) {
    String target = "";
    switch (constraintType) {
      case CLASS_BASED_CONSTRAINT:
        target = focusLabel;
      break;
      case QUERY_BASED_CONSTRAINT:
        target = whereClause;
      break;
      case GLOBAL_CONSTRAINT:
        target = "[Global]";
      break;
    }
    return target;
  }


  public void addQueriesForTrigger(ValidatorConfig vc, ArrayList<String> triggers, String queryId, String whereClause,
                                   int constraintType, String[] args) throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {
    vc.addQueryAndTriggers("Q_" + (vc.getIndividualGlobalQueries().size() + 1),
            getViolationQuery(queryId,false, whereClause, constraintType, Collections.emptyList(), args),
            getViolationQuery(queryId,true, whereClause, constraintType, Collections.emptyList(), args),
            triggers);
  }

  public void addQueriesForTriggerWithDynamicProps(ValidatorConfig vc, ArrayList<String> triggers, String queryId, String whereClause,
                                   int constraintType, List<String> propsOrRels, String[] args) throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {
    vc.addQueryAndTriggers("Q_" + (vc.getIndividualGlobalQueries().size() + 1),
            getViolationQuery(queryId,false, whereClause, constraintType, propsOrRels, args),
            getViolationQuery(queryId,true, whereClause, constraintType, propsOrRels, args),
            triggers);
  }

  private String printConstraintType(IRI i) {
    return (gc != null && gc.getGraphMode() == GRAPHCONF_MODE_RDF) ? "sh:" + i.getLocalName()
                      : i.getLocalName();
  }

  private List<String>  translateUriList(List<String> valueUriList)
          throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {
    List<String> result = new ArrayList<>();
    Iterator<String> iterator = valueUriList.iterator();
    while (iterator.hasNext()){
      result.add(translateUri(iterator.next(),tx,gc));
    }
    return result;
  }

  private boolean typesAsLabels() {
    return (gc == null || (gc!=null && (gc.getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_LABELS ||
            gc.getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES)));
  }

  private boolean typesAsRelToNodes() {
    return (gc!=null && (gc.getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_NODES ||
            gc.getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES));
  }

  private Map<String, Object> createNewSetOfParams(Map<String, Object> allParams, String id) {
    Map<String, Object> params = new HashMap<>();
    allParams.put(id, params);
    return params;
  }

  protected Iterator<Map<String, Object>> parseConstraints(InputStream is, RDFFormat format, Map<String,Object> props)
      throws IOException {
    Repository repo = new SailRepository(new MemoryStore());

    List<Map<String, Object>> constraints = new ArrayList<>();
    try (RepositoryConnection conn = repo.getConnection()) {
      conn.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX,
              props.containsKey("verifyUriSyntax") ? (Boolean) props
                      .get("verifyUriSyntax") : true);
      conn.begin();
      conn.add(new InputStreamReader(is), "http://neo4j.com/base/", format);
      conn.commit();

      TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, PROP_CONSTRAINT_QUERY);
      TupleQueryResult queryResult = tupleQuery.evaluate();
      while (queryResult.hasNext()) {
        Map<String, Object> record = new HashMap<>();
        BindingSet next = queryResult.next();

        Value path = next.hasBinding("invPath") ? next.getValue("invPath"): next.getValue("path");
        if (path instanceof BNode){
          log.debug("current version only processes single property paths");
        } else {
          record.put("item", path.stringValue());
          record.put("inverse", next.hasBinding("invPath"));
          if(!((Literal)next.getValue("targetIsQuery")).booleanValue()) {
            record.put("appliesToCat",
                    next.hasBinding("targetClass") ? next.getValue("targetClass").stringValue() : null);
          } else {
            record.put("appliesToQueryResult",
                    next.hasBinding("targetClass") ? next.getValue("targetClass").stringValue() : null);
          }
          record.put("rangeType",
              next.hasBinding("rangeClass") ? next.getValue("rangeClass").stringValue() : null);
          record.put("rangeKind",
              next.hasBinding("rangeKind") ? next.getValue("rangeKind").stringValue() : null);
          record.put("dataType",
              next.hasBinding("datatype") ? next.getValue("datatype").stringValue() : null);
          record.put("pattern",
              next.hasBinding("pattern") ? next.getValue("pattern").stringValue() : null);
          record.put("maxCount",
              next.hasBinding("maxCount") ? ((Literal) next.getValue("maxCount")).intValue()
                  : null);
          record.put("minCount",
              next.hasBinding("minCount") ? ((Literal) next.getValue("minCount")).intValue()
                  : null);
          record.put("minInc",
              next.hasBinding("minInc") ? ((Literal) next.getValue("minInc")).intValue() : null);
          record.put("minExc",
              next.hasBinding("minExc") ? ((Literal) next.getValue("minExc")).intValue() : null);
          record.put("maxInc",
              next.hasBinding("maxInc") ? ((Literal) next.getValue("maxInc")).intValue() : null);
          record.put("maxExc",
              next.hasBinding("maxExc") ? ((Literal) next.getValue("maxExc")).intValue() : null);
          if (next.hasBinding("disjointProps") && !next.getValue("disjointProps").stringValue()
                  .equals("")) {
            List<String> disjointProps = Arrays
                    .asList(next.getValue("disjointProps").stringValue().split("---"));
            record.put("disjointProps", disjointProps);
          }

          if (next.hasBinding("hasValueLiterals") && !next.getValue("hasValueLiterals")
              .stringValue()
              .equals("")) {
            List<String> hasValueLiterals = Arrays
                .asList(next.getValue("hasValueLiterals").stringValue().split("---"));
            record.put("hasValueLiteral", hasValueLiterals);
          }
          if (next.hasBinding("hasValueUris") && !next.getValue("hasValueUris").stringValue()
              .equals("")) {
            List<String> hasValueUris = Arrays
                .asList(next.getValue("hasValueUris").stringValue().split("---"));
            record.put("hasValueUri", hasValueUris);
          }

          if (next.hasBinding("isliteralIns")) {
            List<String> inVals = Arrays.asList(next.getValue("ins").stringValue().split("---"));
            Literal val = (Literal) next.getValue("isliteralIns");
            if (val.booleanValue()) {
              record.put("inLiterals", inVals);
            } else {
              record.put("inUris", inVals);
            }
          }

          if (next.hasBinding("isliteralNotIns")) {
            List<String> inVals = Arrays.asList(next.getValue("notins").stringValue().split("---"));
            Literal val = (Literal) next.getValue("isliteralNotIns");
            if (val.booleanValue()) {
              record.put("notInLiterals", inVals);
            } else {
              record.put("notInUris", inVals);
            }
          }

          record.put("minStrLen",
              next.hasBinding("minStrLen") ? ((Literal) next.getValue("minStrLen")).intValue()
                  : null);
          record.put("maxStrLen",
              next.hasBinding("maxStrLen") ? ((Literal) next.getValue("maxStrLen")).intValue()
                  : null);
          Value value = next.getValue("ps"); //if  this is null throw exception (?)
          if (value instanceof BNode) {
            //create artificial uri for blank node
            record.put("propShapeUid", BNODE_PREFIX + value.stringValue());
          } else {
            record.put("propShapeUid", value.stringValue());
          }

          record
              .put("severity", next.hasBinding("severity") ? next.getValue("severity").stringValue()
                  : "http://www.w3.org/ns/shacl#Violation");

          record.put("msg", next.hasBinding("msg") ? next.getValue("msg").stringValue() : "");

          constraints.add(record);
        }

      }

      //allowed and not-allowed properties in closed node shapes
      tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, NODE_CONSTRAINT_QUERY);
      queryResult = tupleQuery.evaluate();
      while (queryResult.hasNext()) {
        Map<String, Object> record = new HashMap<>();
        BindingSet next = queryResult.next();
        record.put("constraintType","closedDefinitionPropList");
        if(!((Literal)next.getValue("targetIsQuery")).booleanValue()) {
          record.put("appliesToCat",
                  next.hasBinding("targetClass") ? next.getValue("targetClass").stringValue() : null);
        } else {
          record.put("appliesToQueryResult",
                  next.hasBinding("targetClass") ? next.getValue("targetClass").stringValue() : null);
        }
        record
            .put("nodeShapeUid", next.hasBinding("ns") ? next.getValue("ns").stringValue() : null);
        if (next.hasBinding("definedProps")) {
          List<String> definedProps = new ArrayList<>();
          for (String s: next.getValue("definedProps").stringValue().split("---")) {
            if(!s.equals("")){
              definedProps.add(s);
            }
          }
          record.put("definedProps",definedProps);
        }
        if (next.hasBinding("ignoredProps")) {
          List<String> ignoredProps = new ArrayList<>();
          for (String s:next.getValue("ignoredProps").stringValue().split("---")){
            if(!s.equals("")){
              ignoredProps.add(s);
            }
          }
          record.put("ignoredProps", ignoredProps);
        }

        record.put("msg", next.hasBinding("msg") ? next.getValue("msg").stringValue() : "");

        constraints.add(record);
      }


      //additional node constraints (req and disjoint classes)
      tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, NODE_ADDITIONAL_CONSTRAINT_QUERY);
      queryResult = tupleQuery.evaluate();
      while (queryResult.hasNext()) {
        Map<String, Object> record = new HashMap<>();
        BindingSet next = queryResult.next();
        if(!((Literal)next.getValue("targetIsQuery")).booleanValue()) {
          record.put("appliesToCat",
                  next.hasBinding("targetClass") ? next.getValue("targetClass").stringValue() : null);
        } else {
          record.put("appliesToQueryResult",
                  next.hasBinding("targetClass") ? next.getValue("targetClass").stringValue() : null);
        }
        record
            .put("nodeShapeUid", next.hasBinding("ns") ? next.getValue("ns").stringValue() : null);

        if (next.hasBinding("class") && !next.getValue("class").stringValue().isEmpty()) {
          record.put("reqClass",
              Arrays.asList(next.getValue("class").stringValue().split("---")));
        }

        if (next.hasBinding("disjointclass") && !next.getValue("disjointclass").stringValue().isEmpty()) {
          record.put("disjointClass",
              Arrays.asList(next.getValue("disjointclass").stringValue().split("---")));
        }

        record.put("msg", next.hasBinding("msg") ? next.getValue("msg").stringValue() : "");

        constraints.add(record);
      }

    } catch (Exception e){
      log.error(e.getMessage());
    }
    return constraints.iterator();
  }

  private String getDatatypeCastExpressionPref(String dataType) {
    if (dataType.equals(XSD.BOOLEAN.stringValue())) {
      return "coalesce(toBoolean(toString(";
    } else if (dataType.equals(XSD.STRING.stringValue())) {
      return "coalesce(toString(";
    } else if (dataType.equals(XSD.INTEGER.stringValue())) {
      return "coalesce(toInteger(";
    } else if (dataType.equals(XSD.FLOAT.stringValue()) ||
            dataType.equals(XSD.DECIMAL.stringValue())) {
      return "coalesce(toFloat(";
    } else if (dataType.equals(XSD.DATE.stringValue())) {
      return "n10s.aux.dt.check('" + XSD.DATE.stringValue()+ "',";
    } else if (dataType.equals(XSD.DATETIME.stringValue())) {
      return "n10s.aux.dt.check('" + XSD.DATETIME.stringValue()+ "',";
    } else if (dataType.equals(WKTLITERAL_URI.stringValue())) {
      return "n10s.aux.dt.check('" +WKTLITERAL_URI.stringValue()+ "',";
    } else if (dataType.equals(XSD.ANYURI.stringValue())) {
      return "n10s.aux.dt.check('" +XSD.ANYURI.stringValue()+ "',";
    } else {
      return "";
    }
  }

  private String getDatatypeCastExpressionSuff(String dataType) {
    if (dataType.equals(XSD.BOOLEAN.stringValue())) {
      return ")) = x , false)";
    } else if (dataType.equals(XSD.STRING.stringValue())) {
      return ") = x , false)";
    } else if (dataType.equals(XSD.INTEGER.stringValue())) {
      return ") = x , false)";
    } else if (dataType.equals(XSD.FLOAT.stringValue())||
            dataType.equals(XSD.DECIMAL.stringValue())) {
      return ") = x , false)";
    } else if (dataType.equals(XSD.DATE.stringValue())) {
      return ")";
    } else if (dataType.equals(XSD.DATETIME.stringValue())) {
      return ")";
    } else if (dataType.equals(WKTLITERAL_URI.stringValue())) {
      return ")";
    } else if (dataType.equals(XSD.ANYURI.stringValue())) {
      return ")";
    }else {
      throw new SHACLValidationException(dataType + " data type is not supported for sh:datatype restrictions ");
    }
  }

  private void addCypherToValidationScripts(ValidatorConfig vc, List<String> triggers,
      String querystrGlobal, String querystrOnNodeset, String... args) {
    vc.addQueryAndTriggers("Q_" + (vc.getIndividualGlobalQueries().size() + 1),
        String.format(querystrGlobal, args), String.format(querystrOnNodeset, args), triggers);
  }

  private String getViolationQuery(String queryId, boolean tx, String customWhere, int constraintType,
                                   List<String> propOrRelList, String ... args) throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {
    String query = "";
    String nodeIdFragment = (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, ";
    String nodeTypeFragment = "'[all nodes]' as nodeType, ";
    if (constraintType == CLASS_BASED_CONSTRAINT) {
      nodeTypeFragment = (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") + " as nodeType, ";
    } else if (constraintType == QUERY_BASED_CONSTRAINT){
      nodeTypeFragment = "'[query-based selection]' as nodeType, ";
    }
    String shapeIdFragment = " '%s' as shapeId, " ;
    String propertyNameFragment = (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") + " as propertyName, ";
    String severityFragment = " '%s' as severity,";
    String customMsgFragment = " '%s' as customMsg";
    switch (queryId) {
      case "DataType":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_WHERE : CYPHER_MATCH_ALL_WHERE), tx,
                (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN " +
                        nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
                        + "' as propertyShape, focus.`%s` as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + " 'property value should be of type ' + " +
                        (nodesAreUriIdentified() ? " '%s' " : "n10s.rdf.getIRILocalName('%s')")
                        + " as message , " + customMsgFragment);
        break;
      case "DataType2":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_REL_WHERE : CYPHER_MATCH_ALL_REL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
                        + "' as propertyShape, " + (nodesAreUriIdentified() ? " x.uri " : " 'node id: ' + id(x) ")
                        + "as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
                        + " + ' should be a property, instead it  is a relationship' as message " +
                        " , " + customMsgFragment);
        break;
      case "HasValueOnTypeAsLabel":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind params.theHasTypeTranslatedUris as reqVal " +
                        " with focus, reqVal where not reqVal in labels(focus) "
                        + "RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT + "' as propertyShape" +
                        ", null as offendingValue, "
                        + ((gc == null || gc.getGraphMode() == GRAPHCONF_MODE_LPG) ? " 'type' " : " '" + RDF.TYPE.stringValue() +"' ")
                        + " as propertyName, " + severityFragment
                        + "'The required type ' + reqVal + ' could not be found as a label of the focus node ' as message  " +
                        " ," + customMsgFragment);

        break;
      case "HasValueOnTypeAsNode":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind params.theHasTypeUris as reqVal with focus, reqVal where not (focus)-[:`%s`]->({uri: reqVal}) "
                        + "RETURN "
                        + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT + "' as propertyShape, null as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + "'The required type ' + reqVal  + ' could not be found as value of relationship ' + "
                        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ")
                        + " as message   , " + customMsgFragment);
        break;

      case "HasValueUri":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind params.theHasValueUri as reqVal with focus, reqVal where not (focus)-[:`%s`]->({uri: reqVal}) "
                + "RETURN "
                + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT + "' as propertyShape, null as offendingValue, "
                + propertyNameFragment + severityFragment
                + "'The required value ' + reqVal  + ' could not be found as value of relationship ' + "
                + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ")
                + " as message  ," + customMsgFragment);
        break;
      case "HasValueLiteral":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind params.theHasValueLiteral as  reqVal with focus, reqVal where not reqVal in [] + focus.`%s` "
                        + "RETURN "
                        + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT + "' as propertyShape, null as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + "'The required value \"'+ reqVal + '\" was not found in property ' + " + (
                        shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ") + " as message " +
                        " ," + customMsgFragment);
        break;

      case "GetRangeIRIKind":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_WHERE : CYPHER_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " (focus)-[:`%s`]->() RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
                        + "' as propertyShape, null as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
                        + " + ' should be a property ' as message , " + customMsgFragment);
        break;
      case "GetRangeLiteralKind":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_WHERE : CYPHER_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " focus.`%s` is not null RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
                        + "' as propertyShape, null as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
                        + " + ' should be a relationship but it is a property' as message , " + customMsgFragment
        );
        break;

      case "GetRangeType1":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_REL_WHERE : CYPHER_MATCH_ALL_REL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT x:`%s` RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.CLASS_CONSTRAINT_COMPONENT
                        + "' as propertyShape, " + (nodesAreUriIdentified() ? " x.uri " : " id(x) ")
                        + " as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + " 'value should be of type ' + " + (shallIShorten()
                        ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") + " as message , " + customMsgFragment
        );
        break;

      case "GetRangeType2":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_WHERE : CYPHER_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " focus.`%s` is not null RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.CLASS_CONSTRAINT_COMPONENT
                        + "' as propertyShape, focus.`%s` as offendingValue, "
                        + propertyNameFragment + severityFragment
                        + "'%s should be a relationship but it is a property' as message , " + customMsgFragment
        );
        break;

      case "InLiterals":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind [] + focus.`%s` as val with focus, val where %s val in params.theInLiterals "
                + "RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'" + SHACL.IN_CONSTRAINT_COMPONENT
                + "' as propertyShape, val as offendingValue, "
                + propertyNameFragment + severityFragment
                + "'The value \"'+ val + '\" in property ' + " + (shallIShorten()
                ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
                + "+ 'is not in  the accepted list' as message , " + customMsgFragment);
        break;

      case "TypeAsLabelInUris":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind labels(focus) as val with focus, val " +
                " where val <> 'Resource' and %s val in params.theInTypeTranslatedUris "
                + "RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'" + SHACL.IN_CONSTRAINT_COMPONENT
                + "' as propertyShape, " + "val as offendingValue, "
                        + ((gc == null || gc.getGraphMode() == GRAPHCONF_MODE_LPG) ? " 'type' " : " '" + RDF.TYPE.stringValue() +"' ")
                        + " as propertyName, " + severityFragment
                + "'The label \"'+ val + '\" is not in  the accepted list' as message , " + customMsgFragment);
        break;

      case "TypeAsNodeInUris":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind [(focus)-[:`%s`]->(x) | x ] as val with focus, val where %s val.uri in params.theInTypeUris "
                + "RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'" + SHACL.IN_CONSTRAINT_COMPONENT
                + "' as propertyShape, " + (nodesAreUriIdentified() ? "val.uri" : "id(val)")
                + " as offendingValue, "
                + propertyNameFragment + severityFragment
                + "'The type \"'+ val.uri + '\" (node connected through property ' + " + (shallIShorten()
                ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
                + "+ ') is not in  the accepted list' as message , " + customMsgFragment);
        break;
      case "InUris":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true with params, focus unwind [(focus)-[:`%s`]->(x) | x ] as val with focus, val where %s val.uri in params.theInUris "
                + "RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'" + SHACL.IN_CONSTRAINT_COMPONENT
                + "' as propertyShape, " + (nodesAreUriIdentified() ? "val.uri" : "id(val)")
                + " as offendingValue, " + propertyNameFragment + severityFragment
                + "'The value \"'+ " + (nodesAreUriIdentified() ? " val.uri "
                : " 'node id: '  + id(val) ") + " + '\" in property ' + " + (shallIShorten()
                ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
                + "+ ' is not in  the accepted list' as message , " + customMsgFragment);
        break;
      case "Regex":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT all(x in [] +  coalesce(focus.`%s`,[]) where toString(x) =~ params.theRegex )  "
                + " UNWIND [x in [] +  coalesce(focus.`%s`,[]) where not toString(x) =~ params.theRegex ]  as offval "
                + "RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'" + SHACL.PATTERN_CONSTRAINT_COMPONENT
                + "' as propertyShape, offval as offendingValue, " + propertyNameFragment + severityFragment
                + "'The value of the property does not match the specified regular expression' as message , " + customMsgFragment);
        break;
      case "TypeAsLabelMinCardinality":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT %s size("
                + (nodesAreUriIdentified() ? " [x in labels(focus) where x <> 'Resource' ] " : " labels(focus) " )
                + ") %s RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'%s' as propertyShape,  'number of labels (' + size(" +
                (nodesAreUriIdentified() ? " [x in labels(focus) where x <> 'Resource' ] " : " labels(focus) ") +
                ") +') is outside the defined min-max limits'  as message, " + propertyNameFragment + severityFragment
                + " null as offendingValue , "  + customMsgFragment);
        break;
      case "TypeAsNodeMinCardinality":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT %s ( size([(focus)-[rel:`%s`]->() | rel ])) %s RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'%s' as propertyShape,  'type cardinality (' + coalesce(size([(focus)-[rel:`%s`]->() | rel ]),0) + ') is outside the defined min-max limits'  as message, "
                + propertyNameFragment + severityFragment
                + " null as offendingValue , " + customMsgFragment);
        break;
      case "MinCardinality1":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT %s ( size([(focus)-[rel:`%s`]->()| rel ]) +  size([] + coalesce(focus.`%s`, [])) ) %s RETURN "
                + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'%s' as propertyShape,  'cardinality (' + (coalesce(size([(focus)-[rel:`%s`]->()| rel ]),0) + coalesce(size([] + focus.`%s`),0)) + ') is outside the defined min-max limits'  as message, "
                + propertyNameFragment + severityFragment
                + "null as offendingValue , " + customMsgFragment);
        break;
      case "HasOverlappingValuesinProps":
        StringBuilder suffix1Props  = new StringBuilder(" true with focus , [] + coalesce(focus.`%s`, []) as __baseprop ");
        StringBuilder suffix2Props  = new StringBuilder(" where ");
        int pid = 0 ;
        for (String prop:propOrRelList){
          suffix1Props.append(", [] + coalesce(focus.`" + translateUri(prop, this.tx, gc) + "`, []) as __p" + pid + " ");
          suffix2Props.append(pid>0?" or ":"").append(" any(x IN __baseprop WHERE x in __p" + pid + " ) ");
          pid++;
        }

        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_WHERE : CYPHER_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and ": ""), //no 'and' after the customwhere because in this case is followed by a with
                suffix1Props.toString() + suffix2Props + " RETURN "
                        + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.DISJOINT_CONSTRAINT_COMPONENT + "' as propertyShape,  'property value overlaps with expected disjoint props'  as message, "
                        + propertyNameFragment + severityFragment
                        + "focus.`%s` as offendingValue , " + customMsgFragment);
        break;
      case "HasOverlappingValuesinRels":
        String suffix1Rels = " true with distinct focus, x ";
        StringBuilder suffix2Rels  = new StringBuilder(" where ");
        int rid = 0 ;
        for (String prop:propOrRelList){
          suffix2Rels.append(rid>0?" or ":"").append(" (focus)-[:`" + translateUri(prop, this.tx, gc) + "`]->(x) ");
          rid++;
        }
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_REL_WHERE : CYPHER_MATCH_ALL_REL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                suffix1Rels + suffix2Rels + " RETURN "
                        + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                        + "'" + SHACL.DISJOINT_CONSTRAINT_COMPONENT + "' as propertyShape,  'relationship target overlaps with expected disjoint rels'  as message, "
                        + propertyNameFragment + severityFragment + (nodesAreUriIdentified() ? " x.uri " : " id(x) ")
                        + " as offendingValue , " + customMsgFragment);
        break;
      case "MinCardinality1Inverse":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT %s size([(focus)<-[rel:`%s`]-() | rel ]) %s RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'%s' as propertyShape,  'incoming cardinality (' + coalesce(size([ (focus)<-[rel:`%s`]-()| rel ]),0) +') is outside the defined min-max limits' as message, "
                + propertyNameFragment + severityFragment
                + "null as offendingValue , " + customMsgFragment);
        break;
      case "StrLen":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT all(x in [] +  focus.`%s` where %s size(toString(x)) %s ) RETURN "
                + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'%s' as propertyShape, focus.`%s` as offendingValue, "
                + propertyNameFragment + severityFragment
                + "'' as message , " + customMsgFragment);
        break;
      case "ValueRange":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                "NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'%s' as propertyShape, focus.`%s` as offendingValue, "
                + propertyNameFragment + severityFragment
                + "'' as message , " + customMsgFragment);
        break;
      case "NodeStructure":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_WITH_PARAMS_MATCH_WHERE : CYPHER_WITH_PARAMS_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " true \n" +
                "UNWIND [ x in [(focus)-[r]->()| type(r)] where not x in params.allAllowedProps] + [ x in keys(focus) where "
                +
                (nodesAreUriIdentified() ? " x <> 'uri' and " : "")
                + " not x in params.allAllowedProps] as noProp\n"
                + "RETURN  " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
                + "'" + SHACL.CLOSED_CONSTRAINT_COMPONENT
                + "' as propertyShape, substring(reduce(result='', x in [] + coalesce(focus[noProp],[(focus)-[r]-(x) where type(r)=noProp | "
                + (nodesAreUriIdentified() ? " x.uri " : " id(x) ")
                + "]) | result + ', ' + x ),2) as offendingValue, "
                + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm(noProp)" : " noProp ") + " as propertyName, " + severityFragment
                + "'Closed type definition does not include this property/relationship' as message , " + customMsgFragment);
        break;
      case "reqAndDisjointClass":
        query = getQuery((constraintType == CLASS_BASED_CONSTRAINT ? CYPHER_MATCH_WHERE : CYPHER_MATCH_ALL_WHERE),
                tx, (constraintType == QUERY_BASED_CONSTRAINT ? customWhere + " and " : ""),
                " %s focus:`%s` RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment +
                "'%s' as propertyShape, '%s' as offendingValue, "
                + "'" + (gc !=null && gc.getHandleVocabUris() != GRAPHCONF_VOC_URI_IGNORE ? RDF.TYPE : "type") + "' as propertyName, " + severityFragment
                + " 'type %s: %s' as message , " + customMsgFragment);
        break;
    }

    return String.format(query, args);

  }

  private boolean nodesAreUriIdentified() {
    return gc != null ;
  }

  //TODO: not convinced about the GRAPHCONF_VOC_URI_MAP case below. Unit tests please.
  private boolean shallIShorten() {
    return gc != null && (gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT );
  }

  private String getQuery(String pref, boolean tx, String queryConstraintWhere, String suff) {
    return pref + (tx ? CYPHER_TX_INFIX : "") + queryConstraintWhere + suff;
  }

}
