package n10s.validation;

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
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

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

  String PROP_CONSTRAINT_QUERY = "prefix sh: <http://www.w3.org/ns/shacl#> \n" +
          "SELECT distinct ?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity (coalesce(?pmsg, ?nmsg,\"\") as ?msg)\n" +
          "?targetClass ?targetIsQuery ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc ?minStrLen \n" +
          "?maxStrLen (GROUP_CONCAT (distinct ?hasValueUri; separator=\"---\") AS ?hasValueUris) \n" +
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
          "  \n" +
          "} group by \n" +
          "?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity ?nmsg ?pmsg ?targetClass ?targetIsQuery " +
          "?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc ?minStrLen ?maxStrLen ?inFirst ?notInFirst";

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
          "SELECT ?ns (coalesce(?nmsg,\"\") as ?msg) ?targetClass (GROUP_CONCAT (distinct ?class; separator=\"---\") AS ?class)\n" +
          "(GROUP_CONCAT (distinct ?disjointclass; separator=\"---\") AS ?disjointclass)\n" +
          "{ ?ns a sh:NodeShape .\n" +
          "  \n" +
          "  { \n" +
          "    { \n" +
          "     ?ns sh:targetClass  ?targetClass \n" +
          "    }\n" +
          "    union \n" +
          "    { \n" +
          "     ?targetClass a rdfs:Class . filter(?targetClass = ?ns)\n" +
          "    }\n" +
          "  }\n" +
          "  optional { ?ns sh:message ?nmsg }\n" +
          "  optional { ?ns sh:not [ sh:class ?disjointclass ].  filter(isIRI(?disjointclass)) }\n" +
          "  optional { ?ns sh:class ?class .  filter(isIRI(?class)) }\n" +
          "  filter(bound(?disjointclass) || bound(?class))\n" +
          "} group by ?ns ?nmsg ?targetClass";

  private Transaction tx;
  private Log log;
  private GraphConfig gc;

  public SHACLValidator(Transaction transaction, Log l) {
    this.tx = transaction;
    this.log = l;
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
                (List<String>) theConstraint.get("hasValueLiteral")));
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
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
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
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
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
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
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
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
              printConstraintType(SHACL.IN),
              (theConstraint.containsKey("inUris")?(isConstraintOnType&&typesAsLabels()?translateUriList(valueUriList):valueUriList):
                      "not " + (isConstraintOnType&&typesAsLabels()?translateUriList(valueUriList):valueUriList))));
    }

    if (theConstraint.get("pattern") != null && !isConstraintOnType) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.PATTERN.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("theRegex", (String) theConstraint.get("pattern"));
      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getRegexViolationQuery(false), getRegexViolationQuery(true), paramSetId,
          focusLabel, propOrRel, propOrRel, focusLabel,
          (String) theConstraint.get("propShapeUid"), propOrRel, severity, customMsg);

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
              printConstraintType(SHACL.PATTERN),
              theConstraint.get("pattern")));

    }

    if (theConstraint.get("minCount") != null) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_COUNT.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("minCount", theConstraint.get("minCount"));

      if (!(boolean) theConstraint.get("inverse")) {
        if (isConstraintOnType){
          if(typesAsLabels()) {
            addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    getTypeAsLabelMinCardinalityViolationQuery(false), getTypeAsLabelMinCardinalityViolationQuery(true),
                    paramSetId, focusLabel, " toInteger(params.minCount) <= ",
                    focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity, customMsg);
          }
          //this is not an if/else because both can coexist
          if(typesAsRelToNodes()){
            addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    getTypeAsNodeMinCardinalityViolationQuery(false), getTypeAsNodeMinCardinalityViolationQuery(true), paramSetId, focusLabel,
                    " toInteger(params.minCount) <= ", propOrRel,
                    focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
                    severity, customMsg);
          }
        } else {
          addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                  getMinCardinality1ViolationQuery(false), getMinCardinality1ViolationQuery(true),
                  paramSetId, focusLabel,
                  " toInteger(params.minCount) <= ",
                  propOrRel, propOrRel,
                  focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel, propOrRel,
                  severity, customMsg);
        }
      } else {
        // multivalued attributes not checked for cardinality in the case of inverse??
        // does not make sense in an LPG for properties but yes for rels
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getMinCardinality1InverseViolationQuery(false),
            getMinCardinality1InverseViolationQuery(true),
            paramSetId, focusLabel,
            " toInteger(params.minCount) <= ",
            propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
            severity, customMsg);
      }


      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
              printConstraintType(SHACL.MIN_COUNT),
              theConstraint.get("minCount")));
    }

    if (theConstraint.get("maxCount") != null) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MAX_COUNT.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("maxCount", theConstraint.get("maxCount"));

      if (!(boolean) theConstraint.get("inverse")) {
        if (isConstraintOnType){
          if(typesAsLabels()) {
            addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    getTypeAsLabelMaxCardinalityViolationQuery(false), getTypeAsLabelMaxCardinalityViolationQuery(true),
                    paramSetId, focusLabel, " <= toInteger(params.maxCount) ", focusLabel,
                    (String) theConstraint.get("propShapeUid"), propOrRel, severity, customMsg);
          }
          //this is not an if/else because both can coexist
          if(typesAsRelToNodes()){
            addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                    getTypeAsNodeMaxCardinalityViolationQuery(false), getTypeAsNodeMaxCardinalityViolationQuery(true), paramSetId, focusLabel,
                    propOrRel, " <= toInteger(params.maxCount) ",
                    focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
                    severity, customMsg);
          }
        } else {
          addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
                  getMaxCardinality1ViolationQuery(false), getMaxCardinality1ViolationQuery(true),
                  paramSetId, focusLabel,
                  propOrRel, propOrRel,
                  " <= toInteger(params.maxCount) ",
                  focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel, propOrRel,
                  severity, customMsg);
        }
      } else {
        // multivalued attributes not checked for cardinality in the case of inverse??
        // does not make sense in an LPG
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getMaxCardinality1InverseViolationQuery(false),
            getMaxCardinality1InverseViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel,
            " <= toInteger(params.maxCount) ",
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
            severity, customMsg);
      }

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
              printConstraintType(SHACL.MAX_COUNT),
              theConstraint.get("maxCount")));
    }

    if ((theConstraint.get("minStrLen") != null || theConstraint.get("maxStrLen") != null)
            && !isConstraintOnType ) {

      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_LENGTH.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("minStrLen", theConstraint.get("minStrLen"));
      params.put("maxStrLen", theConstraint.get("maxStrLen"));

      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getStrLenViolationQuery(false), getStrLenViolationQuery(true), paramSetId,
          focusLabel,
          propOrRel,
          theConstraint.get("minStrLen") != null ? " params.minStrLen <= " : "",
          theConstraint.get("maxStrLen") != null ? " <= params.maxStrLen " : "",
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
          severity, customMsg);

      //ADD constraint to the list
      if(theConstraint.get("minStrLen") != null) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
                printConstraintType(SHACL.MIN_LENGTH),
                theConstraint.get("minStrLen")));
      }
      if(theConstraint.get("maxStrLen") != null) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
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

      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getValueRangeViolationQuery(false), getValueRangeViolationQuery(true), paramSetId,
          focusLabel, propOrRel,
          theConstraint.get("minInc") != null ? " params.min <="
              : (theConstraint.get("minExc") != null ? " params.min < " : ""),
          theConstraint.get("maxInc") != null ? " <= params.max "
              : (theConstraint.get("maxExc") != null ? " < params.max " : ""),
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
          severity, customMsg);

      //ADD constraint to the list
      if (theConstraint.get("minInc") != null) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
                printConstraintType(SHACL.MIN_INCLUSIVE),
                theConstraint.get("minInc")));
      }
      if (theConstraint.get("maxInc") != null) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
                printConstraintType(SHACL.MAX_INCLUSIVE),
                theConstraint.get("maxInc")));
      }
      if (theConstraint.get("minExc") != null) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
                printConstraintType(SHACL.MIN_EXCLUSIVE),
                theConstraint.get("minExc")));
      }
      if (theConstraint.get("maxExc") != null) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
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

      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getNodeStructureViolationQuery(false), getNodeStructureViolationQuery(true), paramSetId,
          focusLabel,
          focusLabel,
          (String) theConstraint.get("nodeShapeUid"), "http://www.w3.org/ns/shacl#Violation", customMsg);

      //ADD constraint to the list
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
              printConstraintType(SHACL.IGNORED_PROPERTIES),
              translateUriList((List<String>) theConstraint.get("ignoredProps"))));

    }

    if (theConstraint.get("disjointClass") != null) {

      for (String uri : (List<String>) theConstraint.get("disjointClass")) {
        //disjointClasses.add(translateUri(uri));
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel, translateUri(uri, tx, gc))),
            getDisjointClassesViolationQuery(false), getDisjointClassesViolationQuery(true),
            focusLabel, translateUri(uri, tx, gc),focusLabel,
            (String) theConstraint.get("nodeShapeUid"), translateUri(uri, tx, gc),
            "http://www.w3.org/ns/shacl#Violation", translateUri(uri, tx, gc), customMsg);
      }

      //ADD constraint to the list
      List<String> disjointClassesRaw = (List<String>) theConstraint.get("disjointClass");
      for (String x : disjointClassesRaw) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
                printConstraintType(SHACL.NOT),
                translateUri(x, tx, gc)));
      }

    }

    if (theConstraint.get("reqClass") != null) {

      for (String uri : (List<String>) theConstraint.get("reqClass")) {
        //disjointClasses.add(translateUri(uri));
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel, translateUri(uri, tx, gc))),
                getRequiredClassesViolationQuery(false), getRequiredClassesViolationQuery(true),
                 focusLabel, translateUri(uri, tx, gc),focusLabel,
                (String) theConstraint.get("nodeShapeUid"), translateUri(uri, tx, gc),
                "http://www.w3.org/ns/shacl#Violation", translateUri(uri, tx, gc), customMsg);
      }

      //ADD constraint to the list
      List<String> reqClassesRaw = (List<String>) theConstraint.get("reqClass");
      for (String x : reqClassesRaw) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
                printConstraintType(SHACL.CLASS),
                translateUri(x, tx, gc)));
      }

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
                                   int constraintType, String[] args) {
    vc.addQueryAndTriggers("Q_" + (vc.getIndividualGlobalQueries().size() + 1),
            getViolationQuery(queryId,false, whereClause, constraintType, args),
            getViolationQuery(queryId,true, whereClause, constraintType, args),
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
        record.put("appliesToCat", next.getValue("targetClass").stringValue());
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
        record.put("appliesToCat", next.getValue("targetClass").stringValue());
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
    } else if (dataType.equals(XSD.FLOAT.stringValue())) {
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
    } else if (dataType.equals(XSD.FLOAT.stringValue())) {
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
      return "";
    }
  }

  private void addCypherToValidationScripts(ValidatorConfig vc, List<String> triggers,
      String querystrGlobal, String querystrOnNodeset, String... args) {
    vc.addQueryAndTriggers("Q_" + (vc.getIndividualGlobalQueries().size() + 1),
        String.format(querystrGlobal, args), String.format(querystrOnNodeset, args), triggers);
  }

  private String getViolationQuery(String queryId, boolean tx, String customWhere, int constraintType,
                                   String ... args ){
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
                " exists(focus.`%s`) RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
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
                "exists(focus.`%s`) RETURN " + nodeIdFragment + nodeTypeFragment + shapeIdFragment
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
                .stringValue()
                + "' as propertyShape, " + (nodesAreUriIdentified() ? "val.uri" : "id(val)")
                + " as offendingValue, " + propertyNameFragment + severityFragment
                + "'The value \"'+ " + (nodesAreUriIdentified() ? " val.uri "
                : " 'node id: '  + id(val) ") + " + '\" in property ' + " + (shallIShorten()
                ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
                + "+ ' is not in  the accepted list' as message , " + customMsgFragment);
        break;
    }

    return String.format(query, args);

  }

//  private String getDataTypeViolationQuery(boolean tx) {
//    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_DATATYPE_V_SUFF());
//  }

//  private String getDataTypeViolationQuery2(boolean tx) {
//    return getQuery(CYPHER_MATCH_REL_WHERE, tx, CYPHER_DATATYPE2_V_SUFF());
//  }

//  private String getRangeIRIKindViolationQuery(boolean tx) {
//    return getQuery(CYPHER_MATCH_WHERE, tx, "", CYPHER_IRI_KIND_V_SUFF());
//  }

//  private String getRangeLiteralKindViolationQuery(boolean tx) {
//    return getQuery(CYPHER_MATCH_WHERE, tx, "", CYPHER_LITERAL_KIND_V_SUFF());
//  }

//  private String getRangeType1ViolationQuery(boolean tx) {
//    return getQuery(CYPHER_MATCH_REL_WHERE, tx, "", CYPHER_RANGETYPE1_V_SUFF());
//  }

//  private String getRangeType2ViolationQuery(boolean tx) {
//    return getQuery(CYPHER_MATCH_WHERE, tx, "", CYPHER_RANGETYPE2_V_SUFF());
//  }

  private String getRegexViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_REGEX_V_SUFF());
  }

//  private String getHasValueOnTypeAsLabelViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_HAS_VALUE_ON_TYPE_AS_LABEL_V_SUFF());
//  }

//  private String getHasValueOnTypeAsNodeViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_HAS_VALUE_ON_TYPE_AS_NODE_V_SUFF());
//  }

//  private String getHasValueUriViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_HAS_VALUE_URI_V_SUFF());
//  }


//  private String getHasValueLiteralViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_HAS_VALUE_LITERAL_V_SUFF());
//  }

//  private String getInLiteralsViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_IN_LITERAL_V_SUFF());
//  }

//  private String getInUrisViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_IN_URI_V_SUFF());
//  }

//  private String getTypeAsLabelInUrisViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_TYPE_AS_LABEL_IN_URI_V_SUFF());
//  }

//  private String getTypeAsNodeInUrisViolationQuery(boolean tx) {
//    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_TYPE_AS_NODE_IN_URI_V_SUFF());
//  }

  private String getMinCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_MIN_CARDINALITY1_V_SUFF());
  }

  private String getTypeAsLabelMinCardinalityViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_TYPE_AS_LABEL_MIN_CARDINALITY1_V_SUFF());
  }

  private String getTypeAsNodeMinCardinalityViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_TYPE_AS_NODE_MIN_CARDINALITY1_V_SUFF());
  }

  private String getMinCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF());
  }

  private String getMaxCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_MAX_CARDINALITY1_V_SUFF());
  }

  private String getTypeAsLabelMaxCardinalityViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_TYPE_AS_LABEL_MAX_CARDINALITY1_V_SUFF());
  }

  private String getTypeAsNodeMaxCardinalityViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_TYPE_AS_NODE_MAX_CARDINALITY1_V_SUFF());
  }

  private String getMaxCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF());
  }

  private String getStrLenViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_STRLEN_V_SUFF());
  }

  private String getValueRangeViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_VALRANGE_V_SUFF());
  }

  private String getNodeStructureViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, "", CYPHER_NODE_STRUCTURE_V_SUFF());
  }

  private String getDisjointClassesViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_WHERE, tx, "", CYPHER_NODE_DISJOINT_WITH_V_SUFF());
  }

  private String getRequiredClassesViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_WHERE, tx, "", CYPHER_NODE_REQUIRED_WITH_V_SUFF());
  }

  private boolean nodesAreUriIdentified() {
    return gc != null ;
  }

  //TODO: not convinced about the GRAPHCONF_VOC_URI_MAP case below. Unit tests please.
  private boolean shallIShorten() {
    return gc != null && (gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP);
  }

  private String getQuery(String pref, boolean tx, String queryConstraintWhere, String suff) {
    return pref + (tx ? CYPHER_TX_INFIX : "") + queryConstraintWhere + suff;
  }

//  private String CYPHER_DATATYPE_V_SUFF(int constraintType) {
//    return " NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN " +
//        (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
//        " as nodeType, '%s' as shapeId, '" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
//        + "' as propertyShape, focus.`%s` as offendingValue, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " as propertyName, '%s' as severity,"
//        + " 'property value should be of type ' + " +
//        (nodesAreUriIdentified() ? " '%s' " : "n10s.rdf.getIRILocalName('%s')")
//        + " as message , '%s' as customMsg";
//  }

//  private String CYPHER_DATATYPE2_V_SUFF() {
//    return " true RETURN " + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ")
//        + " as nodeId, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
//        " as nodeType, '%s' as shapeId, '" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
//        + "' as propertyShape, " + (nodesAreUriIdentified() ? " x.uri " : " 'node id: ' + id(x) ")
//        + "as offendingValue, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " as propertyName, '%s' as severity, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " + ' should be a property, instead it  is a relationship' as message " +
//            " , '%s' as customMsg";
//  }

//  private String CYPHER_IRI_KIND_V_SUFF() {
//    return " (focus)-[:`%s`]->() RETURN " + (nodesAreUriIdentified() ? " focus.uri "
//        : " id(focus) ") + " as nodeId, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
//        " as nodeType, '%s' as shapeId, '" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
//        + "' as propertyShape, null as offendingValue, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " as propertyName, '%s' as severity,"
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " + ' should be a property ' as message , '%s' as customMsg ";
//  }

//  private String CYPHER_LITERAL_KIND_V_SUFF() {
//    return " exists(focus.`%s`) RETURN " + (nodesAreUriIdentified() ? " focus.uri "
//        : " id(focus) ") + " as nodeId, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
//        " as nodeType, '%s' as shapeId, '" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
//        + "' as propertyShape, null as offendingValue, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " as propertyName, '%s' as severity,"
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " + ' should be a relationship ' as message , '%s' as customMsg ";
//  }

//  private String CYPHER_RANGETYPE1_V_SUFF() {
//    return "NOT x:`%s` RETURN " + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ")
//        + " as nodeId, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
//        " as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT
//        + "' as propertyShape, " + (nodesAreUriIdentified() ? " x.uri " : " id(x) ")
//        + " as offendingValue, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " as propertyName, '%s' as severity,"
//        + " 'value should be of type ' + " + (shallIShorten()
//        ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") + " as message , '%s' as customMsg ";
//  }

//  private String CYPHER_RANGETYPE2_V_SUFF() {
//    return "exists(focus.`%s`) RETURN " + (nodesAreUriIdentified() ? " focus.uri "
//        : " id(focus) ") + " as nodeId, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
//        " as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT
//        + "' as propertyShape, null as offendingValue, "
//        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//        + " as propertyName, '%s' as severity, "
//        + "'%s should be a relationship but it is a property' as message , '%s' as customMsg ";
//  }

  private String CYPHER_REGEX_V_SUFF() {
    return "NOT all(x in [] +  coalesce(focus.`%s`,[]) where toString(x) =~ params.theRegex )  "
        + " UNWIND [x in [] +  coalesce(focus.`%s`,[]) where not toString(x) =~ params.theRegex ]  as offval "
        + "RETURN "
        + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.PATTERN_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, offval as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "'the value of the property does not match the specified regular expression' as message , '%s' as customMsg ";
  }

//  private String CYPHER_HAS_VALUE_ON_TYPE_AS_LABEL_V_SUFF() {
//    return
//              " true with params, focus unwind params.theHasTypeTranslatedUris as reqVal " +
//                      " with focus, reqVal where not reqVal in labels(focus) "
//                      + "RETURN "
//                      + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//                      + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//                      + "as nodeType, '%s' as shapeId, '"
//                      + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT.stringValue() + "' as propertyShape" +
//                      ", null as offendingValue, "
//                      + ((gc == null || gc.getGraphMode() == GRAPHCONF_MODE_LPG) ? " 'type' " : " '" + RDF.TYPE.stringValue() +"' ")
//                      + " as propertyName, '%s' as severity, "
//                      + "'The required type ' + reqVal + ' could not be found as a label of the focus node ' as message  " +
//                      " , '%s' as customMsg";
//
//  }

//  private String CYPHER_HAS_VALUE_ON_TYPE_AS_NODE_V_SUFF() {
//
//    return
//            " true with params, focus unwind params.theHasTypeUris as reqVal with focus, reqVal where not (focus)-[:`%s`]->({uri: reqVal}) "
//                    + "RETURN "
//                    + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//                    + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//                    + " as nodeType, '%s' as shapeId, '" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT
//                    .stringValue()
//                    + "' as propertyShape, null as offendingValue, "
//                    + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//                    + " as propertyName, '%s' as severity, "
//                    + "'The required type ' + reqVal  + ' could not be found as value of relationship ' + "
//                    + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ")
//                    + " as message   , '%s' as customMsg ";
//  }

//  private String CYPHER_HAS_VALUE_URI_V_SUFF() {
//    return                                                                                      //not reqVal in [(focus)-[:`%s`]->(v) | v.uri ]
//        " true with params, focus unwind params.theHasValueUri as reqVal with focus, reqVal where not (focus)-[:`%s`]->({uri: reqVal}) "
//            + "RETURN "
//            + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as nodeType, '%s' as shapeId, '" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT
//            .stringValue()
//            + "' as propertyShape, null as offendingValue, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as propertyName, '%s' as severity, "
//            + "'The required value ' + reqVal  + ' could not be found as value of relationship ' + "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ")
//            + " as message  , '%s' as customMsg ";
//  }

//  private String CYPHER_HAS_VALUE_LITERAL_V_SUFF() {
//    return
//        " true with params, focus unwind params.theHasValueLiteral as  reqVal with focus, reqVal where not reqVal in [] + focus.`%s` "
//            + "RETURN "
//            + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as nodeType, '%s' as shapeId, '" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT
//            .stringValue()
//            + "' as propertyShape, null as offendingValue, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as propertyName, '%s' as severity, "
//            + "'The required value \"'+ reqVal + '\" was not found in property ' + " + (
//            shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ") + " as message " +
//                " , '%s' as customMsg ";
//  }

//  private String CYPHER_IN_LITERAL_V_SUFF() {
//    return
//        " true with params, focus unwind [] + focus.`%s` as val with focus, val where %s val in params.theInLiterals "
//            + "RETURN "
//            + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
//            .stringValue()
//            + "' as propertyShape, val as offendingValue, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as propertyName, '%s' as severity, "
//            + "'The value \"'+ val + '\" in property ' + " + (shallIShorten()
//            ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
//            + "+ 'is not in  the accepted list' as message , '%s' as customMsg ";
//  }

//  private String CYPHER_IN_URI_V_SUFF() {
//    return
//        " true with params, focus unwind [(focus)-[:`%s`]->(x) | x ] as val with focus, val where %s val.uri in params.theInUris "
//            + "RETURN "
//            + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
//            .stringValue()
//            + "' as propertyShape, " + (nodesAreUriIdentified() ? "val.uri" : "id(val)")
//            + " as offendingValue, "
//            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//            + " as propertyName, '%s' as severity, "
//            + "'The value \"'+ " + (nodesAreUriIdentified() ? " val.uri "
//            : " 'node id: '  + id(val) ") + " + '\" in property ' + " + (shallIShorten()
//            ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
//            + "+ ' is not in  the accepted list' as message , '%s' as customMsg ";
//  }

//  private String CYPHER_TYPE_AS_LABEL_IN_URI_V_SUFF() {
//    // TODO: this query could be optimised by unfolding all the values in the list and checking focus:Type1 and focus:Type2 ... instead of using unwind.
//    return
//            " true with params, focus unwind labels(focus) as val with focus, val " +
//                    " where val <> 'Resource' and %s val in params.theInTypeTranslatedUris "
//                    + "RETURN "
//                    + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//                    + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//                    + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
//                    .stringValue()
//                    + "' as propertyShape, " + "val as offendingValue, "
//                    + ((gc == null || gc.getGraphMode() == GRAPHCONF_MODE_LPG) ? " 'type' " : " '" + RDF.TYPE.stringValue() +"' ")
//                    + " as propertyName, '%s' as severity, "
//                    + "'The label \"'+ val + '\" is not in  the accepted list' as message , '%s' as customMsg ";
//  }

  //paramSetId, focusLabel, propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),propOrRel, severity, propOrRel

//  private String CYPHER_TYPE_AS_NODE_IN_URI_V_SUFF() {
//    return
//            " true with params, focus unwind [(focus)-[:`%s`]->(x) | x ] as val with focus, val where %s val.uri in params.theInTypeUris "
//                    + "RETURN "
//                    + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") + " as nodeId, "
//                    + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//                    + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
//                    .stringValue()
//                    + "' as propertyShape, " + (nodesAreUriIdentified() ? "val.uri" : "id(val)")
//                    + " as offendingValue, "
//                    + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
//                    + " as propertyName, '%s' as severity, "
//                    + "'The type \"'+ val.uri + '\" (node connected through property ' + " + (shallIShorten()
//                    ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
//                    + "+ ') is not in  the accepted list' as message , '%s' as customMsg ";
//  }

  private String CYPHER_VALRANGE_V_SUFF() {
    return "NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN " + (nodesAreUriIdentified()
        ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_EXCLUSIVE_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "'' as message , '%s' as customMsg ";
  }

  private String CYPHER_MIN_CARDINALITY1_V_SUFF() {
    return "NOT %s ( size((focus)-[:`%s`]->()) +  size([] + coalesce(focus.`%s`, [])) )  RETURN "
        + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'cardinality (' + (coalesce(size((focus)-[:`%s`]->()),0) + coalesce(size([] + focus.`%s`),0)) + ') too low'  as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "null as offendingValue , '%s' as customMsg ";
  }

  private String CYPHER_TYPE_AS_NODE_MIN_CARDINALITY1_V_SUFF() {
    return "NOT %s ( size((focus)-[:`%s`]->()))  RETURN "
            + " focus.uri as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
            + "' as propertyShape,  'type cardinality (' + coalesce(size((focus)-[:`%s`]->()),0) + ') is too low'  as message, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + "null as offendingValue , '%s' as customMsg ";
  }

  private String CYPHER_TYPE_AS_NODE_MAX_CARDINALITY1_V_SUFF() {
    return "NOT ( size((focus)-[:`%s`]->())) %s  RETURN "
            + " focus.uri as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
            + "' as propertyShape,  'type cardinality (' + coalesce(size((focus)-[:`%s`]->()),0) + ') is too high'  as message, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + "null as offendingValue , '%s' as customMsg ";
  }

  private String CYPHER_TYPE_AS_LABEL_MIN_CARDINALITY1_V_SUFF() {
    return "NOT %s size("
            + (nodesAreUriIdentified() ? " [x in labels(focus) where x <> 'Resource' ] " : " labels(focus) " )
            + ")  RETURN "
            + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
            " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
            + "' as propertyShape,  'number of labels (' + size(" +
            (nodesAreUriIdentified() ? " [x in labels(focus) where x <> 'Resource' ] " : " labels(focus) ") +
            ") +') too low'  as message, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + " null as offendingValue , '%s' as customMsg ";
  }

  private String CYPHER_TYPE_AS_LABEL_MAX_CARDINALITY1_V_SUFF() {
    return "NOT size("
            + (nodesAreUriIdentified() ? " [x in labels(focus) where x <> 'Resource' ] " : " labels(focus) ")
            + ")  %s   RETURN "
            + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
            " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
            + "' as propertyShape,  'number of labels (' + size(" +
            (nodesAreUriIdentified() ? " [x in labels(focus) where x <> 'Resource' ] " : " labels(focus) ") +
            ") + ') is too high' as message, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + " null as offendingValue , '%s' as customMsg ";
  }

  private String CYPHER_MAX_CARDINALITY1_V_SUFF() {
    return "NOT (size((focus)-[:`%s`]->()) + size([] + coalesce(focus.`%s`,[]))) %s  RETURN " + (
        nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'cardinality (' + (coalesce(size((focus)-[:`%s`]->()),0) + coalesce(size([] + focus.`%s`),0)) + ') is too high' as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "null as offendingValue , '%s' as customMsg ";
  }

  private String CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF() {   //This will need fixing, the coalesce in first line + the changes to cardinality
    return "NOT %s size((focus)<-[:`%s`]-()) RETURN " + (nodesAreUriIdentified() ? " focus.uri "
        : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'incoming cardinality (' + coalesce(size((focus)<-[:`%s`]-()),0) +') is too low' as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "null as offendingValue , '%s' as customMsg ";
  }

  private String CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF() {   //Same as previous
    return "NOT size((focus)<-[:`%s`]-()) %s RETURN " + (nodesAreUriIdentified() ? " focus.uri "
        : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'incoming cardinality (' + coalesce(size((focus)<-[:`%s`]-()),0) + ') is too high' as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "null as offendingValue , '%s' as customMsg ";
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
    return "NOT all(x in [] +  focus.`%s` where %s size(toString(x)) %s ) RETURN " + (
        nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.MAX_LENGTH_CONSTRAINT_COMPONENT
        + "' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "'' as message , '%s' as customMsg ";
  }

  private String CYPHER_NODE_STRUCTURE_V_SUFF() {
    return " true \n" +
        "UNWIND [ x in [(focus)-[r]->()| type(r)] where not x in params.allAllowedProps] + [ x in keys(focus) where "
        +
        (nodesAreUriIdentified() ? " x <> 'uri' and " : "")
        + " not x in params.allAllowedProps] as noProp\n"
        + "RETURN  " + (nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
        " as nodeId , " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '"
        + SHACL.CLOSED_CONSTRAINT_COMPONENT.stringValue()
        + "' as propertyShape, substring(reduce(result='', x in [] + coalesce(focus[noProp],[(focus)-[r]-(x) where type(r)=noProp | "
        + (nodesAreUriIdentified() ? " x.uri " : " id(x) ")
        + "]) | result + ', ' + x ),2) as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm(noProp)" : " noProp ") +
        " as propertyName, '%s' as severity, "
        + "'Closed type definition does not include this property/relationship' as message , '%s' as customMsg ";
  }

  private String CYPHER_NODE_DISJOINT_WITH_V_SUFF() {
    return " focus:`%s` RETURN " + (
        nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.NOT_CONSTRAINT_COMPONENT
        + "' as propertyShape, '%s' as offendingValue, "
        + " '-' as propertyName, '%s' as severity, "
        + " 'type not allowed: ' + '%s' as message , '%s' as customMsg ";
  }

  private String CYPHER_NODE_REQUIRED_WITH_V_SUFF() {
    return " not focus:`%s` RETURN " + (
            nodesAreUriIdentified() ? " focus.uri " : " id(focus) ") +
            " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
            " as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT
            + "' as propertyShape, 'not %s' as offendingValue, "
            + " '-' as propertyName, '%s' as severity, "
            + " 'type missing: ' + '%s' as message , '%s' as customMsg ";
  }
}
