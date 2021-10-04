package n10s.validation;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_KEEP;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_MAP;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN_STRICT;
import static n10s.graphconfig.Params.WKTLITERAL_URI;
import static n10s.utils.UriUtils.translateUri;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.UriUtils.UriNamespaceHasNoAssociatedPrefix;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
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
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

public class SHACLValidator {

  private static final String CYPHER_TX_INFIX = " focus in $touchedNodes AND ";

  private static final String CYPHER_MATCH_WHERE = "MATCH (focus:`%s`) WHERE ";
  private static final String CYPHER_MATCH_REL_WHERE = "MATCH (focus:`%s`)-[r:`%s`]->(x) WHERE ";
  private static final String CYPHER_WITH_PARAMS_MATCH_WHERE = "WITH $`%s` as params MATCH (focus:`%s`) WHERE ";
  private static final String BNODE_PREFIX = "bnode://id/";

  String PROP_CONSTRAINT_QUERY = "prefix sh: <http://www.w3.org/ns/shacl#> \n"
      + "SELECT distinct ?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity \n"
      + "?targetClass ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc ?minStrLen \n"
      + "?maxStrLen (GROUP_CONCAT (distinct ?hasValueUri; separator=\"---\") AS ?hasValueUris) \n"
      + "(GROUP_CONCAT (distinct ?hasValueLiteral; separator=\"---\") AS ?hasValueLiterals) \n"
      + "(GROUP_CONCAT (distinct ?in; separator=\"---\") AS ?ins) \n"
      + "(isLiteral(?inFirst) as ?isliteralIns)\n"
      + "{ ?ns a ?shapeOrNodeShape ;\n"
      + "     sh:node?/sh:property ?ps .\n"
      + "  filter ( ?shapeOrNodeShape = sh:Shape || ?shapeOrNodeShape = sh:NodeShape )\n"
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

  String NODE_CONSTRAINT_QUERY = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
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

  String NODE_ADDITIONAL_CONSTRAINT_QUERY = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
      + "prefix sh: <http://www.w3.org/ns/shacl#>  \n"
      + "SELECT ?ns ?targetClass (GROUP_CONCAT (distinct ?class; separator=\"---\") AS ?class)\n"
      + "(GROUP_CONCAT (distinct ?disjointclass; separator=\"---\") AS ?disjointclass)\n"
      + "{ ?ns a sh:NodeShape .\n"
      + "  \n"
      + "   optional { \n"
      + "     ?ns sh:targetClass  ?targetClass \n"
      + "   }\n"
      + "   \n"
      + "   optional { \n"
      + "     ?targetClass a rdfs:Class . filter(?targetClass = ?ns)\n"
      + "   }\n"
      + "  \n"
      + "  optional { ?ns sh:not [ sh:class ?disjointclass ].  filter(isIRI(?disjointclass)) }\n"
      + "  optional { ?ns sh:class ?class .  filter(isIRI(?class)) }\n"
      + "  filter(bound(?disjointclass) || bound(?class))\n"
      + "} group by ?ns ?targetClass";

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
      if (propConstraint.get("appliesToCat") == null) {
        log.debug(
            "Only class-based targets (sh:targetClass) and implicit class targets are validated.");
      } else if (propConstraint.containsKey("item") && propConstraint.get("item")
          .equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
        log.debug(
            "Constraints on rdf:type are ignored  (temporary solution until we figure out how can they be used).");
      } else {
        processConstraint(propConstraint, vc);
        addPropertyConstraintsToList(propConstraint, vc);
      }
    }

    return vc;
  }

  protected void processConstraint(Map<String, Object> theConstraint, ValidatorConfig vc)
      throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {

    String focusLabel = translateUri((String) theConstraint.get("appliesToCat"), tx, gc);
    String propOrRel =
        theConstraint.containsKey("item") ? translateUri((String) theConstraint.get("item"), tx, gc) : null;
    String severity = theConstraint.containsKey("severity") ? (String) theConstraint.get("severity")
        : SHACL.VIOLATION.stringValue();

    if (theConstraint.get("dataType") != null) {
      //TODO: this will be safer via APOC? maybe exclude some of them? and log the ignored ones?
      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getDataTypeViolationQuery(false), getDataTypeViolationQuery(true), focusLabel,
          propOrRel,
          getDatatypeCastExpressionPref((String) theConstraint.get("dataType")),
          getDatatypeCastExpressionSuff((String) theConstraint.get("dataType")),
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel,
          severity, (String) theConstraint.get("dataType"));

      //TODO: This part is to check that a property for which a datatype constraint has been defined
      // is not being used as a relationship
      addCypherToValidationScripts(vc, Arrays.asList(focusLabel), getDataTypeViolationQuery2(false),
          getDataTypeViolationQuery2(true), focusLabel,
          propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel,
          severity, propOrRel);
    }

    //this type of constraint only makes sense RDF graphs.
    if (shallIUseUriInsteadOfId() && theConstraint.get("hasValueUri") != null) {
      List<String> valueUriList = (List<String>) theConstraint.get("hasValueUri");
      if (!valueUriList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theHasValueUri", valueUriList);

        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getHasValueUriViolationQuery(false), getHasValueUriViolationQuery(true), paramSetId,
            focusLabel,
            propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
            propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("hasValueLiteral") != null) {
      List<String> valueLiteralList = (List<String>) theConstraint.get("hasValueLiteral");
      if (!valueLiteralList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.HAS_VALUE.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theHasValueLiteral", valueLiteralList);

        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getHasValueLiteralViolationQuery(false), getHasValueLiteralViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
            propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("rangeKind") != null) {
      if (theConstraint.get("rangeKind").equals(SHACL.LITERAL.stringValue())) {
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getRangeIRIKindViolationQuery(false), getRangeIRIKindViolationQuery(true), focusLabel,
            propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel);
      } else if (theConstraint.get("rangeKind").equals(SHACL.BLANK_NODE_OR_IRI.stringValue())) {
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getRangeLiteralKindViolationQuery(false), getRangeLiteralKindViolationQuery(true),
            focusLabel,
            propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("rangeType") != null && !theConstraint.get("rangeType")
        .equals("")) {
      addCypherToValidationScripts(vc, new ArrayList<String>(
              Arrays.asList(focusLabel, translateUri((String) theConstraint.get("rangeType"), tx, gc))),
          getRangeType1ViolationQuery(false), getRangeType1ViolationQuery(true), focusLabel,
          propOrRel,
          translateUri((String) theConstraint.get("rangeType"), tx, gc),
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity,
          translateUri((String) theConstraint.get("rangeType"), tx, gc));
      addCypherToValidationScripts(vc, new ArrayList<String>(
              Arrays.asList(focusLabel, translateUri((String) theConstraint.get("rangeType"), tx, gc))),
          getRangeType2ViolationQuery(false), getRangeType2ViolationQuery(true), focusLabel,
          propOrRel,
          focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, severity,
          propOrRel);
    }

    if (theConstraint.get("inLiterals") != null) {
      List<String> valueLiteralList = (List<String>) theConstraint.get("inLiterals");
      if (!valueLiteralList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theInLiterals", valueLiteralList);

        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getInLiteralsViolationQuery(false), getInLiteralsViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel, focusLabel, (String) theConstraint.get("propShapeUid"),
            propOrRel, severity, propOrRel);
      }
    }

    if (theConstraint.get("inUris") != null) {
      List<String> valueLiteralList = (List<String>) theConstraint.get("inUris");
      if (!valueLiteralList.isEmpty()) {
        String paramSetId =
            theConstraint.get("propShapeUid") + "_" + SHACL.IN.stringValue();
        Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
        params.put("theInUris", valueLiteralList);

        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getInUrisViolationQuery(false), getInUrisViolationQuery(true),
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
      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getRegexViolationQuery(false), getRegexViolationQuery(true), paramSetId,
          focusLabel, propOrRel, propOrRel, focusLabel,
          (String) theConstraint.get("propShapeUid"), propOrRel, severity);

    }

    if (theConstraint.get("minCount") != null) {
      String paramSetId =
          theConstraint.get("propShapeUid") + "_" + SHACL.MIN_COUNT.stringValue();
      Map<String, Object> params = createNewSetOfParams(vc.getAllParams(), paramSetId);
      params.put("minCount", theConstraint.get("minCount"));

      if (!(boolean) theConstraint.get("inverse")) {

        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getMinCardinality1ViolationQuery(false), getMinCardinality1ViolationQuery(true),
            paramSetId, focusLabel,
            " params.minCount <= ",
            propOrRel, propOrRel,
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel, propOrRel,
            severity);
      } else {
        // multivalued attributes not checked for cardinality in the case of inverse??
        // does not make sense in an LPG
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getMinCardinality1InverseViolationQuery(false),
            getMinCardinality1InverseViolationQuery(true),
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

        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getMaxCardinality1ViolationQuery(false), getMaxCardinality1ViolationQuery(true),
            paramSetId, focusLabel,
            propOrRel, propOrRel,
            " <= params.maxCount ",
            focusLabel, (String) theConstraint.get("propShapeUid"), propOrRel, propOrRel, propOrRel,
            severity);
      } else {
        // multivalued attributes not checked for cardinality in the case of inverse??
        // does not make sense in an LPG
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
            getMaxCardinality1InverseViolationQuery(false),
            getMaxCardinality1InverseViolationQuery(true),
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

      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getStrLenViolationQuery(false), getStrLenViolationQuery(true), paramSetId,
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

      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getValueRangeViolationQuery(false), getValueRangeViolationQuery(true), paramSetId,
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
      for (String uri : (List<String>) theConstraint.get("ignoredProps")) {
        if (!uri.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
          allowedPropsTranslated.add(translateUri(uri, tx, gc));
        }
      }
      if (theConstraint.get("definedProps") != null) {
        for (String uri : (List<String>) theConstraint.get("definedProps")) {
          if (!uri.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
            allowedPropsTranslated.add(translateUri(uri, tx, gc));
          }
        }
      }
      params.put("allAllowedProps", allowedPropsTranslated);

      addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel)),
          getNodeStructureViolationQuery(false), getNodeStructureViolationQuery(true), paramSetId,
          focusLabel,
          focusLabel,
          (String) theConstraint.get("nodeShapeUid"), "http://www.w3.org/ns/shacl#Violation");


    }

    if (theConstraint.get("disjointClass") != null) {

      for (String uri : (List<String>) theConstraint.get("disjointClass")) {
        //disjointClasses.add(translateUri(uri));
        addCypherToValidationScripts(vc, new ArrayList<String>(Arrays.asList(focusLabel, translateUri(uri, tx, gc))),
            getDisjointClassesViolationQuery(false), getDisjointClassesViolationQuery(true),
            focusLabel, translateUri(uri, tx, gc),focusLabel,
            (String) theConstraint.get("nodeShapeUid"), translateUri(uri, tx, gc),
            "http://www.w3.org/ns/shacl#Violation", translateUri(uri, tx, gc));
      }

    }

  }

  void addPropertyConstraintsToList(Map<String, Object> propConstraint,
      ValidatorConfig vc)
      throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {

    String focusLabel = translateUri((String) propConstraint.get("appliesToCat"), tx, gc);
    String propOrRel =
        propConstraint.containsKey("item") ? translateUri((String) propConstraint.get("item"), tx, gc)
            : null;
    //TODO: add severity and inverse
    //String severity = (String) propConstraint.get("severity");
    //INVERSE?

    if (propConstraint.get("dataType") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.DATATYPE.getLocalName()
              : SHACL.DATATYPE.getLocalName(),
          shallIUseUriInsteadOfId() ? propConstraint.get("dataType")
              : ((String) propConstraint.get("dataType"))
                  .substring(URIUtil.getLocalNameIndex((String) propConstraint.get("dataType")))));
    }

    if (propConstraint.get("hasValueUri") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.HAS_VALUE.getLocalName()
              : SHACL.HAS_VALUE.getLocalName(),
          (List<String>) propConstraint
              .get("hasValueUri"))); //TODO: there  should be a translate here??
    }

    if (propConstraint.get("hasValueLiteral") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.HAS_VALUE.getLocalName()
              : SHACL.HAS_VALUE.getLocalName(),
          (List<String>) propConstraint.get("hasValueLiteral")));
    }

    if (propConstraint.get("rangeKind") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.NODE_KIND.getLocalName()
              : SHACL.NODE_KIND.getLocalName(),
          shallIUseUriInsteadOfId() ? propConstraint.get("rangeKind") :
              ((String) propConstraint.get("rangeKind"))
                  .substring(URIUtil.getLocalNameIndex((String) propConstraint.get("rangeKind")))));
    }

    if (propConstraint.get("rangeType") != null && !propConstraint.get("rangeType")
        .equals("")) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.CLASS.getLocalName()
              : SHACL.CLASS.getLocalName(),
          translateUri((String) propConstraint.get("rangeType"), tx, gc)));
    }

    if (propConstraint.get("inLiterals") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.IN.getLocalName() : SHACL.IN.getLocalName(),
          (List<String>) propConstraint.get("inLiterals")));
    }

    if (propConstraint.get("inUris") != null) {
      List<String> inUrisRaw = (List<String>) propConstraint.get("inUris");
      List<String> inUrisLocal = new ArrayList<>();
      inUrisRaw.forEach(x -> inUrisLocal.add(x.substring(URIUtil.getLocalNameIndex(x))));
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.IN.getLocalName() : SHACL.IN.getLocalName(),
          shallIUseUriInsteadOfId() ? inUrisRaw : inUrisLocal));
    }

    if (propConstraint.get("pattern") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.PATTERN.getLocalName()
              : SHACL.PATTERN.getLocalName(),
          propConstraint.get("pattern")));
    }

    if (propConstraint.get("minCount") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MIN_COUNT.getLocalName()
              : SHACL.MIN_COUNT.getLocalName(),
          propConstraint.get("minCount")));
    }

    if (propConstraint.get("maxCount") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MAX_COUNT.getLocalName()
              : SHACL.MAX_COUNT.getLocalName(),
          propConstraint.get("maxCount")));
    }

    if (propConstraint.get("minStrLen") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MIN_LENGTH.getLocalName()
              : SHACL.MIN_LENGTH.getLocalName(),
          propConstraint.get("minStrLen")));
    }

    if (propConstraint.get("maxStrLen") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MAX_LENGTH.getLocalName()
              : SHACL.MAX_LENGTH.getLocalName(),
          propConstraint.get("maxStrLen")));
    }

    if (propConstraint.get("minInc") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MIN_INCLUSIVE.getLocalName()
              : SHACL.MIN_INCLUSIVE.getLocalName(),
          propConstraint.get("minInc")));
    }

    if (propConstraint.get("maxInc") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MAX_INCLUSIVE.getLocalName()
              : SHACL.MAX_INCLUSIVE.getLocalName(),
          propConstraint.get("maxInc")));
    }

    if (propConstraint.get("minExc") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MIN_EXCLUSIVE.getLocalName()
              : SHACL.MIN_EXCLUSIVE.getLocalName(),
          propConstraint.get("minExc")));
    }

    if (propConstraint.get("maxExc") != null) {
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.MAX_EXCLUSIVE.getLocalName()
              : SHACL.MAX_EXCLUSIVE.getLocalName(),
          propConstraint.get("maxExc")));
    }

    if (propConstraint.get("ignoredProps") != null) {
      List<String> ignoredUrisRaw = (List<String>) propConstraint.get("ignoredProps");
      List<String> ignoredUrisTranslated = new ArrayList<>();
      for (String x : ignoredUrisRaw) {
        if (!x.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
          ignoredUrisTranslated.add(translateUri(x, tx, gc));
        }
      }
      vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
          shallIUseUriInsteadOfId() ? "sh:" + SHACL.IGNORED_PROPERTIES.getLocalName()
              : SHACL.IGNORED_PROPERTIES.getLocalName(),
          ignoredUrisTranslated));
    }

    if (propConstraint.get("disjointClass") != null) {
      List<String> disjointClassesRaw = (List<String>) propConstraint.get("disjointClass");
      for (String x : disjointClassesRaw) {
        vc.addConstraintToList(new ConstraintComponent(focusLabel, propOrRel,
            shallIUseUriInsteadOfId() ? "sh:" + SHACL.NOT.getLocalName()
                : SHACL.NOT.getLocalName(),
            translateUri(x, tx, gc)));
      }
    }

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
          record.put("appliesToCat",
              next.hasBinding("targetClass") ? next.getValue("targetClass").stringValue() : null);
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

          constraints.add(record);
        }

      }

      //allowed and not-allowed properties in closed node shapes
      tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, NODE_CONSTRAINT_QUERY);
      queryResult = tupleQuery.evaluate();
      while (queryResult.hasNext()) {
        Map<String, Object> record = new HashMap<>();
        BindingSet next = queryResult.next();
        record.put("appliesToCat", next.getValue("targetClass").stringValue());
        record
            .put("nodeShapeUid", next.hasBinding("ns") ? next.getValue("ns").stringValue() : null);
        if (next.hasBinding("definedProps")) {
          record.put("definedProps",
              Arrays.asList(next.getValue("definedProps").stringValue().split("---")));
        }
        if (next.hasBinding("ignoredProps")) {
          record.put("ignoredProps",
              Arrays.asList(next.getValue("ignoredProps").stringValue().split("---")));
        }
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
        if (next.hasBinding("class")) {
          record.put("reqClass",
              Arrays.asList(next.getValue("class").stringValue().split("---")));
        }
        if (next.hasBinding("disjointclass")) {
          record.put("disjointClass",
              Arrays.asList(next.getValue("disjointclass").stringValue().split("---")));
        }
        constraints.add(record);
      }

    }
    return constraints.iterator();
  }

  private String getDatatypeCastExpressionPref(String dataType) {
    if (dataType.equals(XMLSchema.BOOLEAN.stringValue())) {
      return "coalesce(toBoolean(toString(";
    } else if (dataType.equals(XMLSchema.STRING.stringValue())) {
      return "coalesce(toString(";
    } else if (dataType.equals(XMLSchema.INTEGER.stringValue())) {
      return "coalesce(toInteger(";
    } else if (dataType.equals(XMLSchema.FLOAT.stringValue())) {
      return "coalesce(toFloat(";
    } else if (dataType.equals(XMLSchema.DATE.stringValue())) {
      return "n10s.aux.dt.check('" + XMLSchema.DATE.stringValue()+ "',";
    } else if (dataType.equals(XMLSchema.DATETIME.stringValue())) {
      return "n10s.aux.dt.check('" + XMLSchema.DATETIME.stringValue()+ "',";
    } else if (dataType.equals(WKTLITERAL_URI.stringValue())) {
      return "n10s.aux.dt.check('" +WKTLITERAL_URI.stringValue()+ "',";
    } else {
      return "";
    }
  }

  private String getDatatypeCastExpressionSuff(String dataType) {
    if (dataType.equals(XMLSchema.BOOLEAN.stringValue())) {
      return ")) = x , false)";
    } else if (dataType.equals(XMLSchema.STRING.stringValue())) {
      return ") = x , false)";
    } else if (dataType.equals(XMLSchema.INTEGER.stringValue())) {
      return ") = x , false)";
    } else if (dataType.equals(XMLSchema.FLOAT.stringValue())) {
      return ") = x , false)";
    } else if (dataType.equals(XMLSchema.DATE.stringValue())) {
      return ")";
    } else if (dataType.equals(XMLSchema.DATETIME.stringValue())) {
      return ")";
    } else if (dataType.equals(WKTLITERAL_URI.stringValue())) {
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

  private String getDataTypeViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_DATATYPE_V_SUFF());
  }

  private String getDataTypeViolationQuery2(boolean tx) {
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

  private String getMinCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MIN_CARDINALITY1_V_SUFF());
  }

  private String getMinCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF());
  }

  private String getMaxCardinality1ViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY1_V_SUFF());
  }

  private String getMaxCardinality1InverseViolationQuery(boolean tx) {
    return getQuery(CYPHER_WITH_PARAMS_MATCH_WHERE, tx, CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF());
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

  private String getDisjointClassesViolationQuery(boolean tx) {
    return getQuery(CYPHER_MATCH_WHERE, tx, CYPHER_NODE_DISJOINT_WITH_V_SUFF());
  }


  private boolean shallIUseUriInsteadOfId() {
    return gc != null && (gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_KEEP);
  }

  private boolean shallIShorten() {
    return gc != null && (gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP);
  }

  private String getQuery(String pref, boolean tx, String suff) {
    return pref + (tx ? CYPHER_TX_INFIX : "") + suff;
  }

  private String CYPHER_DATATYPE_V_SUFF() {
    return " NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN " +
        (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
        + "' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity,"
        + " 'property value should be of type ' + " +
        (shallIUseUriInsteadOfId() ? " '%s' " : "n10s.rdf.getIRILocalName('%s')")
        + " as message ";
  }

  private String CYPHER_DATATYPE2_V_SUFF() {
    return " true RETURN " + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ")
        + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.DATATYPE_CONSTRAINT_COMPONENT
        + "' as propertyShape, " + (shallIUseUriInsteadOfId() ? " x.uri " : " 'node id: ' + id(x) ")
        + "as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " + ' should be a property, instead it  is a relationship' as message ";
  }

  private String CYPHER_IRI_KIND_V_SUFF() {
    return " (focus)-[:`%s`]->() RETURN " + (shallIUseUriInsteadOfId() ? " focus.uri "
        : " id(focus) ") + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity,"
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " + ' should be a property ' as message  ";
  }

  private String CYPHER_LITERAL_KIND_V_SUFF() {
    return " exists(focus.`%s`) RETURN " + (shallIUseUriInsteadOfId() ? " focus.uri "
        : " id(focus) ") + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.NODE_KIND_CONSTRAINT_COMPONENT
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity,"
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " + ' should be a relationship ' as message  ";
  }

  private String CYPHER_RANGETYPE1_V_SUFF() {
    return "NOT x:`%s` RETURN " + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ")
        + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT
        + "' as propertyShape, " + (shallIUseUriInsteadOfId() ? " x.uri " : " id(x) ")
        + " as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity,"
        + " 'value should be of type ' + " + (shallIShorten()
        ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") + " as message  ";
  }

  private String CYPHER_RANGETYPE2_V_SUFF() {
    return "exists(focus.`%s`) RETURN " + (shallIUseUriInsteadOfId() ? " focus.uri "
        : " id(focus) ") + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.CLASS_CONSTRAINT_COMPONENT
        + "' as propertyShape, null as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "'%s should be a relationship but it is a property' as message  ";
  }

  private String CYPHER_REGEX_V_SUFF() {
    return "NOT all(x in [] +  coalesce(focus.`%s`,[]) where toString(x) =~ params.theRegex )  "
        + " UNWIND [x in [] +  coalesce(focus.`%s`,[]) where not toString(x) =~ params.theRegex ]  as offval "
        + "RETURN "
        + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") + " as nodeId, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.PATTERN_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, offval as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "'' as message  ";
  }


  private String CYPHER_HAS_VALUE_URI_V_SUFF() {
    return
        " true with params, focus unwind params.theHasValueUri as reqVal with focus, reqVal where not reqVal in [(focus)-[:`%s`]->(v) | v.uri ] "
            + "RETURN "
            + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") + " as nodeId, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT
            .stringValue()
            + "' as propertyShape, null as offendingValue, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + "'The required value ' + reqVal  + ' could not be found as value of relationship ' + "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ")
            + " as message  ";
  }

  private String CYPHER_HAS_VALUE_LITERAL_V_SUFF() {
    return
        " true with params, focus unwind params.theHasValueLiteral as  reqVal with focus, reqVal where not reqVal in [] + focus.`%s` "
            + "RETURN "
            + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") + " as nodeId, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.HAS_VALUE_CONSTRAINT_COMPONENT
            .stringValue()
            + "' as propertyShape, null as offendingValue, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + "'The required value \"'+ reqVal + '\" was not found in property ' + " + (
            shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s' ") + " as message  ";
  }

  private String CYPHER_IN_LITERAL_V_SUFF() {
    return
        " true with params, focus unwind [] + focus.`%s` as val with focus, val where not val in params.theInLiterals "
            + "RETURN "
            + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") + " as nodeId, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
            .stringValue()
            + "' as propertyShape, val as offendingValue, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + "'The value \"'+ val + '\" in property ' + " + (shallIShorten()
            ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
            + "+ 'is not in  the accepted list' as message  ";
  }

  private String CYPHER_IN_URI_V_SUFF() {
    return
        " true with params, focus unwind [(focus)-[:`%s`]->(x) | x ] as val with focus, val where not val.uri in params.theInUris "
            + "RETURN "
            + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") + " as nodeId, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as nodeType, '%s' as shapeId, '" + SHACL.IN_CONSTRAINT_COMPONENT
            .stringValue()
            + "' as propertyShape, " + (shallIUseUriInsteadOfId() ? "val.uri" : "id(val)")
            + " as offendingValue, "
            + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
            + " as propertyName, '%s' as severity, "
            + "'The value \"'+ " + (shallIUseUriInsteadOfId() ? " val.uri "
            : " 'node id: '  + id(val) ") + " + '\" in property ' + " + (shallIShorten()
            ? "n10s.rdf.fullUriFromShortForm('%s') " : " '%s'")
            + "+ ' is not in  the accepted list' as message  ";
  }

  private String CYPHER_VALRANGE_V_SUFF() {
    return "NOT all(x in [] +  focus.`%s` where %s x %s ) RETURN " + (shallIUseUriInsteadOfId()
        ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_EXCLUSIVE_CONSTRAINT_COMPONENT
        .stringValue()
        + "' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "'' as message  ";
  }

  private String CYPHER_MIN_CARDINALITY1_V_SUFF() {
    return "NOT %s ( size((focus)-[:`%s`]->()) +  size([] + coalesce(focus.`%s`, [])) )  RETURN "
        + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable cardinality: ' + (coalesce(size((focus)-[:`%s`]->()),0) + coalesce(size([] + focus.`%s`),0))  as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MAX_CARDINALITY1_V_SUFF() {
    return "NOT (size((focus)-[:`%s`]->()) + size([] + coalesce(focus.`%s`,[]))) %s  RETURN " + (
        shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable  cardinality: ' + (coalesce(size((focus)-[:`%s`]->()),0) + coalesce(size([] + focus.`%s`),0)) as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MIN_CARDINALITY1_INVERSE_V_SUFF() {   //This will need fixing, the coalesce in first line + the changes to cardinality
    return "NOT %s size((focus)<-[:`%s`]-()) RETURN " + (shallIUseUriInsteadOfId() ? " focus.uri "
        : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MIN_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unnacceptable cardinality: ' + coalesce(size((focus)<-[:`%s`]-()),0) as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "null as offendingValue  ";
  }

  private String CYPHER_MAX_CARDINALITY1_INVERSE_V_SUFF() {   //Same as previous
    return "NOT size((focus)<-[:`%s`]-()) %s RETURN " + (shallIUseUriInsteadOfId() ? " focus.uri "
        : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '" + SHACL.MAX_COUNT_CONSTRAINT_COMPONENT
        + "' as propertyShape,  'unacceptable cardinality: ' + coalesce(size((focus)<-[:`%s`]-()),0) as message, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
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
    return "NOT all(x in [] +  focus.`%s` where %s size(toString(x)) %s ) RETURN " + (
        shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.MAX_LENGTH_CONSTRAINT_COMPONENT
        + "' as propertyShape, focus.`%s` as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as propertyName, '%s' as severity, "
        + "'' as message  ";
  }

  private String CYPHER_NODE_STRUCTURE_V_SUFF() {
    return " true \n" +
        "UNWIND [ x in [(focus)-[r]->()| type(r)] where not x in params.allAllowedProps] + [ x in keys(focus) where "
        +
        (shallIUseUriInsteadOfId() ? " x <> 'uri' and " : "")
        + " not x in params.allAllowedProps] as noProp\n"
        + "RETURN  " + (shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") +
        " as nodeId , " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ")
        + " as nodeType, '%s' as shapeId, '"
        + SHACL.CLOSED_CONSTRAINT_COMPONENT.stringValue()
        + "' as propertyShape, substring(reduce(result='', x in [] + coalesce(focus[noProp],[(focus)-[r]-(x) where type(r)=noProp | "
        + (shallIUseUriInsteadOfId() ? " x.uri " : " id(x) ")
        + "]) | result + ', ' + x ),2) as offendingValue, "
        + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm(noProp)" : " noProp ") +
        " as propertyName, '%s' as severity, "
        + "'Closed type definition does not include this property/relationship' as message  ";
  }

  private String CYPHER_NODE_DISJOINT_WITH_V_SUFF() {
    return " focus:`%s` RETURN " + (
        shallIUseUriInsteadOfId() ? " focus.uri " : " id(focus) ") +
        " as nodeId, " + (shallIShorten() ? "n10s.rdf.fullUriFromShortForm('%s')" : " '%s' ") +
        " as nodeType, '%s' as shapeId, '" + SHACL.NOT_CONSTRAINT_COMPONENT
        + "' as propertyShape, '%s' as offendingValue, "
        + " '-' as propertyName, '%s' as severity, "
        + " 'type not allowed: ' + '%s' as message  ";
  }

}
