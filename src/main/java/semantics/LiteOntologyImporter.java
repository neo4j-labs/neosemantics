package semantics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * Created by jbarrasa on 21/03/2016. <p> Importer of basic ontology (RDFS & OWL) elements: TODO:
 * This whole class needs to be redone. It can be made a lot simpler and clearer!!
 */
public class LiteOntologyImporter {

  private static final String DEFAULT_CLASS_LABEL_NAME = "Class";
  private static final String DEFAULT_SCO_REL_NAME = "SCO";
  private static final String DEFAULT_DATATYPEPROP_LABEL_NAME = "Property";
  private static final String DEFAULT_OBJECTPROP_LABEL_NAME = "Relationship";
  private static final String DEFAULT_SPO_REL_NAME = "SPO";
  private static final String DEFAULT_DOMAIN_REL_NAME = "DOMAIN";
  private static final String DEFAULT_RANGE_REL_NAME = "RANGE";
  private static final boolean DEFAULT_ADD_RESOURCE_LABELS = false;
  @Context
  public GraphDatabaseService db;
  public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD,
      RDFFormat.TURTLE,
      RDFFormat.NTRIPLES, RDFFormat.TRIG};


  @Procedure(mode = Mode.WRITE)
  public Stream<ImportResults> liteOntoImport(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    ImportResults importResults = new ImportResults();
    URL documentUrl;
    int classesLoaded = 0;
    int datatypePropsLoaded = 0;
    int objPropsLoaded = 0;
    int propsLoaded = 0;
    boolean addResourceLabel =
        props.containsKey("addResourceLabels") ? (boolean) props.get("addResourceLabels")
            : DEFAULT_ADD_RESOURCE_LABELS;
    String classLabelName = props.containsKey("classLabel") ? (String) props.get("classLabel")
        : DEFAULT_CLASS_LABEL_NAME;
    String subClassOfRelName =
        props.containsKey("subClassOfRel") ? (String) props.get("subClassOfRel")
            : DEFAULT_SCO_REL_NAME;
    String dataTypePropertyLabelName =
        props.containsKey("dataTypePropertyLabel") ? (String) props.get("dataTypePropertyLabel")
            : DEFAULT_DATATYPEPROP_LABEL_NAME;
    String objectPropertyLabelName =
        props.containsKey("objectPropertyLabel") ? (String) props.get("objectPropertyLabel")
            : DEFAULT_OBJECTPROP_LABEL_NAME;
    String subPropertyOfRelName =
        props.containsKey("subPropertyOfRel") ? (String) props.get("subPropertyOfRel")
            : DEFAULT_SPO_REL_NAME;
    String domainRelName =
        props.containsKey("domainRel") ? (String) props.get("domainRel") : DEFAULT_DOMAIN_REL_NAME;
    String rangeRelName =
        props.containsKey("rangeRel") ? (String) props.get("rangeRel") : DEFAULT_RANGE_REL_NAME;
    try {
      URLConnection urlConn = new URL(url).openConnection();
      if (props.containsKey("headerParams")) {
        ((Map<String, String>) props.get("headerParams"))
            .forEach((k, v) -> urlConn.setRequestProperty(k, v));
      }
      InputStream inputStream = getInputStream(url, props);
      RDFParser rdfParser = Rio.createParser(getFormat(format));
      Model model = new LinkedHashModel();
      rdfParser.setRDFHandler(new StatementCollector(model));
      rdfParser.parse(inputStream, url);
      classesLoaded = extractClasses(model, classLabelName, subClassOfRelName, addResourceLabel);
      objPropsLoaded = extractProps(model, OWL.OBJECTPROPERTY, objectPropertyLabelName,
          subPropertyOfRelName, domainRelName, rangeRelName, addResourceLabel);
      datatypePropsLoaded = extractProps(model, OWL.DATATYPEPROPERTY, dataTypePropertyLabelName,
          subPropertyOfRelName, domainRelName, rangeRelName, addResourceLabel);
      //an rdf:property can be either datatype or objecttype? I'll treat it as an object property
      propsLoaded = extractProps(model, RDF.PROPERTY, objectPropertyLabelName, subPropertyOfRelName,
          domainRelName, rangeRelName, addResourceLabel);

    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
      importResults.setTerminationKO(e.getMessage());
    } finally {
      importResults
          .setElementsLoaded(classesLoaded + datatypePropsLoaded + objPropsLoaded + propsLoaded);
    }
    return Stream.of(importResults);
  }

  // this needs to be refactored. It's also in RDFImport!
  private InputStream getInputStream(@Name("url") String url,
      @Name("props") Map<String, Object> props) throws IOException {
    URLConnection urlConn;
    //This should be delegated to APOC to do handle different protocols, deal with redirection, etc.
    urlConn = new URL(url).openConnection();
    if (props.containsKey("headerParams")) {
      Map<String, String> headerParams = (Map<String, String>) props.get("headerParams");
      Object method = headerParams.get("method");
      if (method != null && urlConn instanceof HttpURLConnection) {
        HttpURLConnection http = (HttpURLConnection) urlConn;
        http.setRequestMethod(method.toString());
      }
      headerParams.forEach((k, v) -> urlConn.setRequestProperty(k, v));
      if (props.containsKey("payload")) {
        urlConn.setDoOutput(true);
        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(urlConn.getOutputStream(), "UTF-8"));
        writer.write(props.get("payload").toString());
        writer.close();
      }
    }
    return urlConn.getInputStream();
  }

  private int extractProps(Model model, IRI propType, String propertyLabelName,
      String subPropertyOfRelName,
      String domainRelName, String rangeRelName, boolean addResourceLabels) {
    int propsLoaded = 0;
    Set<Resource> allProps = model.filter(null, RDF.TYPE, propType).subjects();
    List<Map<String, Object>> allPropParams = new ArrayList<>();
    List<Map<String, Object>> allDomainParams = new ArrayList<>();
    List<Map<String, Object>> allRangeParams = new ArrayList<>();
    for (Resource propResource : allProps) {
      if (!(propResource instanceof BNode)) {
        Map<String, Object> params = new HashMap<>();
        params.put("uri", propResource.stringValue());
        Map<String, Object> props = new HashMap<>();
        Set<Value> propNames = model.filter(propResource, RDFS.LABEL, null).objects();
        props.put("name", propNames.isEmpty() ? ((IRI) propResource).getLocalName() :
            propNames.iterator().next().stringValue());

        Set<Value> propComments = model.filter(propResource, RDFS.COMMENT, null).objects();
        if (!propComments.isEmpty()) {
          props.put("comment", propComments.iterator().next().stringValue());
        }
        params.put("props", props);
        allPropParams.add(params);
        propsLoaded++;
        allDomainParams
            .addAll(extractDomain(model, propResource, propType, propertyLabelName, domainRelName));
        allRangeParams
            .addAll(extractRange(model, propResource, propType, propertyLabelName, rangeRelName));
      }
    }

    Map<String, Object> paramsForPropQuery = new HashMap<>();
    paramsForPropQuery.put("paramList", allPropParams);
    db.execute(String
        .format("UNWIND $paramList AS param MERGE (p:%s`%s` { uri: param.uri}) SET p+=param.props",
            (addResourceLabels ? "Resource:" : ""), propertyLabelName), paramsForPropQuery);

    Map<String, Object> paramsForDomainQuery = new HashMap<>();
    paramsForDomainQuery.put("paramList", allDomainParams);
    db.execute(String.format(
        "UNWIND $paramList AS param MATCH (p:%s`%s` { uri: param.propUri}), (c { uri: param.domainUri}) MERGE (p)-[:`%s`]->(c)",
        // c can be a class or an object property
        (addResourceLabels ? "Resource:" : ""), propertyLabelName, domainRelName),
        paramsForDomainQuery);

    Map<String, Object> paramsForRangeQuery = new HashMap<>();
    paramsForRangeQuery.put("paramList", allRangeParams);
    db.execute(String.format(
        "UNWIND $paramList AS param MATCH (p:%s`%s` { uri: param.propUri}), (c { uri: param.rangeUri}) MERGE (p)-[:`%s`]->(c)",
        (addResourceLabels ? "Resource:" : ""), propertyLabelName, rangeRelName),
        paramsForRangeQuery);

    for (Resource propResource : allProps) {
      if (!(propResource instanceof BNode)) {
        extractPropertyHierarchy(model, propResource, propType, propertyLabelName,
            subPropertyOfRelName);
      }
    }
    return propsLoaded;
  }


  private List<Map<String, Object>> extractDomain(Model model, Resource propResource, IRI propType,
      String propertyLabelName,
      String domainRelName) {
    List<Map<String, Object>> domainParamsList = new ArrayList<>();
    for (Value object : model.filter(propResource, RDFS.DOMAIN, null).objects()) {
      if (object instanceof IRI && (model.contains((IRI) object, RDF.TYPE, OWL.CLASS) ||
          model.contains((IRI) object, RDF.TYPE, RDFS.CLASS) ||
          model.contains((IRI) object, RDF.TYPE, OWL.OBJECTPROPERTY))) {
        //This last bit picks up OWL definitions of attributes on properties.
        // Totally non standard, but in case it's used to accommodate LPG style properties on rels definitions
        Map<String, Object> params = new HashMap<>();
        params.put("propUri", propResource.stringValue());
        params.put("domainUri", object.stringValue());
        domainParamsList.add(params);
      }
    }
    return domainParamsList;
  }

  private List<Map<String, Object>> extractRange(Model model, Resource propResource, IRI propType,
      String propertyLabelName,
      String rangeRelName) {
    List<Map<String, Object>> rangeParamsList = new ArrayList<>();
    for (Value object : model.filter(propResource, RDFS.RANGE, null).objects()) {
      if (object instanceof IRI && (model.contains((IRI) object, RDF.TYPE, OWL.CLASS) ||
          model.contains((IRI) object, RDF.TYPE, RDFS.CLASS))) {
        //only picks ranges that are classes, which means, only ObjectProperties
        // (no XSD ranges for DatatypeProps)
        Map<String, Object> params = new HashMap<>();
        params.put("propUri", propResource.stringValue());
        params.put("rangeUri", object.stringValue());
        rangeParamsList.add(params);
      }
    }
    return rangeParamsList;
  }

  private void extractPropertyHierarchy(Model model, Resource propResource, IRI propType,
      String propertyLabelName, String subPropertyOfRelName) {
    for (Value object : model.filter(propResource, RDFS.SUBPROPERTYOF, null).objects()) {
      if (object instanceof IRI && (model.contains((IRI) object, RDF.TYPE, OWL.OBJECTPROPERTY) ||
          model.contains((IRI) object, RDF.TYPE, OWL.DATATYPEPROPERTY) ||
          model.contains((IRI) object, RDF.TYPE, RDF.PROPERTY))) {
        Map<String, Object> params = new HashMap<>();
        params.put("propUri", propResource.stringValue());
        params.put("parentPropUri", object.stringValue());
        db.execute(String.format(
            "MATCH (p:`%s` { uri:$propUri}), (c { uri:$parentPropUri}) MERGE (p)-[:`%s`]->(c)",
            propertyLabelName, subPropertyOfRelName), params);
      }
    }
  }

  private int extractClasses(Model model, String classLabelName, String scoRelName,
      boolean addResourceLabels) {
    // loads Simple Named Classes (https://www.w3.org/TR/2004/REC-owl-guide-20040210/#SimpleClasses)
    int classesLoaded = 0;
    Set<Resource> allClasses = model.filter(null, RDF.TYPE, OWL.CLASS).subjects();
    allClasses.addAll(model.filter(null, RDF.TYPE, RDFS.CLASS).subjects());
    Model scoStatements = model.filter(null, RDFS.SUBCLASSOF, null);
    allClasses.addAll(scoStatements.subjects());
    scoStatements.objects().stream().filter(x -> x instanceof IRI)
        .forEach(x -> allClasses.add((IRI) x));
    List<Map<String, Object>> paramList = new ArrayList<>();
    for (Resource classResource : allClasses) {
      if (!(classResource instanceof BNode)) {
        Map<String, Object> props = new HashMap<>();
        Set<Value> classNames = model.filter(classResource, RDFS.LABEL, null).objects();
        props.put("name", classNames.isEmpty() ? ((IRI) classResource).getLocalName() :
            classNames.iterator().next().stringValue());

        for (Value classComment : model.filter(classResource, RDFS.COMMENT, null).objects()) {
          props.put("comment", classComment.stringValue().replace("'", "\'"));
          break;
        }
        Map<String, Object> params = new HashMap<>();
        params.put("props", props);
        params.put("uri", classResource.stringValue());
        paramList.add(params);
        classesLoaded++;
      }
    }

    Map<String, Object> classParams = new HashMap<>();
    classParams.put("paramList", paramList);
    db.execute(String
        .format(
            "UNWIND $paramList AS params MERGE (p:%s`%s` { uri:params.uri}) SET p+=params.props",
            (addResourceLabels ? "Resource:" : ""), classLabelName), classParams);

    Set<Map<String, String>> scoPairs = new HashSet<>();
    for (Statement st : scoStatements) {
      Resource subject = st.getSubject();
      Value object = st.getObject();
      if ((subject instanceof IRI) && (object instanceof IRI)) {
        HashMap<String, String> scoPair = new HashMap<>();
        scoPair.put("child", subject.stringValue());
        scoPair.put("parent", object.stringValue());
        scoPairs.add(scoPair);
      }
    }

    Map<String, Object> scoParams = new HashMap<>();
    scoParams.put("scoPairs", new ArrayList(scoPairs));
    db.execute(String.format(
        "UNWIND $scoPairs AS pair MATCH (p:`%s` { uri: pair.parent }), (c:`%s` { uri: pair.child }) "
            +
            "MERGE (p)<-[:`%s`]-(c)", classLabelName, classLabelName, scoRelName
    ), scoParams);

    return classesLoaded;
  }

  private RDFFormat getFormat(String format) {
    if (format != null) {
      for (RDFFormat parser : availableParsers) {
        if (parser.getName().equals(format)) {
          return parser;
        }
      }
    }
    return RDFFormat.TURTLE; //some default
  }

  public static class ImportResults {

    public String terminationStatus = "OK";
    public long elementsLoaded = 0;
    public String extraInfo = "";

    public void setElementsLoaded(long elementsLoaded) {
      this.elementsLoaded = elementsLoaded;
    }

    public void setTerminationKO(String message) {
      this.terminationStatus = "KO";
      this.extraInfo = message;
    }

  }

}
