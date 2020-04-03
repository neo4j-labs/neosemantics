package semantics;

import static semantics.graphconfig.Params.DATATYPE_REGULAR_PATTERN;
import static semantics.graphconfig.Params.DATATYPE_SHORTENED_PATTERN;
import static semantics.graphconfig.Params.LANGUAGE_TAGGED_VALUE_PATTERN;
import static semantics.graphconfig.Params.PREFIX_SEPARATOR;
import static semantics.graphconfig.Params.SHORTENED_URI_PATTERN;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.jsonld.GenericJSONParser;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import semantics.graphconfig.GraphConfig;
import semantics.graphconfig.GraphConfig.GraphConfigNotFound;
import semantics.graphconfig.RDFParserConfig;
import semantics.result.GraphResult;
import semantics.result.NodeResult;
import semantics.result.StreamedStatement;
import semantics.utils.InvalidNamespacePrefixDefinitionInDB;
import semantics.utils.NsPrefixMap;

/**
 * Created by jbarrasa on 21/03/2016. <p> RDF importer based on: 1. Instancdes of DatatypeProperties
 * become node attributes 2. rdf:type relationships are transformed either into labels or
 * relationships to nodes representing the class 3. Instances of ObjectProperties become
 * relationships ( See https://jbarrasa.com/2016/06/07/importing-rdf-data-into-neo4j/ )
 */
public class RDFImport {

  static final int RELATIONSHIP = 0;
  static final int LABEL = 1;
  static final int PROPERTY = 2;
  static final int DATATYPE = 3;
  private static final String UNIQUENESS_CONSTRAINT_ON_URI = "n10s_unique_uri";

  public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD,
      RDFFormat.TURTLE, RDFFormat.NTRIPLES, RDFFormat.TRIG, RDFFormat.NQUADS};

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  @Procedure(mode = Mode.WRITE)
  @Description("Imports RDF from an url (file or http) and stores it in Neo4j as a property graph. "
      + "Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> importRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doImport(format, url, null, props));
  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports an RDF snippet passed as parameter and stores it in Neo4j as a property "
      + "graph. Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> importRDFSnippet(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doImport(format, null, rdfFragment, props));
  }

  private ImportResults doImport(@Name("format") String format, @Name("url") String url,
      @Name("rdf") String rdfFragment,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    DirectStatementLoader statementLoader = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    ImportResults importResults = new ImportResults();
    try {
      checkConstraintExist();
        conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      statementLoader = new DirectStatementLoader(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e){
      importResults.setTerminationKO(e.getMessage());
    } catch (GraphConfig.GraphConfigNotFound e) {
      importResults.setTerminationKO("A Graph Config is required for RDF importing procedures to run");
    } catch (RDFImportBadParams e) {
      importResults.setTerminationKO(e.getMessage());
    }

    if (statementLoader != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementLoader);
        importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
        importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
        importResults.setNamespaces(statementLoader.getNamespaces());
        importResults.setConfigSummary(props);

      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        importResults.setTerminationKO(e.getMessage());
        importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
        importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
        importResults.setConfigSummary(props);
      }
    }
    return importResults;
  }

  protected void parseRDFPayloadOrFromUrl(@Name("format") RDFFormat format, @Name("url") String url, @Name("rdf") String rdfFragment,
        @Name(value = "params", defaultValue = "{}") Map<String, Object> props, ConfiguredStatementHandler statementLoader) throws IOException {
    if (rdfFragment != null) {
      instantiateAndKickOffParser(new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())),
              "http://neo4j.com/base/", format, statementLoader);
    } else {
      instantiateAndKickOffParser(getInputStream(url, props), url, format, statementLoader);
    }
  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof and domain and range info.")
  public Stream<ImportResults> importOntology(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws GraphConfigNotFound {

    return Stream.of(doOntoImport(format, url, null, props));

  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof and domain and range info.")
  public Stream<ImportResults> importOntologySnippet(@Name("rdf") String rdf,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws GraphConfigNotFound {

    return Stream.of(doOntoImport(format, null, rdf, props));

  }

  private ImportResults doOntoImport(String format, String url,
      String rdfFragment, Map<String, Object> props) throws GraphConfigNotFound {

    // TODO: This effectively overrides the graphconfig (and can cause conflict?)
    props.put("handleVocabUris", "IGNORE");


    OntologyImporter ontoImporter = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    ImportResults importResults = new ImportResults();
    try {
      checkConstraintExist();
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      ontoImporter = new OntologyImporter(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e){
      importResults.setTerminationKO(e.getMessage());
    } catch (RDFImportBadParams e) {
      importResults.setTerminationKO(e.getMessage());
    }

    if (ontoImporter!=null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat,url, rdfFragment, props,  ontoImporter);
        importResults.setTriplesLoaded(ontoImporter.totalTriplesMapped);
        importResults.setTriplesParsed(ontoImporter.totalTriplesParsed);
        importResults.setConfigSummary(props);
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        importResults.setTerminationKO(e.getMessage());
        importResults.setTriplesLoaded(ontoImporter.totalTriplesMapped);
        importResults.setTriplesParsed(ontoImporter.totalTriplesParsed);
        importResults.setConfigSummary(props);
        e.printStackTrace();
      }
    }
    return importResults;
  }

  private void instantiateAndKickOffParser(InputStream inputStream, @Name("url") String url,
      @Name("format") RDFFormat format,
      ConfiguredStatementHandler handler)
      throws IOException {
    RDFParser rdfParser = Rio.createParser(format);
    rdfParser
        .set(BasicParserSettings.VERIFY_URI_SYNTAX, handler.getParserConfig().isVerifyUriSyntax());
    rdfParser.setRDFHandler(handler);
    rdfParser.parse(inputStream, url);
  }

  private InputStream getInputStream(String url, Map<String, Object> props) throws IOException {
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
    }
    if (props.containsKey("payload")) {
      urlConn.setDoOutput(true);
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(urlConn.getOutputStream(), "UTF-8"));
      writer.write(props.get("payload").toString());
      writer.close();
    }
    return urlConn.getInputStream();
  }

  @Procedure(mode = Mode.READ)
  @Description("Parses RDF and produces virtual Nodes and relationships for preview in the Neo4j "
      + "browser. No writing to the DB.")
  public Stream<GraphResult> previewRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws RDFImportException {


    //TODO: add this to props (is it needed? is the commit size not ignored bby the StatementPreviewer???
    //conf.setCommitSize(Long.MAX_VALUE);

    GraphResult graphResult = doPreview(url, null, format, props);
    return Stream.of(graphResult);
  }

  @Procedure(mode = Mode.READ)
  @Description("Parses an RDF fragment passed as parameter (no retrieval from url) and produces "
          + "virtual Nodes and relationships for preview in the Neo4j browser. No writing to the DB.")
  public Stream<GraphResult> previewRDFSnippet(@Name("rdf") String rdfFragment,
                                               @Name("format") String format,
                                               @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
          throws RDFImportException {
    GraphResult graphResult = doPreview(null, rdfFragment, format, props);
    return Stream.of(graphResult);
  }

  private GraphResult doPreview(@Name("url") String url, @Name("rdf") String rdfFragment, @Name("format") String format,
                                @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws RDFImportException {
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    StatementPreviewer statementViewer = null;
    Map<String, Node> virtualNodes = new HashMap<>();
    List<Relationship> virtualRels = new ArrayList<>();

    try {
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      statementViewer =  new StatementPreviewer(db, tx, conf, virtualNodes, virtualRels, log);
    }
    catch (RDFImportBadParams e){
      throw new RDFImportException(e);
    } catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException("A Graph Config is required for RDF importing procedures to run");
    }

    if(statementViewer != null ) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementViewer);
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        throw new RDFImportException(e.getMessage());
      }
    }
    return new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
  }


  @Procedure(mode = Mode.READ)
  @Description(
      "Parses RDF and streams each triple as a record with <S,P,O> along with datatype and "
          + "language tag for Literal values. No writing to the DB.")
  public Stream<StreamedStatement> streamRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws RDFImportException {

    return doStream(url, null, format, props);
  }

  @Procedure(mode = Mode.READ)
  @Description(
          "Parses RDF passed as a string and streams each triple as a record with <S,P,O> along "
                  + "with datatype and language tag for Literal values. No writing to the DB.")
  public Stream<StreamedStatement> streamRDFSnippet(@Name("rdf") String rdf,
                                                    @Name("format") String format,
                                                    @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
          throws RDFImportException {

    return doStream(null, rdf, format, props);
  }

  private Stream<StreamedStatement> doStream(@Name("url") String url,
      @Name("rdfFragment") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws RDFImportException {
    StatementStreamer statementStreamer = null;
    RDFFormat rdfFormat = null;
    RDFParserConfig conf = null;
    try{
      rdfFormat = getFormat(format);
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      statementStreamer = new StatementStreamer(conf);
    }catch (RDFImportBadParams e) {
      throw new RDFImportException(e);
    }catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException("A Graph Config is required for RDF importing procedures to run");
    }

    try {
      parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementStreamer);

    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
      throw new RDFImportException(e.getMessage());
    }
    return statementStreamer.getStatements().stream();
  }


  @Procedure(mode = Mode.WRITE)
  @Description(
      "Deletes triples (parsed from url) from Neo4j. Works on a graph resulted of importing RDF via "
          + "semantics.importRDF(). Delete config must match the one used on import.")
  public Stream<DeleteResults> deleteRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    return Stream.of(doDelete(format, url, null, props));
  }

  @Procedure(mode = Mode.WRITE)
  @Description(
      "Deletes triples (passed as string) from Neo4j. Works on a graph resulted of importing RDF via "
          + "semantics.importRDF(). Delete config must match the one used on import.")
  public Stream<DeleteResults> deleteRDFSnippet(@Name("rdf") String rdf,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    return Stream.of(doDelete(format, null, rdf, props));
  }

  private DeleteResults doDelete(String format, String url, String rdfFragment,
      Map<String, Object> props) {

    DirectStatementDeleter statementDeleter = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    DeleteResults deleteResults = new DeleteResults();

    try {
      checkConstraintExist();
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      statementDeleter = new DirectStatementDeleter(db, tx, conf, log);
    }catch (RDFImportPreRequisitesNotMet e){
      deleteResults.setTerminationKO(e.getMessage());
    } catch (GraphConfig.GraphConfigNotFound e) {
      deleteResults.setTerminationKO("A Graph Config is required for RDF importing procedures to run");
    } catch (RDFImportBadParams e) {
      deleteResults.setTerminationKO(e.getMessage());
    }

    if (statementDeleter != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementDeleter);
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        deleteResults.setTerminationKO(e.getMessage());
        e.printStackTrace();
      } finally {
        deleteResults.setTriplesDeleted(
                statementDeleter.totalTriplesMapped - statementDeleter.getNotDeletedStatementCount());
        deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
        deleteResults.setNamespaces(statementDeleter.getNamespaces());
      }
    }
    return deleteResults;
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<ImportResults> importQuadRDF(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    RDFQuadDirectStatementLoader statementLoader = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    ImportResults importResults = new ImportResults();
      try {
        checkIndexExist();
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      if(rdfFormat!=RDFFormat.TRIG && rdfFormat!=RDFFormat.NQUADS){
        throw new RDFImportBadParams (rdfFormat.getName() + " is not a Quad serialisation format");
      }
      statementLoader = new RDFQuadDirectStatementLoader(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e){
      importResults.setTerminationKO(e.getMessage());
    } catch (GraphConfig.GraphConfigNotFound e) {
      importResults.setTerminationKO("A Graph Config is required for RDF importing procedures to run");
    } catch (RDFImportBadParams e) {
      importResults.setTerminationKO(e.getMessage());
    }

    if (statementLoader != null) {
    try {
      instantiateAndKickOffParser(getInputStream(url, props), url, rdfFormat, statementLoader);
      importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
      importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
      importResults.setNamespaces(statementLoader.getNamespaces());
      importResults.setConfigSummary(props);

    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
      importResults.setTerminationKO(e.getMessage());
      importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
      importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
      importResults.setConfigSummary(props);
    }
  }


    return Stream.of(importResults);
  }


  @Procedure(mode = Mode.WRITE)
  public Stream<DeleteResults> deleteQuadRDF(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    RDFQuadDirectStatementDeleter statementDeleter = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    DeleteResults deleteResults = new DeleteResults();
    try {
      checkIndexExist();
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      if(rdfFormat!=RDFFormat.TRIG && rdfFormat!=RDFFormat.NQUADS){
        throw new RDFImportBadParams (rdfFormat.getName() + " is not a Quad serialisation format");
      }
      statementDeleter = new RDFQuadDirectStatementDeleter(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e){
      deleteResults.setTerminationKO(e.getMessage());
    } catch (GraphConfig.GraphConfigNotFound e) {
      deleteResults.setTerminationKO("A Graph Config is required for RDF deleting procedures to run");
    } catch (RDFImportBadParams e) {
      deleteResults.setTerminationKO(e.getMessage());
    }


    if (statementDeleter != null) {

      try {
        RDFParser rdfParser = Rio.createParser(rdfFormat);
        rdfParser.setRDFHandler(statementDeleter);
        rdfParser.parse(getInputStream(url, props), url);
        deleteResults.setTriplesDeleted(statementDeleter.totalTriplesMapped -
                statementDeleter.getNotDeletedStatementCount());
        deleteResults.setNamespaces(statementDeleter.getNamespaces());
        deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException  e) {
        deleteResults.setTerminationKO(e.getMessage());
        deleteResults.setTriplesDeleted(statementDeleter.totalTriplesMapped -
                statementDeleter.getNotDeletedStatementCount());
        deleteResults.setNamespaces(statementDeleter.getNamespaces());
        deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
      }
    }
    return Stream.of(deleteResults);
  }

  @UserFunction
  @Description("Returns the XMLSchema or custom datatype of a property when present")
  public String getDataType(@Name("literal") Object literal) {

    String result;

    if (literal instanceof String) {
      Matcher matcherShortened = DATATYPE_SHORTENED_PATTERN.matcher((String) literal);
      Matcher matcherRegular = DATATYPE_REGULAR_PATTERN.matcher((String) literal);
      if (matcherShortened.matches()) {
        result = matcherShortened.group(2);
      } else if (matcherRegular.matches()) {
        result = matcherRegular.group(2);
      } else {
        result = XMLSchema.STRING.stringValue();
      }
    } else if (literal instanceof Long) {
      result = XMLSchema.LONG.stringValue();
    } else if (literal instanceof Double) {
      result = XMLSchema.DOUBLE.stringValue();
    } else if (literal instanceof Boolean) {
      result = XMLSchema.BOOLEAN.stringValue();
    } else if (literal instanceof LocalDateTime) {
      result = XMLSchema.DATETIME.stringValue();
    } else if (literal instanceof LocalDate) {
      result = XMLSchema.DATE.stringValue();
    } else {
      result = null;
    }

    return result;
  }

  @UserFunction
  @Description("Returns the value of a datatype of a property after stripping out the datatype "
      + "information when present")
  public String getValue(@Name("literal") String literal) {

    Matcher matcherShortened = DATATYPE_SHORTENED_PATTERN.matcher(literal);
    Matcher matcherRegular = DATATYPE_REGULAR_PATTERN.matcher(literal);
    Matcher matcherLanguageTagged = LANGUAGE_TAGGED_VALUE_PATTERN.matcher(literal);
    String result = literal;
    if (matcherShortened.matches()) {
      result = matcherShortened.group(1);
    } else if (matcherRegular.matches()) {
      result = matcherRegular.group(1);
    } else if (matcherLanguageTagged.matches()) {
      result = matcherLanguageTagged.group(1);
    }
    return result;
  }

  @UserFunction
  @Description("Returns the local part of an IRI")
  public String getIRILocalName(@Name("url") String url) {
    return url.substring(URIUtil.getLocalNameIndex(url));
  }

  @UserFunction
  @Description("Returns the namespace part of an IRI")
  public String getIRINamespace(@Name("url") String url) {
    return url.substring(0, URIUtil.getLocalNameIndex(url));
  }

  @UserFunction
  @Description("Returns the first value with the language tag passed as first argument or null if "
      + "there's not a value for the provided tag")
  public String getLangValue(@Name("lang") String lang, @Name("values") Object values) {

    if (values instanceof List) {
      if (((List) values).get(0) instanceof String) {
        for (Object val : (List<String>) values) {
          Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) val);
          if (m.matches() && m.group(2).equals(lang)) {
            return m.group(1);
          }
        }
      }
    } else if (values instanceof String[]) {
      String[] valuesAsArray = (String[]) values;
      for (int i = 0; i < valuesAsArray.length; i++) {
        Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher(valuesAsArray[i]);
        if (m.matches() && m.group(2).equals(lang)) {
          return m.group(1);
        }
      }
    } else if (values instanceof String) {
      Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) values);
      if (m.matches() && m.group(2).equals(lang)) {
        return m.group(1);
      }
    }
    return null;
  }

  @UserFunction
  @Description("Returns the language tag of a value. Returns null if the value is not a string or"
      + "if the string has no language tag")
  public String getLangTag(@Name("value") Object value) {

    if (value instanceof String) {
      Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) value);
      if (m.matches()) {
        return m.group(2);
      }
    }
    return null;
  }

  @UserFunction
  @Description("Returns false if the value is not a string or if the string is not tagged with the "
      + " given language tag")
  public Boolean hasLangTag(@Name("lang") String lang, @Name("value") Object value) {

    if (value instanceof String) {
      Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) value);
      return m.matches() && m.group(2).equals(lang);
    }
    return false;
  }

  @UserFunction
  @Description("Returns the expanded (full) IRI given a shortened one created in the load process "
      + "with semantics.importRDF")
  public String fullUriFromShortForm(@Name("short") String str)
      throws InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName {

    Matcher m = SHORTENED_URI_PATTERN.matcher(str);
    if (!m.matches()) {
      throw new InvalidShortenedName( "Wrong Syntax: " + str + " is not a valid n10s shortened schema name.");
    }
    NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
    if (!prefixDefs.hasPrefix(m.group(1))) {
      throw new InvalidShortenedName( "Prefix Undefined: " + str + " is using an undefined prefix.");
    }

    return prefixDefs.getNsForPrefix(m.group(1)) + m.group(2);
  }

  @UserFunction
  @Description("Returns the shortened version of an IRI using the existing namespace definitions")
  public String shortFormFromFullUri(@Name("uri") String str)
      throws InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName {

    IRI iri = SimpleValueFactory.getInstance().createIRI(str);
    NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
    if (!prefixDefs.hasNs(iri.getNamespace())) {
      throw new InvalidShortenedName( "Prefix Undefined: No prefix defined for this namespace <"  + str + "> .");
    }
    return prefixDefs.getPrefixForNs(iri.getNamespace()) + PREFIX_SEPARATOR + iri.getLocalName();

  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports a json payload and maps it to nodes and relationships (JSON-LD style). "
      + "Requires a uniqueness constraint on :Resource(uri)")
  public Stream<NodeResult> importJSONAsTree(@Name("containerNode") Node containerNode,
      @Name("jsonpayload") String jsonPayload,
      @Name(value = "connectingRel", defaultValue = "_jsonTree") String relName) throws RDFImportException {

    //emptystring, no parsing and return null
    if (jsonPayload.isEmpty()) {
      return Stream.empty();
    }

    try {
      checkConstraintExist();
      RDFParserConfig conf = new RDFParserConfig(new HashMap<>(), new GraphConfig(tx));
      String containerUri = (String) containerNode.getProperty("uri", null);
      PlainJsonStatementLoader plainJSONStatementLoader = new PlainJsonStatementLoader(db, tx, conf, log);
      if (containerUri == null) {
        containerUri = "neo4j://indiv#" + UUID.randomUUID().toString();
        containerNode.setProperty("uri", containerUri);
        containerNode.addLabel(Label.label("Resource"));
      }
      GenericJSONParser rdfParser = new GenericJSONParser();
      rdfParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
      rdfParser.setRDFHandler(plainJSONStatementLoader);
      rdfParser.parse(new ByteArrayInputStream(jsonPayload.getBytes(Charset.defaultCharset())),
          "neo4j://voc#", containerUri, relName);

    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      throw new RDFImportException(e);
    }catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException("A Graph Config is required for RDF importing procedures to run");
    }

    return Stream.of(new NodeResult(containerNode));
  }

  protected void checkConstraintExist() throws RDFImportPreRequisitesNotMet {

    boolean constraintExists = isConstraintOnResourceUriPresent();

    if (!constraintExists) {
      throw new RDFImportPreRequisitesNotMet(
          "The following constraint is required for importing RDF. Please run 'CREATE CONSTRAINT "
              +  UNIQUENESS_CONSTRAINT_ON_URI
              + " ON (r:Resource) ASSERT r.uri IS UNIQUE' and try again.");
    }

  }

  private boolean isConstraintOnResourceUriPresent() {
    Iterator<ConstraintDefinition> constraintIterator = tx.schema().getConstraints().iterator();

    while (constraintIterator.hasNext()) {
      ConstraintDefinition constraintDef = constraintIterator.next();
      if (constraintDef.isConstraintType(ConstraintType.UNIQUENESS) &&
          constraintDef.getLabel().equals(Label.label("Resource")) &&
          sizeOneAndNameUri(constraintDef.getPropertyKeys().iterator())) {
        return true;
      }
    }
    return false;
  }

  private boolean sizeOneAndNameUri(Iterator<String> iterator) {
    // size one and single value (property key) is uri
    return iterator.hasNext() && iterator.next().equals("uri") && !iterator.hasNext();
  }

  private void checkIndexExist() throws RDFImportPreRequisitesNotMet {
    Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
    if (isConstraintOnResourceUriPresent() || missing(indexes.iterator(), "Resource")) {
      throw new RDFImportPreRequisitesNotMet(
          "An index on :Resource(uri) is required for importing RDF Quads. "
              + "Please run 'CREATE INDEX ON :Resource(uri)' and try again. "
              + "Note that uniqueness constraint needs to be dropped if existing");
    }
  }

  private boolean missing(Iterator<IndexDefinition> iterator, String indexLabel) {
    while (iterator.hasNext()) {
      IndexDefinition indexDef = iterator.next();
      if (!indexDef.isCompositeIndex() && indexDef.getLabels().iterator().next().name()
          .equals(indexLabel) &&
          indexDef.getPropertyKeys().iterator().next().equals("uri")) {
        return false;
      }
    }
    return true;
  }

  protected RDFFormat getFormat(String format) throws RDFImportBadParams {
    if (format != null) {
      for (RDFFormat parser : availableParsers) {
        if (parser.getName().equals(format)) {
          return parser;
        }
      }
    }
    throw new RDFImportBadParams("Unrecognized serialization format: " + format);
  }


  public static class ImportResults {

    public String terminationStatus = "OK";
    public long triplesLoaded = 0;
    public long triplesParsed = 0;
    public Map<String, String> namespaces;
    public String extraInfo = "";
    public Map<String, Object> callParams;

    public void setTriplesLoaded(long count) {
      this.triplesLoaded = count;
    }

    public void setTriplesParsed(long count) {
      this.triplesParsed = count;
    }

    public void setConfigSummary(Map<String, Object> summary) {
      this.callParams = summary;
    }

    public void setNamespaces(Map<String, String> namespaces) {
      this.namespaces = namespaces;
    }

    public void setTerminationKO(String message) {
      this.terminationStatus = "KO";
      this.extraInfo = message;
    }

  }

  public static class DeleteResults {

    public String terminationStatus = "OK";
    public long triplesDeleted = 0;
    public Map<String, String> namespaces;
    public String extraInfo = "";

    public void setTriplesDeleted(long triplesDeleted) {
      this.triplesDeleted = triplesDeleted;
    }

    public void setExtraInfo(String extraInfo) {
      this.extraInfo = extraInfo;
    }

    public void setNamespaces(Map<String, String> namespaces) {
      this.namespaces = namespaces;
    }

    public void setTerminationKO(String message) {
      this.terminationStatus = "KO";
      this.extraInfo = message;
    }

  }

  protected class RDFImportPreRequisitesNotMet extends Throwable {
    public RDFImportPreRequisitesNotMet(String message){
      super(message);
    }
  }

  protected class RDFImportBadParams extends Exception {
    public  RDFImportBadParams(String message){
      super(message);
    }
  }

  protected class InvalidShortenedName extends Exception {
    public InvalidShortenedName(String s) {  super(s); }
  }


}
