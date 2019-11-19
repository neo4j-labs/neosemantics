package semantics;

import static semantics.Params.PREFIX_SEPARATOR;
import static semantics.Params.DATATYPE_SHORTENED_PATTERN;
import static semantics.Params.DATATYPE_REGULAR_PATTERN;
import static semantics.Params.LANGUAGE_TAGGED_VALUE_PATTERN;
import static semantics.Params.SHORTENED_URI_PATTERN;

import com.google.common.base.Preconditions;
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
import java.util.Arrays;
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import semantics.result.GraphResult;
import semantics.result.NamespacePrefixesResult;
import semantics.result.NodeResult;
import semantics.result.StreamedStatement;

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

  public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD,
      RDFFormat.TURTLE, RDFFormat.NTRIPLES, RDFFormat.TRIG, RDFFormat.NQUADS};
  @Context
  public GraphDatabaseService db;
  @Context
  public Log log;

  @Procedure(mode = Mode.WRITE)
  @Description("Imports RDF from an url (file or http) and stores it in Neo4j as a property graph. "
      + "Requires and index on :Resource(uri)")
  public Stream<ImportResults> importRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doImport(format, url, null, props));
  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports an RDF snippet passed as parameter and stores it in Neo4j as a property "
      + "graph. Requires and index on :Resource(uri)")
  public Stream<ImportResults> importRDFSnippet(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doImport(format, null, rdfFragment, props));
  }

  private ImportResults doImport(@Name("format") String format, @Name("url") String url,
      @Name("rdf") String rdfFragment,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    Preconditions.checkArgument(
        Arrays.stream(availableParsers).anyMatch(x -> x.getName().equals(format)),
        "Input format not supported");
    RDFParserConfig conf = new RDFParserConfig(props);

    ImportResults importResults = new ImportResults();

    DirectStatementLoader statementLoader = new DirectStatementLoader(db, conf, log);
    try {
      checkIndexesExist();
      if (rdfFragment != null) {
        parseRDF(new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())),
            "http://neo4j.com/base/", format, statementLoader);
      } else {
        parseRDF(getInputStream(url, props), url, format, statementLoader);
      }
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      importResults.setTerminationKO(e.getMessage());
      importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
      importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
      importResults.setConfigSummary(conf.getConfigSummary());
      e.printStackTrace();

    } finally {
      importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
      importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
      importResults.setNamespaces(statementLoader.getNamespaces());
      importResults.setConfigSummary(conf.getConfigSummary());

    }
    return importResults;
  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof and domain and range info.")
  public Stream<ImportResults> importOntology(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doOntoImport(format, url, null, props));

  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof and domain and range info.")
  public Stream<ImportResults> importOntologySnippet(@Name("rdf") String rdf,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doOntoImport(format, null, rdf, props));

  }

  private ImportResults doOntoImport(String format, String url,
      String rdfFragment, Map<String, Object> props) {

    //url handling settings will be ignored
    props.put("handleVocabUris","IGNORE");

    OntologyLoaderConfig conf = new OntologyLoaderConfig(props);

    ImportResults importResults = new ImportResults();

    OntologyImporter ontoImporter = new OntologyImporter(db, conf, log);
    try {
      checkIndexesExist();
      if (rdfFragment != null) {
        parseRDF(new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())),
            "http://neo4j.com/base/", format, ontoImporter);
      } else {
        parseRDF(getInputStream(url, props), url, format, ontoImporter);
      }
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      importResults.setTerminationKO(e.getMessage());
      importResults.setTriplesLoaded(ontoImporter.totalTriplesMapped);
      importResults.setTriplesParsed(ontoImporter.totalTriplesParsed);
      importResults.setConfigSummary(conf.getConfigSummary());
      e.printStackTrace();
    } finally {
      importResults.setTriplesLoaded(ontoImporter.totalTriplesMapped);
      importResults.setTriplesParsed(ontoImporter.totalTriplesParsed);
      importResults.setConfigSummary(conf.getConfigSummary());
    }
    return importResults;
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<ImportResults> importQuadRDF(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    Preconditions.checkArgument(
        format.equals(RDFFormat.TRIG.getName()) || format.equals(RDFFormat.NQUADS.getName()),
        "Input format not supported");
    RDFParserConfig conf = new RDFParserConfig(props);

    ImportResults importResults = new ImportResults();

    RDFQuadDirectStatementLoader statementLoader = new RDFQuadDirectStatementLoader(db, conf,
        log);
    try {
      checkIndexesExist();
      parseRDF(getInputStream(url, props), url, format, statementLoader);

    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      importResults.setTerminationKO(e.getMessage());
      e.printStackTrace();
    } finally {
      importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
      importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
      importResults.setNamespaces(statementLoader.getNamespaces());
      importResults.setConfigSummary(conf.getConfigSummary());
    }
    return Stream.of(importResults);
  }

  private void parseRDF(InputStream inputStream, @Name("url") String url,
      @Name("format") String format,
      ConfiguredStatementHandler handler)
      throws IOException, RDFImportPreRequisitesNotMet {
    RDFParser rdfParser = Rio.createParser(getFormat(format));
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
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    Preconditions.checkArgument(
        Arrays.stream(availableParsers).anyMatch(x -> x.getName().equals(format)),
        "Input format not supported");
    RDFParserConfig conf = new RDFParserConfig(props);
    conf.setCommitSize(Long.MAX_VALUE);

    Map<String, Node> virtualNodes = new HashMap<>();
    List<Relationship> virtualRels = new ArrayList<>();

    StatementPreviewer statementViewer = new StatementPreviewer(db, conf, virtualNodes, virtualRels,
        log);
    try {
      parseRDF(getInputStream(url, props), url, format, statementViewer);
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      e.printStackTrace();
    }

    GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
    return Stream.of(graphResult);


  }

  @Procedure(mode = Mode.READ)
  @Description(
      "Parses RDF and streams each triple as a record with <S,P,O> along with datatype and "
          + "language tag for Literal values. No writing to the DB.")
  public Stream<StreamedStatement> streamRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    Preconditions.checkArgument(
        Arrays.stream(availableParsers).anyMatch(x -> x.getName().equals(format)),
        "Input format not supported");
    final boolean verifyUriSyntax = (props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true);

    return doStream(url, null, format, props);
  }

  private Stream<StreamedStatement> doStream(@Name("url") String url, @Name("rdfFragment") String rdfFragment,
      @Name("format") String format, @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    StatementStreamer statementStreamer = new StatementStreamer(new RDFParserConfig(props));
    try {
      if (rdfFragment != null) {
        parseRDF(new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())),
            "http://neo4j.com/base/", format, statementStreamer);
      } else {
        parseRDF(getInputStream(url, props), url, format, statementStreamer);
      }

    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      e.printStackTrace();
      statementStreamer.setErrorMsg(e.getMessage());
    }
    return statementStreamer.getStatements().stream();
  }

  @Procedure(mode = Mode.READ)
  @Description(
      "Parses RDF passed as a string and streams each triple as a record with <S,P,O> along "
          + "with datatype and language tag for Literal values. No writing to the DB.")
  public Stream<StreamedStatement> streamRDFSnippet(@Name("rdf") String rdf, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    Preconditions.checkArgument(
        Arrays.stream(availableParsers).anyMatch(x -> x.getName().equals(format)),
        "Input format not supported");
    final boolean verifyUriSyntax = (props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true);

    return doStream(null, rdf, format, props);
  }

  @Procedure(mode = Mode.READ)
  @Description("Parses an RDF fragment passed as parameter (no retrieval from url) and produces "
      + "virtual Nodes and relationships for preview in the Neo4j browser. No writing to the DB.")
  public Stream<GraphResult> previewRDFSnippet(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    Preconditions.checkArgument(
        Arrays.stream(availableParsers).anyMatch(x -> x.getName().equals(format)),
        "Input format not supported");
    RDFParserConfig conf = new RDFParserConfig(props);
    conf.setCommitSize(Long.MAX_VALUE);

    Map<String, Node> virtualNodes = new HashMap<>();
    List<Relationship> virtualRels = new ArrayList<>();

    StatementPreviewer statementViewer = new StatementPreviewer(db, conf, virtualNodes, virtualRels,
        log);
    try {
      parseRDF(new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())),
          "http://neo4j.com/base/", format, statementViewer);
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      e.printStackTrace();
    }

    GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
    return Stream.of(graphResult);


  }

  @Procedure(mode = Mode.WRITE)
  @Description("Deletes triples (parsed from url) from Neo4j. Works on a graph resulted of importing RDF via "
      + "semantics.importRDF(). Delete config must match the one used on import.")
  public Stream<DeleteResults> deleteRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    return Stream.of(doDelete(format, url, null, props));
  }

  @Procedure(mode = Mode.WRITE)
  @Description("Deletes triples (passed as string) from Neo4j. Works on a graph resulted of importing RDF via "
      + "semantics.importRDF(). Delete config must match the one used on import.")
  public Stream<DeleteResults> deleteRDFSnippet(@Name("rdf") String rdf, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    return Stream.of(doDelete(format, null, rdf, props));
  }

  private DeleteResults doDelete(String format, String url, String rdfFragment, Map<String, Object> props) {
    Preconditions.checkArgument(
        Arrays.stream(availableParsers).anyMatch(x -> x.getName().equals(format)),
        "Input format not supported");
    RDFParserConfig conf = new RDFParserConfig(props);
    conf.setCommitSize(Long.MAX_VALUE);

    DeleteResults deleteResults = new DeleteResults();

    DirectStatementDeleter statementDeleter = new DirectStatementDeleter(db, conf, log);
    try {
      checkIndexesExist();
      InputStream inputStream;

      if (rdfFragment != null) {
        inputStream = new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset()));
        url = "http://neo4j.com/base/";
      } else {
        inputStream = getInputStream(url, props);
      }

      RDFParser rdfParser = Rio.createParser(getFormat(format));
      rdfParser.setRDFHandler(statementDeleter);
      rdfParser.parse(inputStream, url);
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      deleteResults.setTerminationKO(e.getMessage());
      e.printStackTrace();
    } finally {
      deleteResults.setTriplesDeleted(
          statementDeleter.totalTriplesMapped - statementDeleter.getNotDeletedStatementCount());
      deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
      deleteResults.setNamespaces(statementDeleter.getNamespaces());
    }
    return deleteResults;
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<DeleteResults> deleteQuadRDF(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    Preconditions.checkArgument(format.equals("TriG") || format.equals("N-Quads"),
        "Input format not supported");
    RDFParserConfig conf = new RDFParserConfig(props);
    conf.setCommitSize(Long.MAX_VALUE);

    DeleteResults deleteResults = new DeleteResults();

    RDFQuadDirectStatementDeleter statementDeleter = new RDFQuadDirectStatementDeleter(db,
        conf, log);
    try {
      checkIndexesExist();

      InputStream inputStream = getInputStream(url, props);
      RDFParser rdfParser = Rio.createParser(getFormat(format));
      rdfParser.setRDFHandler(statementDeleter);
      rdfParser.parse(inputStream, url);
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      deleteResults.setTerminationKO(e.getMessage());
      e.printStackTrace();
    } finally {
      deleteResults.setTriplesDeleted(
          statementDeleter.totalTriplesMapped - statementDeleter.getNotDeletedStatementCount());
      deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
      deleteResults.setNamespaces(statementDeleter.getNamespaces());
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
  public String uriFromShort(@Name("short") String str) {

    Matcher m = SHORTENED_URI_PATTERN.matcher(str);
    if (m.matches()) {
      ResourceIterator<Node> nspd = db.findNodes(Label.label("NamespacePrefixDefinition"));
      if (nspd.hasNext()) {
        Map<String, Object> namespaces = nspd.next().getAllProperties();
        Iterator<Map.Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
        while (nsIterator.hasNext()) {
          Map.Entry<String, Object> kv = nsIterator.next();
          if (m.group(1).equals(kv.getValue())) {
            return kv.getKey() + m.group(2);
          }
        }
      }
    }
    //default return original value
    return str;
  }

  @UserFunction
  @Description("Returns the shortened version of an IRI using the existing namespace definitions")
  public String shortFromUri(@Name("uri") String str) {
    try {
      IRI iri = SimpleValueFactory.getInstance().createIRI(str);
      ResourceIterator<Node> nspd = db.findNodes(Label.label("NamespacePrefixDefinition"));
      if (nspd.hasNext()) {
        Map<String, Object> namespaces = nspd.next().getAllProperties();
        Iterator<Map.Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
        while (nsIterator.hasNext()) {
          Map.Entry<String, Object> kv = nsIterator.next();
          if (kv.getKey().equals(iri.getNamespace())) {
            return kv.getValue() + PREFIX_SEPARATOR + iri.getLocalName();
          }
        }
      }
      return str;
    } catch (Exception e) {
      return str;
    }
  }

  @Procedure(mode = Mode.WRITE)
  @Description("Adds namespace - prefix pair definition")
  public Stream<NamespacePrefixesResult> addNamespacePrefix(@Name("prefix") String prefix,
      @Name("ns") String ns) {

    Map<String, Object> params = new HashMap<>();
    params.put("prefix", prefix);

    return db
        .execute(String.format("MERGE (n:NamespacePrefixDefinition) SET n.`%s` = $prefix "
            + "WITH n UNWIND keys(n) as ns\n"
            + "RETURN n[ns] as prefix, ns as namespace", ns), params).stream().map(
            n -> new NamespacePrefixesResult((String) n.get("prefix"),
                (String) n.get("namespace")));

  }

  @Procedure(mode = Mode.WRITE)
  @Description("Adds namespace - prefix pair definition")
  public Stream<NamespacePrefixesResult> addNamespacePrefixesFromText(@Name("prefix") String prefix,
      @Name("ns") String ns) {

    Map<String, Object> params = new HashMap<>();
    params.put("prefix", prefix);

    return db
        .execute(String.format("MERGE (n:NamespacePrefixDefinition) SET n.`%s` = $prefix "
            + "WITH n UNWIND keys(n) as ns\n"
            + "RETURN n[ns] as prefix, ns as namespace", ns), params).stream().map(
            n -> new NamespacePrefixesResult((String) n.get("prefix"),
                (String) n.get("namespace")));

  }

  @Procedure(mode = Mode.READ)
  @Description("Lists all existing namespace prefix definitions")
  public Stream<NamespacePrefixesResult> listNamespacePrefixes() {

    return db
        .execute("MATCH (n:NamespacePrefixDefinition) \n" +
            "UNWIND keys(n) AS namespace\n" +
            "RETURN namespace, n[namespace] AS prefix").stream().map(
            n -> new NamespacePrefixesResult((String) n.get("prefix"),
                (String) n.get("namespace")));

  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports a json payload and maps it to nodes and relationships (JSON-LD style). "
      + "Requires a uniqueness constraint on :Resource(uri)")
  public Stream<NodeResult> importJSONAsTree(@Name("containerNode") Node containerNode,
      @Name("jsonpayload") String jsonPayload,
      @Name(value = "connectingRel", defaultValue = "_jsonTree") String relName) {


    //emptystring, no parsing and return null
    if (jsonPayload.isEmpty()) return null;

    HashMap<String, Object> params = new HashMap<>();
    params.put("handleVocabUris","IGNORE");
    params.put("commitSize",Long.MAX_VALUE);
    RDFParserConfig conf = new RDFParserConfig(params);

    PlainJsonStatementLoader plainJSONStatementLoader = new PlainJsonStatementLoader(db, conf, log);
    try {
      checkIndexesExist();
      String containerUri = (String)containerNode.getProperty("uri", null);
      if (containerUri == null ){
        containerUri = "neo4j://indiv#" + UUID.randomUUID().toString();
        containerNode.setProperty("uri",containerUri);
        containerNode.addLabel(Label.label("Resource"));
      }
      GenericJSONParser rdfParser = new GenericJSONParser();
      rdfParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
      rdfParser.setRDFHandler(plainJSONStatementLoader);
      rdfParser.parse(new ByteArrayInputStream(jsonPayload.getBytes(Charset.defaultCharset())),
          "neo4j://voc#", containerUri,relName);

    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      e.printStackTrace();

    }
    return Stream.of(new NodeResult(containerNode));
  }

  private void checkIndexesExist() throws RDFImportPreRequisitesNotMet {
    Iterable<IndexDefinition> indexes = db.schema().getIndexes();
    if (missing(indexes.iterator(), "Resource")) {
      throw new RDFImportPreRequisitesNotMet(
          "The following index is required for importing RDF. Please run 'CREATE INDEX ON :Resource(uri)' and try again.");
    }
  }

  private boolean missing(Iterator<IndexDefinition> iterator, String indexLabel) {
    while (iterator.hasNext()) {
      IndexDefinition indexDef = iterator.next();
      if (!indexDef.isCompositeIndex() && indexDef.getLabels().iterator().next().name().equals(indexLabel) &&
          indexDef.getPropertyKeys().iterator().next().equals("uri")) {
        return false;
      }
    }
    return true;
  }

  private RDFFormat getFormat(String format) throws RDFImportPreRequisitesNotMet {
    if (format != null) {
      for (RDFFormat parser : availableParsers) {
        if (parser.getName().equals(format)) {
          return parser;
        }
      }
    }
    throw new RDFImportPreRequisitesNotMet("Unrecognized serialization format: " + format);
  }


  public static class ImportResults {

    public String terminationStatus = "OK";
    public long triplesLoaded = 0;
    public long triplesParsed = 0;
    public Map<String, String> namespaces;
    public String extraInfo = "";
    public Map<String, Object> configSummary;

    public void setTriplesLoaded(long count) {
      this.triplesLoaded = count;
    }

    public void setTriplesParsed(long count) {
      this.triplesParsed = count;
    }

    public void setConfigSummary(Map<String, Object> summary) {
      this.configSummary = summary;
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

  private class RDFImportPreRequisitesNotMet extends Exception {

    String message;

    public RDFImportPreRequisitesNotMet(String s) {
      message = s;
    }

    @Override
    public String getMessage() {
      return message;
    }
  }
}
