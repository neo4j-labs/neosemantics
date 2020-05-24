package n10s;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;

public class CommonProcedures {


  public static final String UNIQUENESS_CONSTRAINT_ON_URI = "n10s_unique_uri";

  protected static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML,
      RDFFormat.JSONLD,
      RDFFormat.TURTLE, RDFFormat.NTRIPLES, RDFFormat.TRIG, RDFFormat.NQUADS,
      RDFFormat.TURTLESTAR, RDFFormat.TRIGSTAR};

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  protected void checkConstraintExist() throws RDFImportPreRequisitesNotMet {

    boolean constraintExists = isConstraintOnResourceUriPresent();

    if (!constraintExists) {
      throw new RDFImportPreRequisitesNotMet(
          "The following constraint is required for importing RDF. Please run 'CREATE CONSTRAINT "
              + UNIQUENESS_CONSTRAINT_ON_URI
              + " ON (r:Resource) ASSERT r.uri IS UNIQUE' and try again.");
    }

  }

  protected boolean isConstraintOnResourceUriPresent() {
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

  protected boolean sizeOneAndNameUri(Iterator<String> iterator) {
    // size one and single value (property key) is uri
    return iterator.hasNext() && iterator.next().equals("uri") && !iterator.hasNext();
  }

  protected void checkIndexExist() throws RDFImportPreRequisitesNotMet {
    Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
    if (isConstraintOnResourceUriPresent() || missing(indexes.iterator(), "Resource")) {
      throw new RDFImportPreRequisitesNotMet(
          "An index on :Resource(uri) is required for importing RDF Quads. "
              + "Please run 'CREATE INDEX ON :Resource(uri)' and try again. "
              + "Note that uniqueness constraint needs to be dropped if existing");
    }
  }

  protected boolean missing(Iterator<IndexDefinition> iterator, String indexLabel) {
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

  protected void parseRDFPayloadOrFromUrl( RDFFormat format, String url, String rdfFragment,
     Map<String, Object> props, ConfiguredStatementHandler statementLoader) throws IOException {
    if (rdfFragment != null) {
      instantiateAndKickOffParser(
          new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())),
          "http://neo4j.com/base/", format, statementLoader);
    } else {
      instantiateAndKickOffParser(getInputStream(url, props), url, format, statementLoader);
    }
  }

  protected void instantiateAndKickOffParser(InputStream inputStream, @Name("url") String url,
      @Name("format") RDFFormat format,
      ConfiguredStatementHandler handler)
      throws IOException {
    RDFParser rdfParser = Rio.createParser(format);
    rdfParser
        .set(BasicParserSettings.VERIFY_URI_SYNTAX, handler.getParserConfig().isVerifyUriSyntax());
    rdfParser.setRDFHandler(handler);
    rdfParser.parse(inputStream, url);
  }

  protected InputStream getInputStream(String url, Map<String, Object> props) throws IOException {
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


  public class RDFImportPreRequisitesNotMet extends Throwable {

    public RDFImportPreRequisitesNotMet(String message) {
      super(message);
    }
  }

  public class RDFImportBadParams extends Exception {

    public RDFImportBadParams(String message) {
      super(message);
    }
  }

  public class InvalidShortenedName extends Exception {

    public InvalidShortenedName(String s) {
      super(s);
    }
  }
}
