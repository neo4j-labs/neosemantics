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
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.IOUtils;
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
  public static final String UNIQUENESS_CONSTRAINT_STATEMENT = "CREATE CONSTRAINT " + UNIQUENESS_CONSTRAINT_ON_URI +
          " FOR (r:Resource) REQUIRE r.uri IS UNIQUE";

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
          "The following constraint is required for importing RDF. Please run '" + UNIQUENESS_CONSTRAINT_STATEMENT + "' and try again.");
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

    if (isConstraintOnResourceUriPresent() || missingIndexOnResourceUri()) {
      throw new RDFImportPreRequisitesNotMet(
          "An index on :Resource(uri) is required for importing RDF Quads. "
              + "Please run 'CREATE INDEX ON :Resource(uri)' and try again. "
              + "Note that uniqueness constraint needs to be dropped if existing");
    }
  }

  protected boolean missingIndexOnResourceUri() {
    Iterator<IndexDefinition> indexesIterator = tx.schema().getIndexes().iterator();
    while (indexesIterator.hasNext()) {
      IndexDefinition indexDef = indexesIterator.next();
      if (!indexDef.isCompositeIndex() && indexDef.isNodeIndex() &&
              indexDef.getLabels().iterator().hasNext() &&
              indexDef.getLabels().iterator().next().name().equals("Resource") &&
              indexDef.getPropertyKeys().iterator().hasNext() &&
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

    if ((url.contains(".zip") || url.contains(".tgz")|| url.contains(".gz")|| url.contains(".bz2"))) {
      String zipFileName = null;
      if (url.contains("!")) {
        String[] tokens = url.split("!");
        url = tokens[0];
        zipFileName = tokens[1];
        if (zipFileName.equals("")){
          throw new IllegalArgumentException("filename can't be empty");
        }
        if (!(url.endsWith(".tar.gz") || url.endsWith(".tgz")||url.endsWith(".zip"))){
          throw new IllegalArgumentException("'!' notation for filenames can only be used with zip or tgz files");
        }
      }
      urlConn = new URL(url).openConnection();

      if (url.endsWith(".zip")) {
        ZipInputStream zip = new ZipInputStream(urlConn.getInputStream());
        if (zipFileName!=null){
          ZipEntry zipEntry;
          while ((zipEntry = zip.getNextEntry()) != null) {
            if (!zipEntry.isDirectory() && zipEntry.getName().equals(zipFileName)) {
              return new ByteArrayInputStream(IOUtils.toByteArray(zip));
            }
          }
        }else {
          throw new IllegalArgumentException("Filename is required for zip files (use '!' notation)");
        }
      } else if (url.endsWith(".tar.gz") || url.endsWith(".tgz")) {
        TarArchiveInputStream tgz = new TarArchiveInputStream(new GZIPInputStream(urlConn.getInputStream()));
        if (zipFileName!=null){
          ArchiveEntry zipEntry;
          while ((zipEntry = tgz.getNextEntry()) != null) {
            if (!zipEntry.isDirectory() && zipEntry.getName().equals(zipFileName)) {
              return new ByteArrayInputStream(IOUtils.toByteArray(tgz));
            }
          }
        } else {
          return tgz;
        }
      } else if (url.endsWith(".gz")){
        return new GZIPInputStream(urlConn.getInputStream());
      } else if (url.endsWith(".bz2")){
        return new BZip2CompressorInputStream(urlConn.getInputStream());
      }
    }else {
      urlConn = new URL(url).openConnection();
    }


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
      String newUrl = handleRedirect(urlConn, url);
      if (newUrl != null && !url.equals(newUrl)) {
        urlConn.getInputStream().close();
        return getInputStream(newUrl, props);
      }
      return urlConn.getInputStream();
  }

  //Taken from APOC (apoc.util.Util)
  private static String handleRedirect(URLConnection con, String url) throws IOException {
    if (!(con instanceof HttpURLConnection)) return url;
    if (!isRedirect(((HttpURLConnection)con))) return url;
    return con.getHeaderField("Location");
  }

  public static boolean isRedirect(HttpURLConnection con) throws IOException {
    int code = con.getResponseCode();
    boolean isRedirectCode = code >= 300 && code < 400;
    if (isRedirectCode) {
      URL location = new URL(con.getHeaderField("Location"));
      String oldProtocol = con.getURL().getProtocol();
      String protocol = location.getProtocol();
      if (!protocol.equals(oldProtocol) && !protocol.startsWith(oldProtocol)) { // we allow http -> https redirect and similar
        throw new RuntimeException("The redirect URI has a different protocol: " + location.toString());
      }
    }
    return isRedirectCode;
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


  public class RDFImportPreRequisitesNotMet extends Exception {

    public RDFImportPreRequisitesNotMet(String message) {
      super(message);
    }
  }

  public class RDFImportBadParams extends Exception {

    public RDFImportBadParams(String message) {
      super(message);
    }
  }

  public class InvalidURI extends Exception {

    public InvalidURI(String message) {
      super(message);
    }
  }

  public class InvalidShortenedName extends Exception {

    public InvalidShortenedName(String s) {
      super(s);
    }
  }
}
