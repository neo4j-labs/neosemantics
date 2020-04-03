package semantics.experimental;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.jsonld.GenericJSONParser;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import semantics.RDFImport;
import semantics.RDFImportException;
import semantics.graphconfig.GraphConfig;
import semantics.graphconfig.GraphConfig.GraphConfigNotFound;
import semantics.graphconfig.RDFParserConfig;
import semantics.result.NodeResult;

public class ExperimentalImports extends RDFImport {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;


  @Procedure(mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof and domain and range info.")
  public Stream<ImportResults> importSKOS(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws GraphConfigNotFound {

    return Stream.of(doSkosImport(format, url, null, props));

  }


  private ImportResults doSkosImport(String format, String url,
      String rdfFragment, Map<String, Object> props) throws GraphConfigNotFound {

    // TODO: This effectively overrides the graphconfig (and can cause conflict?)
    props.put("handleVocabUris", "IGNORE");


    SkosImporter skosImporter = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    ImportResults importResults = new ImportResults();
    try {
      checkConstraintExist();
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      skosImporter = new SkosImporter(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e){
      importResults.setTerminationKO(e.getMessage());
    } catch (RDFImportBadParams e) {
      importResults.setTerminationKO(e.getMessage());
    }

    if (skosImporter!=null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat,url, rdfFragment, props,  skosImporter);
        importResults.setTriplesLoaded(skosImporter.totalTriplesMapped);
        importResults.setTriplesParsed(skosImporter.totalTriplesParsed);
        importResults.setConfigSummary(props);
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        importResults.setTerminationKO(e.getMessage());
        importResults.setTriplesLoaded(skosImporter.totalTriplesMapped);
        importResults.setTriplesParsed(skosImporter.totalTriplesParsed);
        importResults.setConfigSummary(props);
        e.printStackTrace();
      }
    }
    return importResults;
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

}
