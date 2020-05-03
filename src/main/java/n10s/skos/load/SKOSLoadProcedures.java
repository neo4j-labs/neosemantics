package n10s.skos.load;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import n10s.RDFImportException;
import n10s.experimental.PlainJsonStatementLoader;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.RDFParserConfig;
import n10s.rdf.RDFProcedures;
import n10s.result.NodeResult;
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

public class SKOSLoadProcedures extends RDFProcedures {

  @Procedure(name = "n10s.skos.import.fetch", mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof and domain and range info.")
  public Stream<ImportResults> importSKOSFromUrl(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {

    return Stream.of(doSkosImport(format, url, null, props));

  }

  @Procedure(name = "n10s.skos.import.inline", mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof and domain and range info.")
  public Stream<ImportResults> importSKOSInline(@Name("skosFragment") String skosFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {

    return Stream.of(doSkosImport(format, null, skosFragment, props));

  }


  private ImportResults doSkosImport(String format, String url,
      String rdfFragment, Map<String, Object> props) throws RDFImportException {

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
    } catch (RDFImportPreRequisitesNotMet e) {
      importResults.setTerminationKO(e.getMessage());
    } catch (RDFImportBadParams e) {
      importResults.setTerminationKO(e.getMessage());
    } catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException(
          "A Graph Config is required for the SKOS import procedure to run");
    }

    if (skosImporter != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, skosImporter);
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

}
