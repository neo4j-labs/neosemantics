package semantics.experimental;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import semantics.RDFImport;
import semantics.graphconfig.GraphConfig;
import semantics.graphconfig.GraphConfig.GraphConfigNotFound;
import semantics.graphconfig.RDFParserConfig;

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

}
