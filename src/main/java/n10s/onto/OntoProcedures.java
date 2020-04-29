package n10s.onto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import n10s.CommonProcedures;
import n10s.ConfiguredStatementHandler.TripleLimitReached;
import n10s.RDFImportException;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import n10s.graphconfig.RDFParserConfig;
import n10s.rdf.RDFProcedures.ImportResults;
import n10s.result.GraphResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;

public class OntoProcedures extends CommonProcedures {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;


  protected ImportResults doOntoImport(String format, String url,
      String rdfFragment, Map<String, Object> props) throws GraphConfigNotFound {

    // TODO: This effectively overrides the graphconfig (and can cause conflict?)
    props.put("handleVocabUris", "IGNORE");

    OntologyImporter ontoImporter = null;
    RDFParserConfig conf;
    RDFFormat rdfFormat = null;
    ImportResults importResults = new ImportResults();
    try {
      checkConstraintExist();
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      ontoImporter = new OntologyImporter(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e) {
      importResults.setTerminationKO(e.getMessage());
    } catch (RDFImportBadParams e) {
      importResults.setTerminationKO(e.getMessage());
    }

    if (ontoImporter != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, ontoImporter);
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

  protected GraphResult doPreviewOnto(@Name("url") String url, @Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    OntologyPreviewer ontoViewer = null;
    Map<String, Node> virtualNodes = new HashMap<>();
    List<Relationship> virtualRels = new ArrayList<>();

    try {
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      ontoViewer = new OntologyPreviewer(db, tx, conf, virtualNodes, virtualRels, log);
    } catch (RDFImportBadParams e) {
      throw new RDFImportException(e);
    } catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException(
          "A Graph Config is required for the Ontology preview procedure to run");
    }

    if (ontoViewer != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, ontoViewer);
      } catch (TripleLimitReached e){
        //preview interrupted by reaching the triple limit. All good.
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        throw new RDFImportException(e.getMessage());
      }
    }
    return new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
  }

}
