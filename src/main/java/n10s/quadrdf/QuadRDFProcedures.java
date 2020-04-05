package n10s.quadrdf;

import java.io.IOException;
import java.util.Map;
import n10s.CommonProcedures;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.RDFParserConfig;
import n10s.rdf.RDFProcedures.DeleteResults;
import n10s.rdf.RDFProcedures.ImportResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.procedure.Name;

public class QuadRDFProcedures extends CommonProcedures {

  protected ImportResults doQuadRDFImport(@Name("format") String format, @Name("url") String url,
      @Name("rdf") String rdfFragment,
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
        throw new RDFImportBadParams(rdfFormat.getName() + " is not a Quad serialisation format");
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


  protected DeleteResults doQuadRDFDelete(@Name("format") String format, @Name("url") String url,
      @Name("rdf") String rdfFragment,
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
        throw new RDFImportBadParams(rdfFormat.getName() + " is not a Quad serialisation format");
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
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementDeleter);
        deleteResults.setTriplesDeleted(statementDeleter.totalTriplesMapped -
            statementDeleter.getNotDeletedStatementCount());
        deleteResults.setNamespaces(statementDeleter.getNamespaces());
        deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        deleteResults.setTerminationKO(e.getMessage());
        deleteResults.setTriplesDeleted(statementDeleter.totalTriplesMapped -
            statementDeleter.getNotDeletedStatementCount());
        deleteResults.setNamespaces(statementDeleter.getNamespaces());
        deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
      }
    }
    return deleteResults;
  }
}
