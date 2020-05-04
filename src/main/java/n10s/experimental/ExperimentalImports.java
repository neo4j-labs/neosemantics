package n10s.experimental;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;
import n10s.RDFImportException;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.RDFParserConfig;
import n10s.rdf.RDFProcedures;
import n10s.result.NodeResult;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.jsonld.GenericJSONParser;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class ExperimentalImports extends RDFProcedures {

  @Procedure(mode = Mode.WRITE)
  @Description("Imports a json payload and maps it to nodes and relationships (JSON-LD style). "
      + "Requires a uniqueness constraint on :Resource(uri)")
  public Stream<NodeResult> importJSONAsTree(@Name("containerNode") Node containerNode,
      @Name("jsonpayload") String jsonPayload,
      @Name(value = "connectingRel", defaultValue = "_jsonTree") String relName)
      throws RDFImportException {

    //emptystring, no parsing and return null
    if (jsonPayload.isEmpty()) {
      return Stream.empty();
    }

    try {
      checkConstraintExist();
      RDFParserConfig conf = new RDFParserConfig(new HashMap<>(), new GraphConfig(tx));
      String containerUri = (String) containerNode.getProperty("uri", null);
      PlainJsonStatementLoader plainJSONStatementLoader = new PlainJsonStatementLoader(db, tx, conf,
          log);
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
    } catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException(
          "A Graph Config is required for RDF importing procedures to run");
    }

    return Stream.of(new NodeResult(containerNode));
  }

}
