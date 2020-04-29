package n10s.rdf.preview;

import java.util.Map;
import java.util.stream.Stream;
import n10s.RDFImportException;
import n10s.rdf.RDFProcedures;
import n10s.result.GraphResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class RDFPreviewProcedures extends RDFProcedures {

  @Procedure(mode = Mode.READ)
  @Description("Parses RDF and produces virtual Nodes and relationships for preview in the Neo4j "
      + "browser. No writing to the DB.")
  public Stream<GraphResult> fetch(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {

    props.put("commitSize",Long.MAX_VALUE);

    GraphResult graphResult = doPreview(url, null, format, props);
    return Stream.of(graphResult);
  }

  @Procedure(mode = Mode.READ)
  @Description("Parses an RDF fragment passed as parameter (no retrieval from url) and produces "
      + "virtual Nodes and relationships for preview in the Neo4j browser. No writing to the DB.")
  public Stream<GraphResult> inline(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {

    props.put("commitSize",Long.MAX_VALUE);

    GraphResult graphResult = doPreview(null, rdfFragment, format, props);
    return Stream.of(graphResult);
  }

}
