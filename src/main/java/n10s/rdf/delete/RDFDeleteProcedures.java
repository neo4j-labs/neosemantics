package n10s.rdf.delete;

import java.util.Map;
import java.util.stream.Stream;
import n10s.rdf.RDFProcedures;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class RDFDeleteProcedures extends RDFProcedures {


  @Procedure(mode = Mode.WRITE)
  @Description(
      "Deletes triples (parsed from url) from Neo4j. Works on a graph resulted of importing RDF via "
          + "n10s.rdf.import ")
  public Stream<DeleteResults> fetch(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    return Stream.of(doDelete(format, url, null, props));
  }

  @Procedure(mode = Mode.WRITE)
  @Description(
      "Deletes triples (passed as string) from Neo4j. Works on a graph resulted of importing RDF via "
          + "n10s.rdf.import ")
  public Stream<DeleteResults> inline(@Name("rdf") String rdf,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    return Stream.of(doDelete(format, null, rdf, props));
  }

}
