package n10s.quadrdf.delete;

import java.util.Map;
import java.util.stream.Stream;
import n10s.quadrdf.QuadRDFProcedures;
import n10s.rdf.RDFProcedures.DeleteResults;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class QuadRDFDeleteProcedures extends QuadRDFProcedures {

  @Procedure(name = "n10s.experimental.quadrdf.delete.url", mode = Mode.WRITE)
  public Stream<DeleteResults> fetch(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doQuadRDFDelete(format, url, null, props));

  }

  @Procedure(name = "n10s.experimental.quadrdf.delete.inline", mode = Mode.WRITE)
  public Stream<DeleteResults> inline(@Name("url") String rdf,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doQuadRDFDelete(format, null, rdf, props));

  }

}
