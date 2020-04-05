package n10s.quadrdf.load;

import java.util.Map;
import java.util.stream.Stream;
import n10s.quadrdf.QuadRDFProcedures;
import n10s.rdf.RDFProcedures.ImportResults;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class QuadRDFLoadProcedures extends QuadRDFProcedures {

  @Procedure(mode = Mode.WRITE)
  public Stream<ImportResults> fetch(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doQuadRDFImport(format, url, null, props));

  }

  @Procedure(mode = Mode.WRITE)
  @Description("Imports an RDF snippet passed as parameter and stores it in Neo4j as a property "
      + "graph. Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> inline(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doQuadRDFImport(format, null, rdfFragment, props));
  }

}
