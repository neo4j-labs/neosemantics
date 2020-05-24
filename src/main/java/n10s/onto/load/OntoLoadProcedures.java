package n10s.onto.load;

import java.util.Map;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import n10s.graphconfig.GraphConfig.InvalidParamException;
import n10s.onto.OntoProcedures;
import n10s.rdf.RDFProcedures.ImportResults;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class OntoLoadProcedures extends OntoProcedures {

  @Procedure(name = "n10s.onto.import.fetch", mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof, and domain and range info.")
  public Stream<ImportResults> fetch(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws GraphConfigNotFound, InvalidParamException {

    return Stream.of(doOntoImport(format, url, null, props));

  }

  @Procedure(name = "n10s.onto.import.inline", mode = Mode.WRITE)
  @Description("Imports classes, properties (dataType and Object), hierarchies thereof, and domain and range info.")
  public Stream<ImportResults> inline(@Name("rdf") String rdf,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws GraphConfigNotFound, InvalidParamException {

    return Stream.of(doOntoImport(format, null, rdf, props));

  }


}
