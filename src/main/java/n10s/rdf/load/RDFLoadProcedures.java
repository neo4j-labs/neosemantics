package n10s.rdf.load;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.InvalidParamException;
import n10s.rdf.RDFProcedures;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class RDFLoadProcedures extends RDFProcedures {

  @Procedure(name = "n10s.rdf.import.fetch", mode = Mode.WRITE)
  @Description("Imports RDF from an url (file or http) and stores it in Neo4j as a property graph. "
      + "Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> fetch(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doImport(format, url, null, props, null));
  }

  @Procedure(name = "n10s.rdf.import.inline", mode = Mode.WRITE)
  @Description("Imports an RDF snippet passed as parameter and stores it in Neo4j as a property "
      + "graph. Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> inline(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    return Stream.of(doImport(format, null, rdfFragment, props, null));
  }

  @Procedure(name = "n10s.experimental.validation.shacl.import.fetch", mode = Mode.WRITE)
  @Description("Imports RDF from an url (file or http) and stores it in Neo4j as a property graph. "
      + "Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> fetchSHACL(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws InvalidParamException {

    return Stream.of(doImport(format, url, null, props, new GraphConfig(new HashMap<String,Object>())));
  }

  @Procedure(name = "n10s.experimental.validation.shacl.import.inline", mode = Mode.WRITE)
  @Description("Imports an RDF snippet passed as parameter and stores it in Neo4j as a property "
      + "graph. Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> inlineSHACL(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws InvalidParamException {

    return Stream.of(doImport(format, null, rdfFragment, props, new GraphConfig(new HashMap<String,Object>())));
  }

}
