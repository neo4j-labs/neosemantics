package n10s.rdf.export;

import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Statement;

public abstract class ExportProcessor {

  public abstract Stream<Statement> streamTriplesFromCypher(String cypher, Map<String, Object> params);


}
