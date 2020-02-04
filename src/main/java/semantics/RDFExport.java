package semantics;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Literal;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import semantics.result.StreamedStatement;

public class RDFExport {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  @Procedure(mode = Mode.READ)
  @Description("[Experimental] Executes a cypher query returning graph elements (nodes,rels) and serialises "
      + "the output as triples.")
  public Stream<StreamedStatement> cypherResultsAsRDF(@Name("cypher") String cypher,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    LPGToRDFProcesssor processor = new LPGToRDFProcesssor(db, tx);
    return processor.streamTriplesFromCypher(cypher,
        (props.containsKey("cypherParams") ? (Map<String, Object>) props.get("cypherParams") :
            new HashMap<>())).map(st -> new StreamedStatement(
        st.getSubject().stringValue(), st.getPredicate().stringValue(),
        st.getObject().stringValue(), st.getObject() instanceof Literal,
        (st.getObject() instanceof Literal ? ((Literal) st.getObject()).getDatatype().stringValue()
            : null),
        (st.getObject() instanceof Literal ? ((Literal) st.getObject()).getLanguage().orElse(null)
            : null)
    ));


  }

}
