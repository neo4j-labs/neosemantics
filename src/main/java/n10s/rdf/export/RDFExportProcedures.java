package n10s.rdf.export;

import static n10s.mapping.MappingUtils.getExportMappingsFromDB;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import n10s.rdf.RDFProcedures;
import n10s.result.StreamedStatement;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import org.eclipse.rdf4j.model.Literal;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class RDFExportProcedures extends RDFProcedures {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;


  @Procedure(mode = Mode.READ)
  @Description(
      "[Experimental] Executes a cypher query returning graph elements (nodes,rels) and serialises "
          + "the output as triples.")
  public Stream<StreamedStatement> cypher(@Name("cypher") String cypher,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws InvalidNamespacePrefixDefinitionInDB {

    ExportProcessor proc;

    if (getGraphConfig(tx) == null) {
      proc = new LPGToRDFProcesssor(db, tx,
          getExportMappingsFromDB(db), props.containsKey("mappedElemsOnly") &&
          props.get("mappedElemsOnly").equals(true));
    } else {
      proc = new LPGRDFToRDFProcesssor(db, tx);
    }
    return proc.streamTriplesFromCypher(cypher,
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


  private GraphConfig getGraphConfig(Transaction tx) {
    GraphConfig result = null;
    try {
      result = new GraphConfig(tx);
    } catch (GraphConfigNotFound graphConfigNotFound) {
      //it's an LPG (no RDF import config)
    }
    return result;
  }

}

