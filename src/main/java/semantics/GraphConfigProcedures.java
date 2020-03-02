package semantics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import semantics.config.GraphConfig;
import semantics.result.NodeResult;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class GraphConfigProcedures {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  @Procedure(mode = Mode.WRITE)
  @Description("Defines the config that drives the behavior of the graph")
  public Stream<NodeResult> setGraphConfig(@Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws GraphConfigException{

    //identify config changes that are acceptable (additive)
    if(graphIsEmpty()) {
      try {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("props", new GraphConfig(props).serialiseConfig());
        return tx.execute("MERGE (gc:_GraphConfig {_id: 1 }) " +
                " SET gc+= $props RETURN gc ", queryParams).stream().map(n -> (Node) n.get("gc")).map(NodeResult::new);
      } catch (GraphConfig.InvalidParamException e) {
        throw new GraphConfigException("Invalid Config: " + e.getMessage());
      }
    } else{
      throw new GraphConfigException("The graph is non-empty. Config cannot be changed.");
    }
  }

  private boolean graphIsEmpty() {
    return !tx.execute("match (r:Resource) return id(r) limit 1").hasNext();
  }

  private class GraphConfigException extends Throwable {
    public GraphConfigException(String msg) {
      super(msg);
    }
  }
}
