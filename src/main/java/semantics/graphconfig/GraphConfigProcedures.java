package semantics.graphconfig;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import semantics.graphconfig.GraphConfig.GraphConfigNotFound;
import semantics.result.GraphConfigItemResult;

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
  @Description("Sets the config that drives the behavior of the graph")
  public Stream<GraphConfigItemResult> set(@Name(value = "params", defaultValue = "{}") Map<String, Object> props) throws GraphConfigException{

    //TODO: identify config changes that are acceptable (additive)?
    if(graphIsEmpty()) {
      try {
        Map<String, Object> queryParams = new HashMap<>();
        GraphConfig newGraphConfig = new GraphConfig(props);
        queryParams.put("props", newGraphConfig.serialiseConfig());
        tx.execute("MERGE (gc:_GraphConfig) SET gc+= $props", queryParams);
        return  newGraphConfig.getAsGraphConfigResults().stream();
      } catch (GraphConfig.InvalidParamException e) {
        throw new GraphConfigException("Invalid Config: " + e.getMessage());
      }
    } else{
      throw new GraphConfigException("The graph is non-empty. Config cannot be changed.");
    }
  }

  @Procedure(mode = Mode.READ)
  @Description("Shows the current graph config")
  public Stream<GraphConfigItemResult> show() throws GraphConfigException{
    try {
      return new GraphConfig(tx).getAsGraphConfigResults().stream();
    } catch (GraphConfigNotFound e) {
      return Stream.empty();
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
