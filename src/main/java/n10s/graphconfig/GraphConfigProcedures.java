package n10s.graphconfig;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import n10s.graphconfig.GraphConfig.InvalidParamException;
import n10s.result.GraphConfigItemResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class GraphConfigProcedures {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  @Procedure(mode = Mode.WRITE)
  @Description("Initialises the config that drives the behavior of the graph")
  public Stream<GraphConfigItemResult> init(
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws GraphConfigException {
    //create or overwite
    if (graphIsEmpty()) {
      try {
        GraphConfig currentGraphConfig = new GraphConfig(props);
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("props", currentGraphConfig.serialiseConfig());
        tx.execute("MERGE (gc:_GraphConfig) SET gc+= $props", queryParams);
        return currentGraphConfig.getAsGraphConfigResults().stream();
      } catch (InvalidParamException ipe) {
        throw new GraphConfigException(ipe.getMessage());
      }
    } else {
      throw new GraphConfigException("The graph is non-empty. Config cannot be changed.");
    }
  }

  @Procedure(mode = Mode.WRITE)
  @Description("sets specific params to the config that drives the behavior of the graph")
  public Stream<GraphConfigItemResult> set(
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws GraphConfigException {
    //update
    //TODO: identify config changes that are acceptable (additive) when graph is not empty?
    if (graphIsEmpty()) {
      GraphConfig currentGraphConfig;
      try {
        try {
          currentGraphConfig = new GraphConfig(tx);
          currentGraphConfig.add(props);
        } catch (GraphConfigNotFound e) {
          throw new GraphConfigException("Graph config not found. Call 'init' method first.");
        }
      } catch (InvalidParamException ipe) {
        throw new GraphConfigException(ipe.getMessage());
      }
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put("props", currentGraphConfig.serialiseConfig());
      tx.execute("MERGE (gc:_GraphConfig) SET gc+= $props", queryParams);
      return currentGraphConfig.getAsGraphConfigResults().stream();
    } else {
      throw new GraphConfigException("The graph is non-empty. Config cannot be changed.");
    }
  }

  @Procedure(mode = Mode.READ)
  @Description("Shows the current graph config")
  public Stream<GraphConfigItemResult> show() throws GraphConfigException {
    try {
      return new GraphConfig(tx).getAsGraphConfigResults().stream();
    } catch (GraphConfigNotFound e) {
      return Stream.empty();
    }
  }

  @Procedure(mode = Mode.WRITE)
  @Description("removes the current graph config")
  public Stream<GraphConfigItemResult> drop() throws GraphConfigException {
    if (!graphIsEmpty()) {
      throw new GraphConfigException("The graph is non-empty. Config cannot be changed.");
    }

    ResourceIterator<Node> graphConfigs = tx
        .findNodes(Label.label("_GraphConfig"));

    if (graphConfigs.hasNext()) {
      graphConfigs.next().delete();
    }

    return Stream.empty();

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
