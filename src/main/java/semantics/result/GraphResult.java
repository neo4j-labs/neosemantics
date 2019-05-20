package semantics.result;

import java.util.List;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * (taken from APOC)
 *
 * @author mh
 * @since 26.02.16
 */
public class GraphResult {

  public final List<Node> nodes;
  public final List<Relationship> relationships;

  public GraphResult(List<Node> nodes, List<Relationship> relationships) {
    this.nodes = nodes;
    this.relationships = relationships;
  }
}
