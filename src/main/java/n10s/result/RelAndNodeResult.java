package n10s.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class RelAndNodeResult {


  public final Relationship rel;
  public final Node node;

  public RelAndNodeResult(Relationship rel, Node node) {
    this.rel = rel;
    this.node = node;
  }

}
