package n10s.result;
import org.neo4j.graphdb.Path;

/**
 * (taken from APOC)
 *
 * @author mh
 * @since 26.02.16
 */
public class PathResult {

  public Path path;

  public PathResult(Path path) {
    this.path = path;
  }
}