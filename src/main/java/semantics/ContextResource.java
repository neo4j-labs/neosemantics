package semantics;

import java.util.Objects;

/**
 * ContextResource represents a Resource with an optional graph (context) uri.
 * <p>
 * It is used as Key for the Maps containing labels and properties.
 *
 * Created on 06/06/2019
 *
 * @author Emre Arkan
 * @see RDFQuadDirectStatementLoader
 * @see RDFQuadDirectStatementDeleter
 */
public class ContextResource {

  private final String uri;
  private final String graphUri;

  public ContextResource(String uri, String graphUri) {
    this.uri = uri;
    this.graphUri = graphUri;
  }

  /**
   * @return uri the URI of the current instance
   */
  public String getUri() {
    return uri;
  }

  /**
   * @return graphUri the graph URI of the current instance if given, {@code null} otherwise
   */
  public String getGraphUri() {
    return graphUri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContextResource that = (ContextResource) o;
    return Objects.equals(getUri(), that.getUri()) &&
        Objects.equals(getGraphUri(), that.getGraphUri());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getUri(), getGraphUri());
  }

}