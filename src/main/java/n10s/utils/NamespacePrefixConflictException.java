package n10s.utils;

import org.eclipse.rdf4j.rio.RDFHandlerException;

public class NamespacePrefixConflictException extends RDFHandlerException {

  public NamespacePrefixConflictException(String message) {
    super(message);
  }
}
