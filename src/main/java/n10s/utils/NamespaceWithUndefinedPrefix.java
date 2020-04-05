package n10s.utils;

import org.eclipse.rdf4j.rio.RDFHandlerException;

public class NamespaceWithUndefinedPrefix extends RDFHandlerException {

  public NamespaceWithUndefinedPrefix(String message) {
    super(message);
  }
}
