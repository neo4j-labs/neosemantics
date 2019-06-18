package semantics;

import org.eclipse.rdf4j.rio.RDFHandler;

public abstract class ConfiguredStatementHandler implements RDFHandler {

  abstract RDFParserConfig getParserConfig();

}
