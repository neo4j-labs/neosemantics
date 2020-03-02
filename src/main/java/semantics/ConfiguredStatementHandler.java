package semantics;

import org.eclipse.rdf4j.rio.RDFHandler;
import semantics.config.RDFParserConfig;

public abstract class ConfiguredStatementHandler implements RDFHandler {

  abstract RDFParserConfig getParserConfig();

}
