package semantics;

import org.eclipse.rdf4j.rio.RDFHandler;
import semantics.graphconfig.RDFParserConfig;

public abstract class ConfiguredStatementHandler implements RDFHandler {

  abstract RDFParserConfig getParserConfig();

}
