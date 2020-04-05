package n10s;

import n10s.graphconfig.RDFParserConfig;
import org.eclipse.rdf4j.rio.RDFHandler;

public abstract class ConfiguredStatementHandler implements RDFHandler {

  public abstract RDFParserConfig getParserConfig();

}
