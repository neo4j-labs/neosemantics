package n10s;

import org.eclipse.rdf4j.rio.RDFHandler;
import n10s.graphconfig.RDFParserConfig;

public abstract class ConfiguredStatementHandler implements RDFHandler {

  public abstract RDFParserConfig getParserConfig();

}
