package n10s;

import n10s.graphconfig.RDFParserConfig;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFParseException;

public abstract class ConfiguredStatementHandler implements RDFHandler {

    public abstract RDFParserConfig getParserConfig();

    public class TripleLimitReached extends RDFParseException {

    public TripleLimitReached(String s) {
      super(s);
    }
  }

}
