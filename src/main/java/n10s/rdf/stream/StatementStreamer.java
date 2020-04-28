package n10s.rdf.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import n10s.ConfiguredStatementHandler;
import n10s.graphconfig.RDFParserConfig;
import n10s.result.StreamedStatement;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;

public class StatementStreamer extends ConfiguredStatementHandler {

  private final RDFParserConfig parserConfig;
  private List<StreamedStatement> statements;

  public StatementStreamer(
      RDFParserConfig pc) {
    parserConfig = pc;

  }

  @Override
  public void startRDF() throws RDFHandlerException {
    statements = new ArrayList<>();
  }

  @Override
  public void endRDF() throws RDFHandlerException {

  }

  @Override
  public void handleNamespace(String s, String s1) throws RDFHandlerException {

  }

  @Override
  public void handleStatement(Statement st) throws RDFHandlerException {
    if(statements.size()< parserConfig.getStreamTripleLimit()) {
      Value object = st.getObject();
      StreamedStatement statement = new StreamedStatement(st.getSubject().stringValue(),
          st.getPredicate().stringValue(), object.stringValue(),
          (object instanceof Literal),
          ((object instanceof Literal) ? ((Literal) object).getDatatype().stringValue() : null),
          (object instanceof Literal ? ((Literal) object).getLanguage().orElse(null) : null));
      statements.add(statement);
    }  else {
      throw new StreamerLimitReached(parserConfig.getStreamTripleLimit() + " triples parsed");
    }

  }

  @Override
  public void handleComment(String s) throws RDFHandlerException {

  }


  public List<StreamedStatement> getStatements() {
    return statements;
  }

  @Override
  public RDFParserConfig getParserConfig() {
    return parserConfig;
  }

  public class StreamerLimitReached extends RDFParseException {

    public StreamerLimitReached(String s) {
      super(s);
    }
  }
}
