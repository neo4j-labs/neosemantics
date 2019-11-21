package semantics;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import semantics.result.StreamedStatement;

public class StatementStreamer extends ConfiguredStatementHandler {

  private final RDFParserConfig parserConfig;
  private List<StreamedStatement> statements;

  StatementStreamer(
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
    Value object = st.getObject();
    StreamedStatement statement = new StreamedStatement(st.getSubject().stringValue(),
        st.getPredicate().stringValue(), object.stringValue(),
        (object instanceof Literal),
        ((object instanceof Literal) ? ((Literal) object).getDatatype().stringValue() : null),
        (object instanceof Literal ? ((Literal) object).getLanguage().orElse(null) : null));
    statements.add(statement);

  }

  @Override
  public void handleComment(String s) throws RDFHandlerException {

  }


  public List<StreamedStatement> getStatements() {
    return statements;
  }

  @Override
  RDFParserConfig getParserConfig() {
    return parserConfig;
  }

  public void setErrorMsg(String errorMsg) {
    if (this.statements == null) {
      this.statements = new ArrayList<>();
    } else {
      this.statements.clear();
    }
    this.statements
        .add(new StreamedStatement("neo4j://error", "neo4j://message", errorMsg, true, null, null));
  }
}
