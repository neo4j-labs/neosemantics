package semantics;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import semantics.result.StreamedStatement;

import java.util.ArrayList;
import java.util.List;

public class StatementStreamer implements RDFHandler {

    private List<StreamedStatement> statements;

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
        StreamedStatement statement = new StreamedStatement(st.getSubject().stringValue(), st.getPredicate().stringValue(), object.stringValue(),
                (object instanceof Literal), ((object instanceof Literal)? ((Literal) object).getDatatype().stringValue(): null),
                (object instanceof Literal ? ((Literal) object).getLanguage().orElse(null) : null));
        statements.add(statement);

    }

    @Override
    public void handleComment(String s) throws RDFHandlerException {

    }


    public List<StreamedStatement> getStatements() {
        return statements;
    }
}
