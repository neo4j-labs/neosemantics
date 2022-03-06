package n10s.rdf.stream;

import n10s.graphconfig.RDFParserConfig;
import n10s.result.StreamedStatement;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import java.util.ArrayList;
import java.util.List;

public class StarFormatStatementStreamer extends StatementStreamer{
    public StarFormatStatementStreamer(RDFParserConfig pc) {
        super(pc);
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        if (statements.size() < parserConfig.getStreamTripleLimit()) {
            if(parserConfig.getPredicateExclusionList() == null || !parserConfig
                    .getPredicateExclusionList()
                    .contains(st.getPredicate().stringValue())) {
                StreamedStatement statement;
                Value object = st.getObject();
                if(st.getSubject().isTriple()){
                    List<String> subjectSPO = new ArrayList<>();
                    Triple subjectAsTriple = (Triple)st.getSubject();
                    subjectSPO.add(subjectAsTriple.getSubject().stringValue());
                    subjectSPO.add(subjectAsTriple.getPredicate().stringValue());
                    subjectSPO.add(subjectAsTriple.getObject().stringValue());
                    statement = new StreamedStatement(st.getSubject().stringValue(),
                            st.getPredicate().stringValue(), object.stringValue(),
                            (object instanceof Literal),
                            ((object instanceof Literal) ? ((Literal) object).getDatatype().stringValue() : null),
                            (object instanceof Literal ? ((Literal) object).getLanguage().orElse(null) : null),
                            subjectSPO);
                } else {
                    statement = new StreamedStatement(st.getSubject().stringValue(),
                            st.getPredicate().stringValue(), object.stringValue(),
                            (object instanceof Literal),
                            ((object instanceof Literal) ? ((Literal) object).getDatatype().stringValue() : null),
                            (object instanceof Literal ? ((Literal) object).getLanguage().orElse(null) : null));
                }

                statements.add(statement);
            }
        } else {
            throw new TripleLimitReached(parserConfig.getStreamTripleLimit() + " triples streamed");
        }

    }
}
