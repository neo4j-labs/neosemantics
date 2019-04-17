package semantics;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ModelTestUtils {

    public static boolean comparemodels(String expected, RDFFormat formatExpected, String actual, RDFFormat formatActual) throws IOException {

        Model expectedModel = createModel(expected, formatExpected);
        Model actualModel = createModel(actual, formatActual);

        return included(expectedModel, actualModel) && included(actualModel,expectedModel);
    }

    private static Boolean included(Model expectedModel, Model actualModel) {
        return actualModel.filter(null, null, null).stream().map(x -> expectedModel.contains(x.getSubject(), x.getPredicate(), x.getObject())).reduce(true, (a, b) -> a && b);
    }

    private static Model createModel(String expected, RDFFormat formatExpected) throws IOException {
        RDFParser rdfParser = Rio.createParser(formatExpected);
        Model model = new LinkedHashModel();
        rdfParser.setRDFHandler(new StatementCollector(model));
        rdfParser.parse(new ByteArrayInputStream(expected.getBytes()), "");
        return model;
    }
}
