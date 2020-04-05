package n10s;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

public class ModelTestUtils {

  public static boolean compareModels(String expected, RDFFormat formatExpected, String actual,
      RDFFormat formatActual) throws IOException {

    Model expectedModel = createModel(expected, formatExpected);
    Model actualModel = createModel(actual, formatActual);

    return Models.isomorphic(expectedModel, actualModel);
  }

  private static Model createModel(String expected, RDFFormat formatExpected) throws IOException {
    RDFParser rdfParser = Rio.createParser(formatExpected);
    rdfParser.getParserConfig().set(BasicParserSettings.NORMALIZE_DATATYPE_VALUES, true);
    Model model = new LinkedHashModel();
    rdfParser.setRDFHandler(new StatementCollector(model));
    rdfParser.parse(new ByteArrayInputStream(expected.getBytes()), "");
    return model;
  }
}
