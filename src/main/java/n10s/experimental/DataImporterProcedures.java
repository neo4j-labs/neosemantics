package n10s.experimental;

import n10s.CommonProcedures;
import n10s.experimental.dimodel.DIModelBuilder;
import n10s.experimental.dimodel.DIModelSummary;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.UriUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class DataImporterProcedures extends CommonProcedures {


    @Procedure(name = "n10s.experimental.stream.dimodel.inline", mode = Mode.READ)
    @Description("Generates a data importer tool model aligned with the selected ontology")
    public Stream<DIModelSummary> streamDIModel(@Name("rdf") String rdfFragment,
                                          @Name("format") String format,
                                          @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
            throws IOException, CommonProcedures.RDFImportBadParams, InvalidNamespacePrefixDefinitionInDB, UriUtils.UriNamespaceHasNoAssociatedPrefix {

        return doBuild(format, null, rdfFragment, props, false);
    }

    @Procedure(name = "n10s.experimental.stream.dimodel.fetch", mode = Mode.READ)
    @Description("Generates a data importer tool model aligned with the selected ontology")
    public Stream<DIModelSummary> streamDIModelFromURL(@Name("url") String url,
                                                         @Name("format") String format,
                                                         @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
            throws IOException, CommonProcedures.RDFImportBadParams {

        return doBuild(format, url, null, props, false);

    }

    @Procedure(name = "n10s.experimental.export.dimodel.inline", mode = Mode.READ)
    @Description("Generates a data importer tool model aligned with the selected ontology")
    public Stream<DIModelSummary> exportDIModel(@Name("rdf") String rdfFragment,
                                                  @Name("format") String format,
                                                  @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
            throws IOException, CommonProcedures.RDFImportBadParams, InvalidNamespacePrefixDefinitionInDB, UriUtils.UriNamespaceHasNoAssociatedPrefix {

        return doBuild(format, null, rdfFragment, props, true);
    }

    @Procedure(name = "n10s.experimental.export.dimodel.fetch", mode = Mode.READ)
    @Description("Generates a data importer tool model aligned with the selected ontology")
    public Stream<DIModelSummary> exportDIModelFromURL(@Name("url") String url,
                                                         @Name("format") String format,
                                                         @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
            throws IOException, CommonProcedures.RDFImportBadParams {

        return doBuild(format, url, null, props, true);

    }

    private Stream<DIModelSummary> doBuild(String format, String url, String rdfFragment,
                                   Map<String, Object> props, boolean writeToFile)
            throws IOException, RDFImportBadParams {

        InputStream is;
        if (rdfFragment != null) {
            is = new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset()));
        } else {
            is = getInputStream(url, props);
        }

        DIModelBuilder dimb = new DIModelBuilder(tx, log);
        dimb.buildDIModel(is, getFormat(format), props);
        if(writeToFile){
            return dimb.exportDIModelToFile(dimb.getModelAsSerialisableObject(),dimb.getModelMappings());
        } else {
            return dimb.exportDIModelAsString(dimb.getModelAsSerialisableObject(),dimb.getModelMappings());
        }

    }

}
