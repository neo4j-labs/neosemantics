package semantics;

import apoc.result.GraphResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import org.openrdf.model.*;
import org.openrdf.model.Resource;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * Created by jbarrasa on 21/03/2016.
 * <p>
 * RDF importer based on:
 * 1. DatatypeProperties become node attributes
 * 2. rdf:type relationships are transformed into labels on the subject node
 * 3. rdf:type relationships generate :Class nodes on the object
 */
public class RDFImport {
    @Context
    public GraphDatabaseAPI db;
    @Context
    public Log log;

    public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD, RDFFormat.TURTLE,
            RDFFormat.NTRIPLES, RDFFormat.TRIG};



    @Procedure
    @PerformsWrites
    public Stream<ImportResults> importRDF(@Name("url") String url, @Name("format") String format,
                                                         @Name("shorten") boolean shortenUrls,
                                                         @Name("typesToLabels") boolean typesToLabels,
                                                         @Name("commitSize") long commitSize) {

        ImportResults importResults = new ImportResults();
        URL documentUrl;
        DirectStatementLoader statementLoader = new DirectStatementLoader(db, (commitSize > 0 ? commitSize : 5000), shortenUrls, typesToLabels, log);
        try {
            checkIndexesExist();
            documentUrl = new URL(url);
            InputStream inputStream = documentUrl.openStream();
            RDFParser rdfParser = Rio.createParser(getFormat(format));
            rdfParser.setRDFHandler(statementLoader);
            rdfParser.parse(inputStream, documentUrl.toString());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
            importResults.setTerminationKO(e.getMessage());
        } finally {
            importResults.setTriplesLoaded(statementLoader.getIngestedTriples());
            importResults.setNamespaces(statementLoader.getNamespaces());
        }
        return Stream.of(importResults);
    }

    @Procedure
    public Stream<GraphResult> previewRDF(@Name("url") String url, @Name("format") String format,
                                          @Name("shorten") boolean shortenUrls,
                                          @Name("typesToLabels") boolean typesToLabels) {
        URL documentUrl;
        Map<String,Node> virtualNodes = new HashMap<>();
        List<Relationship> virtualRels = new ArrayList<>();

        StatementPreviewer statementViewer = new StatementPreviewer(db, shortenUrls, typesToLabels, virtualNodes, virtualRels, log);
        try {
            documentUrl = new URL(url);
            InputStream inputStream = documentUrl.openStream();
            RDFFormat rdfFormat = getFormat(format);
            log.info("Data set to be parsed as " + rdfFormat);
            RDFParser rdfParser = Rio.createParser(rdfFormat);
            rdfParser.setRDFHandler(statementViewer);
            rdfParser.parse(inputStream, "http://neo4j.com/base/");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
            e.printStackTrace();
        }

        GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
        return Stream.of(graphResult);


    }

    @Procedure
    public Stream<GraphResult> previewRDFSnippet(@Name("rdf") String rdfFragment, @Name("format") String format,
                                                 @Name("shorten") boolean shortenUrls,
                                                 @Name("typesToLabels") boolean typesToLabels) {
        Map<String,Node> virtualNodes = new HashMap<>();
        List<Relationship> virtualRels = new ArrayList<>();

        StatementPreviewer statementViewer = new StatementPreviewer(db, shortenUrls, typesToLabels, virtualNodes, virtualRels, log);
        try {
            InputStream inputStream = new ByteArrayInputStream( rdfFragment.getBytes(Charset.defaultCharset()) ); //rdfFragment.openStream();
            RDFFormat rdfFormat = getFormat(format);
            log.info("Data set to be parsed as " + rdfFormat);
            RDFParser rdfParser = Rio.createParser(rdfFormat);
            rdfParser.setRDFHandler(statementViewer);
            rdfParser.parse(inputStream, "http://neo4j.com/base/");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
            e.printStackTrace();
        }

        GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
        return Stream.of(graphResult);


    }
    private void checkIndexesExist() throws RDFImportPreRequisitesNotMet {
        Iterable<IndexDefinition> indexes = db.schema().getIndexes();
        if(missing(indexes.iterator(),"Resource")){
            throw new RDFImportPreRequisitesNotMet("The required index on :Resource(uri) could not be found");
        }
    }

    private boolean missing(Iterator<IndexDefinition> iterator, String indexLabel) {
        while (iterator.hasNext()){
            IndexDefinition indexDef = iterator.next();
            if(indexDef.getLabel().name().equals(indexLabel) &&
                    indexDef.getPropertyKeys().iterator().next().equals("uri")) {
                return false;
            }
        }
        return true;
    }

    private RDFFormat getFormat(String format) throws RDFImportPreRequisitesNotMet {
        if (format != null) {
            for (RDFFormat parser : availableParsers) {
                if (parser.getName().equals(format))
                    return parser;
            }
        }
        throw new RDFImportPreRequisitesNotMet("Unrecognized serialization format: " + format);
    }




    public static class ImportResults {
        public String terminationStatus = "OK";
        public long triplesLoaded = 0;
        public Map<String,String> namespaces;
        public String extraInfo = "";

        public void setTriplesLoaded(long triplesLoaded) {
            this.triplesLoaded = triplesLoaded;
        }

        public void setNamespaces(Map<String, String> namespaces) {
            this.namespaces = namespaces;
        }

        public void setTerminationKO(String message) {
            this.terminationStatus = "KO";
            this.extraInfo = message;
        }

    }

    private class RDFImportPreRequisitesNotMet extends Exception {
        String message;

        public RDFImportPreRequisitesNotMet(String s) {
            message = s;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }
}
