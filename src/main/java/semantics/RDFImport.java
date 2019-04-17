package semantics;

import org.eclipse.rdf4j.model.util.URIUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.eclipse.rdf4j.rio.*;
import semantics.result.GraphResult;
import semantics.result.StreamedStatement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.*;
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
    private static final boolean DEFAULT_SHORTEN_URLS = true;
    private static final boolean DEFAULT_TYPES_TO_LABELS = true;
    private static final long DEFAULT_COMMIT_SIZE = 25000;
    private static final long DEFAULT_NODE_CACHE_SIZE = 10000;
    public static final String PREFIX_SEPARATOR = "__";
    static final int URL_SHORTEN = 0;
    static final int URL_IGNORE = 1;
    static final int URL_KEEP = 2;

    static final int RELATIONSHIP = 0;
    static final int LABEL = 1;
    static final int PROPERTY = 2;

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD, RDFFormat.TURTLE,
            RDFFormat.NTRIPLES, RDFFormat.TRIG};



    @Procedure(mode = Mode.WRITE)
    public Stream<ImportResults> importRDF(@Name("url") String url, @Name("format") String format,
                                           @Name("props") Map<String, Object> props) {

        final int handleUrls = (props.containsKey("shortenUrls")?((boolean)props.get("shortenUrls")?0:2):0);
        final int ignoreUrls = (props.containsKey("ignoreUrls")?((boolean)props.get("ignoreUrls")?1:0):0);
        final boolean typesToLabels = (props.containsKey("typesToLabels")?(boolean)props.get("typesToLabels"):DEFAULT_TYPES_TO_LABELS);
        final long commitSize = (props.containsKey("commitSize")?(long)props.get("commitSize"):DEFAULT_COMMIT_SIZE);
        final long nodeCacheSize = (props.containsKey("nodeCacheSize")?(long)props.get("nodeCacheSize"):DEFAULT_NODE_CACHE_SIZE);
        final String languageFilter = (props.containsKey("languageFilter")?(String)props.get("languageFilter"):null);

        ImportResults importResults = new ImportResults();
        URLConnection urlConn;
        DirectStatementLoader statementLoader = new DirectStatementLoader(db, (commitSize > 0 ? commitSize : 5000),
                nodeCacheSize, (handleUrls>ignoreUrls?handleUrls:ignoreUrls), typesToLabels, languageFilter, log);
        try {
            checkIndexesExist();
            urlConn = new URL(url).openConnection();
            if (props.containsKey("headerParams")) {
                ((Map<String, String>) props.get("headerParams")).forEach( (k,v) -> urlConn.setRequestProperty(k,v));
            }
            InputStream inputStream = urlConn.getInputStream();
            RDFParser rdfParser = Rio.createParser(getFormat(format));
            rdfParser.setRDFHandler(statementLoader);
            rdfParser.parse(inputStream, url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
            importResults.setTerminationKO(e.getMessage());
            e.printStackTrace();
        } finally {
            importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
            importResults.setNamespaces(statementLoader.getNamespaces());
        }
        return Stream.of(importResults);
    }

    @Procedure(mode = Mode.READ)
    public Stream<GraphResult> previewRDF(@Name("url") String url, @Name("format") String format,
                                          @Name("props") Map<String, Object> props) {

        final int handleUrls = (props.containsKey("shortenUrls")?((boolean)props.get("shortenUrls")?0:2):0);
        final int ignoreUrls = (props.containsKey("ignoreUrls")?((boolean)props.get("ignoreUrls")?1:0):0);
        final boolean typesToLabels = (props.containsKey("typesToLabels")?(boolean)props.get("typesToLabels"):DEFAULT_TYPES_TO_LABELS);
        final String languageFilter = (props.containsKey("languageFilter")?(String)props.get("languageFilter"):null);

        URLConnection urlConn;
        Map<String,Node> virtualNodes = new HashMap<>();
        List<Relationship> virtualRels = new ArrayList<>();

        StatementPreviewer statementViewer = new StatementPreviewer(db, (handleUrls>ignoreUrls?handleUrls:ignoreUrls), typesToLabels, virtualNodes, virtualRels, languageFilter, log);
        try {
            urlConn = new URL(url).openConnection();
            if (props.containsKey("headerParams")) {
                ((Map<String, String>) props.get("headerParams")).forEach( (k,v) -> urlConn.setRequestProperty(k,v));
            }
            InputStream inputStream = urlConn.getInputStream();
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

    @Procedure(mode = Mode.READ)
    public Stream<StreamedStatement> streamRDF(@Name("url") String url, @Name("format") String format,
                                               @Name("props") Map<String, Object> props) {

        final boolean shortenUrls = (props.containsKey("shortenUrls")?(boolean)props.get("shortenUrls"):DEFAULT_SHORTEN_URLS);
        final boolean typesToLabels = (props.containsKey("typesToLabels")?(boolean)props.get("typesToLabels"):DEFAULT_TYPES_TO_LABELS);
        final String languageFilter = (props.containsKey("languageFilter")?(String)props.get("languageFilter"):null);

        URLConnection urlConn;
        Map<String,Node> virtualNodes = new HashMap<>();
        List<Relationship> virtualRels = new ArrayList<>();

        StatementStreamer statementStreamer = new StatementStreamer();
        try {
            urlConn = new URL(url).openConnection();
            if (props.containsKey("headerParams")) {
                ((Map<String, String>) props.get("headerParams")).forEach( (k,v) -> urlConn.setRequestProperty(k,v));
            }
            InputStream inputStream = urlConn.getInputStream();
            RDFFormat rdfFormat = getFormat(format);
            log.info("Data set to be parsed as " + rdfFormat);
            RDFParser rdfParser = Rio.createParser(rdfFormat);
            rdfParser.setRDFHandler(statementStreamer);
            rdfParser.parse(inputStream, "http://neo4j.com/base/");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
            e.printStackTrace();
        }

        return statementStreamer.getStatements().stream();


    }

    @Procedure(mode = Mode.READ)
    public Stream<GraphResult> previewRDFSnippet(@Name("rdf") String rdfFragment, @Name("format") String format,
                                                 @Name("props") Map<String, Object> props) {

        final int handleUrls = (props.containsKey("shortenUrls")?((boolean)props.get("shortenUrls")?0:2):0);
        final int ignoreUrls = (props.containsKey("ignoreUrls")?((boolean)props.get("ignoreUrls")?1:0):0);
        final boolean typesToLabels = (props.containsKey("typesToLabels")?(boolean)props.get("typesToLabels"):DEFAULT_TYPES_TO_LABELS);
        final String languageFilter = (props.containsKey("languageFilter")?(String)props.get("languageFilter"):null);

        Map<String,Node> virtualNodes = new HashMap<>();
        List<Relationship> virtualRels = new ArrayList<>();

        StatementPreviewer statementViewer = new StatementPreviewer(db, (handleUrls>ignoreUrls?handleUrls:ignoreUrls), typesToLabels, virtualNodes, virtualRels, languageFilter, log);
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

    @UserFunction
    public String getIRILocalName(@Name("url") String url) {
        return url.substring(URIUtil.getLocalNameIndex(url));
    }

    @UserFunction
    public String getIRINamespace(@Name("url") String url) {
        return url.substring(0,URIUtil.getLocalNameIndex(url));
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
