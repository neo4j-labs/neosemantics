package semantics;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleIRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.neo4j.graphdb.*;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.sun.tools.doclint.Entity.lang;

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
    static final int URL_MAP = 2;
    static final int URL_KEEP = 3;
    static final int PROP_OVERWRITE = 0;
    static final int PROP_ARRAY = 1;
    static final int PROP_REIFY = 2;

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

        final int handleVocabUris = (props.containsKey("handleVocabUris")?getHandleVocabUrisAsInt((String)props.get("handleVocabUris")):0);
        final int handleMultival = (props.containsKey("handleMultival")?getHandleMultivalAsInt((String)props.get("handleMultival")):0);
        final List<String> multivalPropList = (props.containsKey("multivalPropList")?(List<String>)props.get("multivalPropList"):null);
        final List<String> predicateExclusionList = (props.containsKey("predicateExclusionList")?(List<String>)props.get("predicateExclusionList"):null);
        final boolean typesToLabels = (props.containsKey("typesToLabels")?(boolean)props.get("typesToLabels"):DEFAULT_TYPES_TO_LABELS);
        final boolean keepLangTag = (props.containsKey("keepLangTag")?(boolean)props.get("keepLangTag"):false);
        final long commitSize = (props.containsKey("commitSize")?(long)props.get("commitSize"):DEFAULT_COMMIT_SIZE);
        final long nodeCacheSize = (props.containsKey("nodeCacheSize")?(long)props.get("nodeCacheSize"):DEFAULT_NODE_CACHE_SIZE);
        final String languageFilter = (props.containsKey("languageFilter")?(String)props.get("languageFilter"):null);

        ImportResults importResults = new ImportResults();
        URLConnection urlConn;
        DirectStatementLoader statementLoader = new DirectStatementLoader(db, (commitSize > 0 ? commitSize : 5000),
                nodeCacheSize, handleVocabUris, handleMultival, (multivalPropList==null?null:new HashSet<String>(multivalPropList)),
                (predicateExclusionList==null?null:new HashSet<String>(predicateExclusionList)), typesToLabels, keepLangTag,
                languageFilter, log);
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

    private int getHandleVocabUrisAsInt(String handleVocUrisAsText) {
        if (handleVocUrisAsText.equals("SHORTEN")){
            return 0;
        } else if(handleVocUrisAsText.equals("IGNORE")){
            return 1;
        } else if (handleVocUrisAsText.equals("MAP")){
            return 2;
        } else { //KEEP
            return 3;
        }
    }

    private int getHandleMultivalAsInt(String ignoreUrlsAsText) {
        if (ignoreUrlsAsText.equals("OVERWRITE")){
            return 0;
        } else if(ignoreUrlsAsText.equals("ARRAY")){
            return 1;
        } else if (ignoreUrlsAsText.equals("REIFY")){
            return 2;
        } else { //HYBRID
            return 3;
        }
    }

    @Procedure(mode = Mode.READ)
    public Stream<GraphResult> previewRDF(@Name("url") String url, @Name("format") String format,
                                          @Name("props") Map<String, Object> props) {

        final int handleVocabUris = (props.containsKey("handleVocabUris")?getHandleVocabUrisAsInt((String)props.get("handleVocabUris")):0);
        final int handleMultival = (props.containsKey("handleMultival")?getHandleMultivalAsInt((String)props.get("handleMultival")):0);
        final List<String> multivalPropList = (props.containsKey("multivalPropList")?(List<String>)props.get("multivalPropList"):null);
        final List<String> predicateExclusionList = (props.containsKey("predicateExclusionList")?(List<String>)props.get("predicateExclusionList"):null);
        final boolean typesToLabels = (props.containsKey("typesToLabels")?(boolean)props.get("typesToLabels"):DEFAULT_TYPES_TO_LABELS);
        final boolean keepLangTag = (props.containsKey("keepLangTag")?(boolean)props.get("keepLangTag"):false);
        final String languageFilter = (props.containsKey("languageFilter")?(String)props.get("languageFilter"):null);

        URLConnection urlConn;
        Map<String,Node> virtualNodes = new HashMap<>();
        List<Relationship> virtualRels = new ArrayList<>();

        StatementPreviewer statementViewer = new StatementPreviewer(db, handleVocabUris, handleMultival,
                (multivalPropList==null?null:new HashSet<String>(multivalPropList)),
                (predicateExclusionList==null?null:new HashSet<String>(predicateExclusionList)),typesToLabels, virtualNodes,
                virtualRels, keepLangTag, languageFilter, log);
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

        URLConnection urlConn;

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

        final int handleVocabUris = (props.containsKey("handleVocabUris")?getHandleVocabUrisAsInt((String)props.get("handleVocabUris")):0);
        final int handleMultival = (props.containsKey("handleMultival")?getHandleMultivalAsInt((String)props.get("handleMultival")):0);
        final List<String> multivalPropList = (props.containsKey("multivalPropList")?(List<String>)props.get("multivalPropList"):null);
        final List<String> predicateExclusionList = (props.containsKey("predicateExclusionList")?(List<String>)props.get("predicateExclusionList"):null);
        final boolean typesToLabels = (props.containsKey("typesToLabels")?(boolean)props.get("typesToLabels"):DEFAULT_TYPES_TO_LABELS);
        final boolean keepLangTag = (props.containsKey("keepLangTag")?(boolean)props.get("keepLangTag"):false);
        final String languageFilter = (props.containsKey("languageFilter")?(String)props.get("languageFilter"):null);

        Map<String,Node> virtualNodes = new HashMap<>();
        List<Relationship> virtualRels = new ArrayList<>();

        StatementPreviewer statementViewer = new StatementPreviewer(db, handleVocabUris, handleMultival,
                (multivalPropList==null?null:new HashSet<String>(multivalPropList)),
                (predicateExclusionList==null?null:new HashSet<String>(predicateExclusionList)),
                typesToLabels, virtualNodes, virtualRels, keepLangTag, languageFilter, log);
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

    @UserFunction
    public String getLangValue(@Name("lang") String lang, @Name("values") Object values) {
        Pattern p = Pattern.compile("^(.*)@([a-z,\\-]+)$");
        if (values instanceof List) {
            if (((List)values).get(0) instanceof String) {
                for (Object val : (List<String>) values) {
                    Matcher m = p.matcher((String) val);
                    if (m.matches() && m.group(2).equals(lang)) {
                        return m.group(1);
                    }
                }
            }
        } else if (values instanceof String[]) {
            String[] valuesAsArray = (String[])values;
            for (int i=0;i<valuesAsArray.length;i++) {
                Matcher m = p.matcher(valuesAsArray[i]);
                if (m.matches() && m.group(2).equals(lang)) {
                    return m.group(1);
                }
            }
        } else if (values instanceof String){
            Matcher m = p.matcher((String)values);
            if (m.matches() && m.group(2).equals(lang)) {
                return m.group(1);
            }
        }
        return null;
    }

    @UserFunction
    public String uriFromShort(@Name("short") String str) {
        Pattern p = Pattern.compile("^(\\w+)__(\\w+)$");
        Matcher m = p.matcher(str);
        if (m.matches()) {
            ResourceIterator<Node> nspd = db.findNodes(Label.label("NamespacePrefixDefinition"));
            if (nspd.hasNext()){
                Map<String, Object> namespaces = nspd.next().getAllProperties();
                Iterator<Map.Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
                while(nsIterator.hasNext()){
                    Map.Entry<String, Object> kv = nsIterator.next();
                    if(m.group(1).equals(kv.getValue())){
                        return kv.getKey() + m.group(2);
                    }
                }
            }
        }
        //default return original value
        return str;
    }

    @UserFunction
    public String shortFromUri(@Name("uri") String str) {
        try{
            IRI iri = SimpleValueFactory.getInstance().createIRI(str);
            ResourceIterator<Node> nspd = db.findNodes(Label.label("NamespacePrefixDefinition"));
            if (nspd.hasNext()){
                Map<String, Object> namespaces = nspd.next().getAllProperties();
                Iterator<Map.Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
                while(nsIterator.hasNext()) {
                    Map.Entry<String, Object> kv = nsIterator.next();
                    if (kv.getKey().equals(iri.getNamespace())){
                        return kv.getValue() + PREFIX_SEPARATOR + iri.getLocalName();
                    }
                }
            }
            return str;
        }catch (Exception e){
            return str;
        }
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
