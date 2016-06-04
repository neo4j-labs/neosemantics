package semantics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import org.openrdf.model.*;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * Created by jbarrasa on 21/03/2016.
 *
 * RDF importer based on:
 * 1. DatatypeProperties become node attributes
 * 2. rdf:type relationships are transformed into labels on the subject node
 * 3. rdf:type relationships generate :Class nodes on the object
 *
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
    public Stream<ImportResults> importRDF(@Name("url") String url, @Name("format") String format) {
        ImportResults importResults = new ImportResults();
        URL documentUrl;

        StatementLoader statementLoader = new StatementLoader(db, 25000);
        try {
            documentUrl = new URL(url);
            InputStream inputStream = documentUrl.openStream();
            RDFParser rdfParser = Rio.createParser(getFormat(format));
            rdfParser.setRDFHandler(statementLoader);
            rdfParser.parse(inputStream, documentUrl.toString());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
            importResults.setTerminationKO(e.getMessage());
        } finally {
            importResults.setTriplesLoaded(statementLoader.getIngestedTriples());
        }
        return Stream.of(importResults);
    }

    private RDFFormat getFormat(String format) {
        if (format != null) {
            for (RDFFormat parser : availableParsers) {
                if (parser.getName().equals(format))
                    return parser;
            }
        }
        return RDFFormat.TURTLE; //some default
    }

    class StatementLoader extends RDFHandlerBase implements Callable<Integer>{

        private int ingestedTriples = 0;
        private GraphDatabaseService graphdb;
        private int commitFreq;
        private List<QueryWithParams> cypherStatements = new ArrayList<QueryWithParams>();
        //private List<Map<String, Object>> cypherStatementParams = new ArrayList<Map<String, Object>>();

        public StatementLoader(GraphDatabaseService db, int batchSize) {
            graphdb = db;
            commitFreq = batchSize;
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            int finalCount = Utils.inTx(graphdb,this);
            ingestedTriples+= finalCount;
            log.info("Successfully committed " + finalCount + " cypher statements. Total number of triples imported is " + ingestedTriples);
        }

        @Override
        public void handleStatement(Statement st) {
            URI predicate = st.getPredicate();
            Resource subject = st.getSubject(); //includes blank nodes
            Value object = st.getObject();
            String cypher;
            if (object instanceof Literal) {
                // known issue: does not deal with multivalued properties. Probably the obvious approach would be
                // to create arrays when multiple values
                Map<String, Object> params = new HashMap<>();
                params.put("subject", subject.stringValue().replace("'", "\'"));
                params.put("object", getObjectValue((Literal) object));
                cypherStatements.add(new QueryWithParams(String.format("MERGE (x:Resource:%s {uri:{subject}}) SET x.`%s` = {object}",
                        (subject instanceof BNode ? "BNode" : "URI"), predicate.stringValue()), params));
            } else if (predicate.equals(RDF.TYPE) && !(object instanceof BNode)){
                // Optimization -> rdf:type is transformed into a label. Reduces node density and uses indexes.
                Map<String, Object> params = new HashMap<>();
                params.put("subject", subject.stringValue().replace("'", "\'"));
                params.put("object", object.stringValue());
                cypherStatements.add(new QueryWithParams(String.format("MERGE (x:Resource:%s {uri:{subject}}) SET x:`%s` MERGE (y:%s {uri:{object}}) SET y:Class",
                        (subject instanceof BNode ? "BNode" : "URI"),
                        object.stringValue(), (object instanceof BNode ? "BNode" : "URI")), params));
            } else {
                Map<String, Object> params = new HashMap<>();
                params.put("subject", subject.stringValue().replace("'", "\'"));
                params.put("object", object.stringValue());
                cypherStatements.add(new QueryWithParams(String.format("MERGE (x:Resource:%s {uri:{subject}}) MERGE (y:Resource:%s {uri:{object}}) MERGE (x)-[:`%s`]->(y)",
                        (subject instanceof BNode ? "BNode" : "URI"),
                        (object instanceof BNode ? "BNode" : "URI"),
                        predicate.stringValue()),params));
            }

            if (cypherStatements.size()%commitFreq == 0) {
                ingestedTriples+= Utils.inTx(graphdb,this);
                log.info("Successful periodic commit of " + commitFreq + " cypher statements. " + ingestedTriples + " triples ingested so far...");
            }
        }

        private String getObjectValue(Literal object) {
            IRI datatype = object.getDatatype();
            if (datatype.equals(XMLSchema.DECIMAL) || datatype.equals(XMLSchema.DOUBLE) ||
                    datatype.equals(XMLSchema.FLOAT) || datatype.equals(XMLSchema.INT) ||
                    datatype.equals(XMLSchema.INTEGER) || datatype.equals(XMLSchema.LONG)) {
                return object.stringValue();
            } else if (datatype.equals(XMLSchema.BOOLEAN)) {
                return object.stringValue();
            } else {
                return object.stringValue().replace("\\", "\\\\").replace("'", "\\'");
                //not sure this is the best way to 'clean' the property value
            }
        }

        public int getIngestedTriples() {
            return ingestedTriples;
        }

        @Override
        public Integer call() throws Exception {
            int count = 0;
            for(QueryWithParams qwp : cypherStatements){
                graphdb.execute(qwp.getCypherString(), qwp.getParams());
                count++;
            }
            cypherStatements.clear();
            return count;
        }

        private class QueryWithParams {

            private final String cypherString;
            private final Map<String, Object> params;

            public QueryWithParams(String cypherStr, Map<String, Object> par) {
                cypherString = cypherStr;
                params = par;
            }


            public String getCypherString() {
                return cypherString;
            }

            public Map<String, Object> getParams() {
                return params;
            }
        }
    }


    public static class ImportResults {
        public String terminationStatus = "OK";
        public long triplesLoaded = 0;
        public String extraInfo = "";

        public void setTriplesLoaded(long triplesLoaded) {
            this.triplesLoaded = triplesLoaded;
        }

        public void setTerminationKO(String message) {
            this.terminationStatus = "KO";
            this.extraInfo = message;
        }

    }

}
