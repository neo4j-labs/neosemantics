package semantics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
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
    public GraphDatabaseService db;
    public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD, RDFFormat.TURTLE,
            RDFFormat.NTRIPLES, RDFFormat.TRIG};


    @Procedure
    @PerformsWrites
    public Stream<ImportResults> importRDF(@Name("url") String url, @Name("format") String format) {
        ImportResults importResults = new ImportResults();
        URL documentUrl;
        StatementLoader statementLoader = new StatementLoader(db);
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

    class StatementLoader extends RDFHandlerBase {

        private int ingestedTriples = 0;
        private GraphDatabaseService graphdb;

        public StatementLoader(GraphDatabaseService db) {

            graphdb = db;
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
                cypher = String.format("MERGE (x:%s {uri:'%s'}) SET x.`%s` = %s",
                        (subject instanceof BNode ? "BNode" : "URI"), subject.stringValue(),
                        predicate.stringValue(), getObjectValue((Literal) object));
                graphdb.execute(cypher);
            } else if (predicate.equals(RDF.TYPE) && !(object instanceof BNode)){
                // Optimization -> rdf:type is transformed into a label. Reduces node density and uses indexes.
                // Notice that we are doing some inference here by adding :Class to the object
                cypher = String.format("MERGE (x:%s {uri:'%s'}) SET x:`%s` MERGE (y:%s {uri:'%s'}) SET y:Class",
                        (subject instanceof BNode ? "BNode" : "URI"), subject.stringValue(),
                        object.stringValue(), (object instanceof BNode ? "BNode" : "URI"), object.stringValue());
                graphdb.execute(cypher);
            } else {
                cypher = String.format("MERGE (x:%s {uri:'%s'}) MERGE (y:%s {uri:'%s'}) MERGE (x)-[:`%s`]->(y)",
                        (subject instanceof BNode ? "BNode" : "URI"), subject.stringValue(),
                        (object instanceof BNode ? "BNode" : "URI"), object.stringValue(),
                        predicate.stringValue());
                graphdb.execute(cypher);
            }

            ingestedTriples++;
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
                return "'" + object.stringValue().replace("\\", "\\\\").replace("'", "\\'") + "'";
                //not sure this is the best way to 'clean' the property value
            }
        }

        public int getIngestedTriples() {
            return ingestedTriples;
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
