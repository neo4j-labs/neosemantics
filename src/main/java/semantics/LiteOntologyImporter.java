package semantics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import org.openrdf.model.*;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.StatementCollector;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Created by jbarrasa on 21/03/2016.
 *
 * Importer of basic ontology (RDFS & OWL) elements:
 *
 */
public class LiteOntologyImporter {
    @Context
    public GraphDatabaseService db;
    public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD, RDFFormat.TURTLE,
            RDFFormat.NTRIPLES, RDFFormat.TRIG};


    @Procedure
    @PerformsWrites
    public Stream<ImportResults> LiteOntoImport(@Name("url") String url, @Name("format") String format) {
        ImportResults importResults = new ImportResults();
        URL documentUrl;
        int classesLoaded = 0;
        int datatypePropsLoaded = 0;
        int objPropsLoaded = 0;
        try {
            documentUrl = new URL(url);
            InputStream inputStream = documentUrl.openStream();
            RDFParser rdfParser = Rio.createParser(getFormat(format));
            Model model = new LinkedHashModel();
            rdfParser.setRDFHandler(new StatementCollector(model));
            rdfParser.parse(inputStream, documentUrl.toString());
            classesLoaded = extractClasses(model);
            objPropsLoaded = extractProps(model, OWL.OBJECTPROPERTY);
            datatypePropsLoaded = extractProps(model, OWL.DATATYPEPROPERTY);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
            importResults.setTerminationKO(e.getMessage());
        } finally {
            importResults.setElementsLoaded(classesLoaded + datatypePropsLoaded + objPropsLoaded);
        }
        return Stream.of(importResults);
    }

    private int extractProps(Model model, IRI propType) {
        // loads properties
        int propsLoaded = 0;
        Set<Resource> allDatatypeProps = model.filter(null, RDF.TYPE, propType).subjects();
        for ( Resource propResource : allDatatypeProps) {
            if (!(propResource instanceof BNode)) {
                String cypher = String.format("MERGE (p:%s { uri:'%s'}) SET p+={props}",
                        getNeoEquivalentForProp(propType),
                        propResource.stringValue());
                Map<String, Object> props = new HashMap<>();
                for (Value propLabel : model.filter(propResource, RDFS.LABEL, null).objects()) {
                    props.put("label", propLabel.stringValue().replace("'", "\'"));
                    break;
                }
                for (Value propComment : model.filter(propResource, RDFS.COMMENT, null).objects()) {
                    props.put("comment", propComment.stringValue().replace("'", "\'"));
                    break;
                }
                Map<String, Object> params = new HashMap<>();
                params.put("props", props);
                db.execute(cypher, params);
                propsLoaded++;
                extractDomainAndRange(model, propResource, propType);
            }
        }
        return propsLoaded;
    }

    private String getNeoEquivalentForProp(IRI propType) {
        if(propType.equals(OWL.DATATYPEPROPERTY)){
            return "DatatypeProperty";
        }else {
            //It is an objectproperty
            return "ObjectProperty";
        }
    }

    private void extractDomainAndRange(Model model, Resource propResource, IRI propType) {
        for (Value object: model.filter(propResource, RDFS.DOMAIN, null).objects()){
            if (object instanceof IRI && (model.contains((IRI)object,RDF.TYPE, OWL.CLASS) ||
                    model.contains((IRI)object,RDF.TYPE, RDFS.CLASS) ||
                    model.contains((IRI)object,RDF.TYPE, OWL.OBJECTPROPERTY))){
                //This last bit picks up OWL definitions of attributes on properties.
                db.execute(String.format(
                        "MATCH (p:%s { uri:'%s'}), (c { uri:'%s'}) MERGE (p)-[:DOMAIN]->(c)",
                        // c can be a class or an object property
                        getNeoEquivalentForProp(propType), propResource.stringValue(), object.stringValue()));
            }
        }
        for (Value object: model.filter(propResource, RDFS.RANGE, null).objects()){
            if (object instanceof IRI && (model.contains((IRI)object,RDF.TYPE, OWL.CLASS) ||
                    model.contains((IRI)object,RDF.TYPE, RDFS.CLASS))){
                    //only picks ranges that are classes, which means, only ObjectProperties
                    // (no XSD ranges for DatatypeProps)
                db.execute(String.format(
                        "MATCH (p:%s { uri:'%s'}), (c:Class { uri:'%s'}) MERGE (p)-[:RANGE]->(c)",
                        getNeoEquivalentForProp(propType), propResource.stringValue(), object.stringValue()));
            }
        }
    }

    private int extractClasses(Model model) {
        // loads Simple Named Classes (https://www.w3.org/TR/2004/REC-owl-guide-20040210/#SimpleClasses)
        int classesLoaded = 0;
        Set<Resource> allClasses = model.filter(null, RDF.TYPE, OWL.CLASS).subjects();
        allClasses.addAll(model.filter(null, RDF.TYPE, RDFS.CLASS).subjects());
        for ( Resource classResource : allClasses) {
            if (!(classResource instanceof BNode)) {
                String cypher = String.format("MERGE (p:Class { uri:'%s'}) SET p+={props}", classResource.stringValue());
                Map<String, Object> props = new HashMap<>();
                for (Value classLabel : model.filter(classResource, RDFS.LABEL, null).objects()) {
                    props.put("label", classLabel.stringValue().replace("'", "\'"));
                    break;
                }
                for (Value classComment : model.filter(classResource, RDFS.COMMENT, null).objects()) {
                    props.put("comment", classComment.stringValue().replace("'", "\'"));
                    break;
                }
                Map<String, Object> params = new HashMap<>();
                params.put("props", props);
                db.execute(cypher, params);
                classesLoaded++;
            }
        }
        return classesLoaded;
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

    public static class ImportResults {
        public String terminationStatus = "OK";
        public long elementsLoaded = 0;
        public String extraInfo = "";

        public void setElementsLoaded(long elementsLoaded) {
            this.elementsLoaded = elementsLoaded;
        }

        public void setTerminationKO(String message) {
            this.terminationStatus = "KO";
            this.extraInfo = message;
        }

    }
}
