package semantics;

import org.eclipse.rdf4j.model.util.URIUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
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


    @Procedure(mode = Mode.WRITE)
    public Stream<ImportResults> liteOntoImport(@Name("url") String url, @Name("format") String format) {
        ImportResults importResults = new ImportResults();
        URL documentUrl;
        int classesLoaded = 0;
        int datatypePropsLoaded = 0;
        int objPropsLoaded = 0;
        int propsLoaded = 0;
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
            propsLoaded = extractProps(model, RDF.PROPERTY);

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
                Set<Value> classeNames = model.filter(propResource, RDFS.LABEL, null).objects();
                props.put("name",classeNames.isEmpty()?((IRI)propResource).getLocalName():
                        classeNames.iterator().next().stringValue().replace("'", "\'"));

                Set<Value> comments = model.filter(propResource, RDFS.COMMENT, null).objects();
                if (!comments.isEmpty()){
                    props.put("comment", comments.iterator().next().stringValue().replace("'", "\'"));
                }
                Map<String, Object> params = new HashMap<>();
                params.put("props", props);
                db.execute(cypher, params);
                propsLoaded++;
                extractDomainAndRange(model, propResource, propType);
                extractPropertyHierarchy(model, propResource, propType);
            }
        }
        return propsLoaded;
    }

    private String getNeoEquivalentForProp(IRI propType) {
        if(propType.equals(OWL.DATATYPEPROPERTY)){
            return "Property";
        } else if(propType.equals(OWL.OBJECTPROPERTY)) {
            //It is an objectproperty
            return "Relationship";
        } else {
            //it's an rdfs:Property
            return "Property:Relationship";
        }
    }

    private void extractDomainAndRange(Model model, Resource propResource, IRI propType) {
        for (Value object: model.filter(propResource, RDFS.DOMAIN, null).objects()){
            if (object instanceof IRI && (model.contains((IRI)object,RDF.TYPE, OWL.CLASS) ||
                    model.contains((IRI)object,RDF.TYPE, RDFS.CLASS) ||
                    model.contains((IRI)object,RDF.TYPE, OWL.OBJECTPROPERTY))){
                //This last bit picks up OWL definitions of attributes on properties.
                // Totally non standard, but in case it's used to accommodate LPG style properties on rels definitions
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

    private void extractPropertyHierarchy(Model model, Resource propResource, IRI propType) {
        for (Value object: model.filter(propResource, RDFS.SUBPROPERTYOF, null).objects()){
            if (object instanceof IRI && (model.contains((IRI)object,RDF.TYPE, OWL.OBJECTPROPERTY) ||
                    model.contains((IRI)object,RDF.TYPE, OWL.DATATYPEPROPERTY) ||
                    model.contains((IRI)object,RDF.TYPE, RDF.PROPERTY))){
                db.execute(String.format(
                        "MATCH (p:%s { uri:'%s'}), (c { uri:'%s'}) MERGE (p)-[:%s]->(c)",
                        getNeoEquivalentForProp(propType), propResource.stringValue(), object.stringValue(),
                        (propType.equals(OWL.DATATYPEPROPERTY)?"SPO":"SRO")));
            }
        }
    }

    private int extractClasses(Model model) {
        // loads Simple Named Classes (https://www.w3.org/TR/2004/REC-owl-guide-20040210/#SimpleClasses)
        int classesLoaded = 0;
        Set<Resource> allClasses = model.filter(null, RDF.TYPE, OWL.CLASS).subjects();
        allClasses.addAll(model.filter(null, RDF.TYPE, RDFS.CLASS).subjects());
        Model scoStatements = model.filter(null, RDFS.SUBCLASSOF, null);
        allClasses.addAll(scoStatements.subjects());
        scoStatements.objects().stream().filter(x -> x instanceof IRI).forEach( x -> allClasses.add((IRI)x));
        for ( Resource classResource : allClasses) {
            if (!(classResource instanceof BNode)) {
                String cypher = String.format("MERGE (p:Class { uri:'%s'}) SET p+={props}", classResource.stringValue());
                Map<String, Object> props = new HashMap<>();
                for (Value classLabel : model.filter(classResource, RDFS.LABEL, null).objects()) {
                    props.put("name", classLabel.stringValue().replace("'", "\'"));
                    break;
                }
                for (Value classComment : model.filter(classResource, RDFS.COMMENT, null).objects()) {
                    props.put("comment", classComment.stringValue().replace("'", "\'"));
                    break;
                }
                Map<String, Object> params = new HashMap<>();
                params.put("props", props);
                db.execute(cypher, params);
                extractClassHierarchy(model,classResource);
                classesLoaded++;
            }
        }
        return classesLoaded;
    }

    private void extractClassHierarchy(Model model, Resource classResource) {
        for (Value object: model.filter(classResource, RDFS.SUBCLASSOF, null).objects()){
            if (object instanceof IRI){
                db.execute(String.format(
                        "MATCH (p:Class { uri:'%s'}), (c { uri:'%s'}) MERGE (p)-[:SCO]->(c)",
                        classResource.stringValue(), object.stringValue()));
            }
        }
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
