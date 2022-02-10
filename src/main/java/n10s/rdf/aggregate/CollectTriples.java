package n10s.rdf.aggregate;

import n10s.mapping.MappingUtils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.DynamicModel;
import org.eclipse.rdf4j.model.impl.LinkedHashModelFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.neo4j.procedure.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CollectTriples {
    //kept for backwards compatibility
    @UserAggregationFunction(name = "n10s.rdf.collect")
    @Description( "n10s.rdf.collect(subject,predicate,object,isLiteral,literalType,literalLang) - " +
            "collects a set of triples as returned by n10s.rdf.export.* or n10s.rdf.stream.* " +
            "and returns them serialised as N-triples" )
    public TripleCollector collect()
    {
        return new TripleCollector(RDFFormat.NTRIPLES);
    }

    @UserAggregationFunction(name = "n10s.rdf.collect.ttl")
    @Description( "n10s.rdf.collect(subject,predicate,object,isLiteral,literalType,literalLang) - " +
            "collects a set of triples as returned by n10s.rdf.export.* or n10s.rdf.stream.* " +
            "and returns them serialised as Turtle" )
    public TripleCollector collectTurtle()
    {
        return new TripleCollector(RDFFormat.TURTLE);
    }

    @UserAggregationFunction(name = "n10s.rdf.collect.nt")
    @Description( "n10s.rdf.collect(subject,predicate,object,isLiteral,literalType,literalLang) - " +
            "collects a set of triples as returned by n10s.rdf.export.* or n10s.rdf.stream.* " +
            "and returns them serialised as Turtle" )
    public TripleCollector collectNTriples()
    {
        return new TripleCollector(RDFFormat.NTRIPLES);
    }

    @UserAggregationFunction(name = "n10s.rdf.collect.xml")
    @Description( "n10s.rdf.collect(subject,predicate,object,isLiteral,literalType,literalLang) - " +
            "collects a set of triples as returned by n10s.rdf.export.* or n10s.rdf.stream.* " +
            "and returns them serialised as RDF/XML" )
    public TripleCollector collectRdfXml()
    {
        return new TripleCollector(RDFFormat.RDFXML);
    }

    @UserAggregationFunction(name = "n10s.rdf.collect.json")
    @Description( "n10s.rdf.collect(subject,predicate,object,isLiteral,literalType,literalLang) - " +
            "collects a set of triples as returned by n10s.rdf.export.* or n10s.rdf.stream.* " +
            "and returns them serialised as JSON-LD" )
    public TripleCollector collectJson()
    {
        return new TripleCollector(RDFFormat.JSONLD);
    }

    @UserAggregationFunction(name = "n10s.rdf.collect.ttlstar")
    @Description( "n10s.rdf.collect(subject,predicate,object,isLiteral,literalType,literalLang) - " +
            "collects a set of triples as returned by n10s.rdf.export.* or n10s.rdf.stream.* " +
            "and returns them serialised as JSON-LD" )
    public TripleCollector collectTurtleStar()
    {
        return new TripleCollector(RDFFormat.TURTLESTAR);
    }

    public static class TripleCollector
    {
        Model m = new DynamicModel(new LinkedHashModelFactory());
        ValueFactory vf = SimpleValueFactory.getInstance();
        ByteArrayOutputStream baos =  new ByteArrayOutputStream();
        private RDFFormat format;

        public TripleCollector(RDFFormat f) {

            this.format = f;
        }


        @UserAggregationUpdate
        public void addTriple(
                @Name( "subject" ) String subject, @Name( "predicate" ) String predicate, @Name( "object" ) String object,
                @Name( "isLiteral" ) Boolean isLiteral, @Name( "literalType" ) String literalType,
                @Name( "literalLang" ) String literalLang)
        {
            Resource s = (subject.indexOf(58) < 0?vf.createBNode(subject):vf.createIRI(subject));
            IRI p = vf.createIRI(predicate);
            if(isLiteral){
                Literal o;
                if (literalType.equals(RDF.LANGSTRING.stringValue())){
                    o = vf.createLiteral(object, literalLang);
                } else{
                    o = vf.createLiteral(object,vf.createIRI(literalType));
                }

                //TODO: lang is lost here
                m.add(s,p,o);
            } else{
                Resource o = (object.indexOf(58) < 0?vf.createBNode(object):vf.createIRI(object));
                m.add(s,p,o);
            }

        }


        @UserAggregationResult
        public String result()
        {
            try {
                Rio.write(m, baos, format);
                return baos.toString();
            } finally {
                try {
                    baos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
