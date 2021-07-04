package n10s.rdf.aggregate;

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

public class CollectTriples {
    @UserAggregationFunction
    @Description( "n10s.rdf.collect(...) - collects a set of triples and serialises them as N-triples" )
    public TripleCollector collect()
    {
        return new TripleCollector();
    }

    public static class TripleCollector
    {
        Model m = new DynamicModel(new LinkedHashModelFactory());
        ValueFactory vf = SimpleValueFactory.getInstance();
        ByteArrayOutputStream baos =  new ByteArrayOutputStream();


        @UserAggregationUpdate
        public void findLongest(
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
                Rio.write(m, baos, RDFFormat.NTRIPLES);
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
