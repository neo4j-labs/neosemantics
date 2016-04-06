package semantics;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFImport {
    @Context
    public GraphDatabaseService db;

    // Result class
    public static class Degree {
        public String label;
        // note, that "int" values are not supported
        public long count, max, min = Long.MAX_VALUE;

        public Degree(String label) {
            this.label = label;
        }

        // method to consume a degree and compute min, max, count
        private void add(int degree) {
            if (degree < min) min = degree;
            if (degree > max) max = degree;
            count ++;
        }
    }

    @Procedure
    public Stream<ConsistencyViolation> runConsistencyChecks() {
        //@Name("breakOnFirst") String breakOnFirst
        // create holder class for results
        Result cc1 = db.execute("MATCH (n:Class)<-[:DOMAIN]-(p:DatatypeProperty) \n" +
                "RETURN DISTINCT n.uri as classUri, n.label as classLabel, p.uri as prop, p.label as propLabel");

        Map<String,Set<String>> propLabelPairs = new HashMap<String,Set<String>>();
        while (cc1.hasNext()){
            Map<String, Object> record = cc1.next();
            if(propLabelPairs.containsKey(record.get("propLabel"))){
                propLabelPairs.get(record.get("propLabel")).add((String) record.get("classLabel"));
            }else {
                Set labels = new HashSet<String> ();
                labels.add((String) record.get("classLabel"));
                propLabelPairs.put((String) record.get("propLabel"), labels);
            }
        }
        Set<String> props = propLabelPairs.keySet();
        StringBuffer sb = new StringBuffer();
        for (String prop : props) {
            sb.append(" exists(x." + prop + ") AND (");
            Set<String> labels = propLabelPairs.get(prop);
            boolean first = true;
            for(String label : labels){
                if(!first) sb.append(" OR ");
                sb.append("(NOT '" + label + "' IN labels(x))");
                first = false;
            }
            sb.append(")");
        }

//        "MATCH (x) WHERE  " +
//                "RETURN  id(x) AS nodeUID, \n" +
//                "        'domain of ' + propLabel + ' [' + prop + ']' AS `check failed`,  \n" +
//                "\t\t'Node labels: (' + reduce(s = '', l IN Labels(x) | s + ' ' + l) + ') should include ' + classLabel AS extraInfo");


//
//        db.getAllPropertyKeys();
//        // iterate over all nodes with label
//        try (ResourceIterator<Node> it = db.findNodes(Label.label(label))) {
//            while (it.hasNext()) {
//                // submit degree to holder for consumption (i.e. max, min, count)
//                degree.add(it.next().getDegree());
//            }
//        }
        // we only return a "Stream" of a single element in this case.
        return cc1.stream().map(ConsistencyViolation::new);
    }

    public static class ConsistencyViolation
    {
        public String extraInfo;
        public String checkFailed;
        public long nodeId;

        public ConsistencyViolation( Map<String, Object> record)
        {
            this.nodeId = (long) record.get("nodeUID");
            this.checkFailed = (String) record.get("check failed");
            this.extraInfo = (String) record.get("extraInfo");
        }
    }
}
