package semantics;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class ConsistencyChecker {
    @Context
    public GraphDatabaseService db;

    @Procedure
    public Stream<ConsistencyViolation> runConsistencyChecks() {

        Result dp_d = check_DP_D();

        return dp_d.stream().map(ConsistencyViolation::new);

    }

    private Result check_DP_D() {
        Result cc1 = db.execute("MATCH (n:Class)<-[:DOMAIN]-(p:DatatypeProperty) \n" +
                "RETURN DISTINCT n.uri as classUri, n.label as classLabel, p.uri as prop, p.label as propLabel");

        Result detectedConsistencyViolations = null;
        Map<String,Set<String>> propLabelPairs = new HashMap<>();
        while (cc1.hasNext()){
            Map<String, Object> record = cc1.next();
            if(propLabelPairs.containsKey(record.get("propLabel"))){
                propLabelPairs.get(record.get("propLabel")).add((String) record.get("classLabel"));
            }else {
                Set<String> labels = new HashSet<>();
                labels.add((String) record.get("classLabel"));
                propLabelPairs.put((String) record.get("propLabel"), labels);
            }
        }
        Set<String> props = propLabelPairs.keySet();
        StringBuffer sb = new StringBuffer();
        sb.append("MATCH (x) WHERE ");
        for (String prop : props) {
            sb.append(" exists(x." + prop + ") AND (");
            Set<String> labels = propLabelPairs.get(prop);
            boolean first = true;
            for(String label : labels){
                if(!first) sb.append(" OR ");
                sb.append("(NOT '" + label + "' IN labels(x))");
                first = false;
            }
            sb.append(") RETURN id(x) as nodeUID, 'DPD' as checkFailed , " +
                    "'Node labels [' + reduce(s = '', l IN Labels(x) | s + ' ' + l)  + '] should include " +
                    labels + "' as extraInfo");
            detectedConsistencyViolations = db.execute(sb.toString());
        }
        return detectedConsistencyViolations;
    }

    private Result check_OP_D() {
        Result cc1 = db.execute("MATCH (n:Class)<-[:DOMAIN]-(p:DatatypeProperty) \n" +
                "RETURN DISTINCT n.uri as classUri, n.label as classLabel, p.uri as prop, p.label as propLabel");

        Result detectedConsistencyViolations = null;
        Map<String,Set<String>> propLabelPairs = new HashMap<>();
        while (cc1.hasNext()){
            Map<String, Object> record = cc1.next();
            if(propLabelPairs.containsKey(record.get("propLabel"))){
                propLabelPairs.get(record.get("propLabel")).add((String) record.get("classLabel"));
            }else {
                Set<String> labels = new HashSet<>();
                labels.add((String) record.get("classLabel"));
                propLabelPairs.put((String) record.get("propLabel"), labels);
            }
        }
        Set<String> props = propLabelPairs.keySet();
        StringBuffer sb = new StringBuffer();
        sb.append("MATCH (x) WHERE ");
        for (String prop : props) {
            sb.append(" exists(x." + prop + ") AND (");
            Set<String> labels = propLabelPairs.get(prop);
            boolean first = true;
            for(String label : labels){
                if(!first) sb.append(" OR ");
                sb.append("(NOT '" + label + "' IN labels(x))");
                first = false;
            }
            sb.append(") RETURN id(x) as nodeUID, 'DPD' as checkFailed , " +
                    "'Node labels [' + reduce(s = '', l IN Labels(x) | s + ' ' + l)  + '] should include " +
                    labels + "' as extraInfo");
            detectedConsistencyViolations = db.execute(sb.toString());
        }
        return detectedConsistencyViolations;
    }

    public static class ConsistencyViolation
    {
        public String extraInfo;
        public String checkFailed;
        public long nodeId;

        public ConsistencyViolation( Map<String, Object> record)
        {
            this.nodeId = (long) record.get("nodeUID");
            this.checkFailed = (String) record.get("checkFailed");
            this.extraInfo = (String) record.get("extraInfo");
        }
    }
}
