package semantics.inference;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import semantics.result.NodeResult;
import semantics.result.RelAndNodeResult;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.RelationshipType.withName;

public class MicroReasoners {

    static final String sloInferenceCypher = "MATCH (:Label { name: $virtLabel})<-[:SLO*0..]-(sublabel:Label) WITH collect(sublabel.name) + $virtLabel AS subPlusSelf UNWIND subPlusSelf as sublabel RETURN distinct sublabel";
    static final String sroInferenceCypher = "MATCH (:Relationship { name: $virtRel})<-[:SRO*0..]-(subRel:Relationship) WITH COLLECT(subRel.name) + $virtRel AS subPlusSelf UNWIND subPlusSelf as subRel RETURN DISTINCT subRel";
    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    @Procedure(mode = Mode.READ)
    @Description("semantics.inference.getNodes('virtLabel') - returns all nodes with label 'virtLabel' or its sublabels.")
    public Stream<NodeResult> getNodes(@Name("virtLabel") String virtLabel) {

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("virtLabel", virtLabel);
        Result results = db.execute(sloInferenceCypher, params);
        StringBuilder sb = new StringBuilder();
        boolean isFirstSubLabel = true;
        while (results.hasNext()) {
            Map<String, Object> result = results.next();
            String subLabel = (String) result.get("sublabel");
            if (!isFirstSubLabel) sb.append(" UNION "); else isFirstSubLabel = false;
            sb.append(" MATCH (x:`").append(subLabel).append("`) RETURN x as result ");
        }
        if (!sb.toString().isEmpty()) {
            return db.execute(sb.toString()).stream().map(n -> (Node) n.get("result")).map(NodeResult::new);
        } else {
            return null;
        }
    }

    @Procedure(mode = Mode.READ)
    @Description("semantics.inference.getRels(node,'virtRel','>') - returns all outgoing relationships of type 'virtRel' " +
            "or its subtypes along with the target nodes.")
    public Stream<RelAndNodeResult> getRels(@Name("node") Node node, @Name("virtRel") String virtRel,
                                                 @Name(value="reldir",defaultValue = "") String directionString) {

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("virtRel", virtRel);

        Result results = db.execute(sroInferenceCypher, params);
        Set<RelationshipType> rts = new HashSet<RelationshipType>();
        while (results.hasNext()) {
            rts.add(withName((String)results.next().get("subRel")));
        }

        Direction direction = (directionString.equals(">")?Direction.OUTGOING:(directionString.equals("<")?Direction.INCOMING:Direction.BOTH));

        return StreamSupport.stream(node.getRelationships(direction, rts.toArray(new RelationshipType[0])).spliterator(),true)
                .map(n -> new RelAndNodeResult(n,n.getOtherNode(node)));

    }


    @UserFunction
    @Description("semantics.inference.hasLabel(node,'Label') - checks whether node is explicitly or implicitly labeled as 'Label'.")
    public boolean hasLabel(
            @Name("node") Node node,
            @Name("virtLabel") String virtLabel) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("virtLabel", virtLabel);
        Result results = db.execute(sloInferenceCypher, params);
        Set<String> sublabels = new HashSet<>();
        sublabels.add(virtLabel);
        while (results.hasNext()) {
            sublabels.add((String)results.next().get("sublabel"));
        }
        Iterable<Label> labels = node.getLabels();
        boolean is = false;
        for (Label label : labels) {
            is |= sublabels.contains(label.name());
        }

        return is;
    }

}
