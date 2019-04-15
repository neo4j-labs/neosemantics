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

    private static final String sloInferenceCypher = "RETURN $virtLabel as sublabel UNION MATCH (:Label { name: $virtLabel})<-[:SLO*]-(sublabel:Label) RETURN distinct sublabel.name as sublabel";
    private static final String scoAsLabelInferenceCypher = "MATCH (cat)<-[:SCO*0..]-(subcat) WHERE ID(cat) = $catId RETURN COLLECT(distinct ID(subcat)) as catIds";
    private static final String scoInferenceCypher = "MATCH (cat)<-[:SCO*0..]-(subcat) WHERE ID(cat) = $catId RETURN COLLECT(distinct ID(subcat)) as catIds";
    private static final String sroInferenceCypher = "RETURN $virtRel as subRel UNION MATCH (:Relationship { name: $virtRel})<-[:SRO*]-(subRel:Relationship) RETURN DISTINCT subRel.name as subRel";
    private static final String DEFAULT_SCO_REL_NAME = "SCO";
    private static final String DEFAULT_IN_CAT_REL_NAME = "IN_CAT";

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    @Procedure(mode = Mode.READ)
    @Description("semantics.inference.getNodesWithLabel('virtLabel') - returns all nodes with label 'virtLabel' or its sublabels.")
    public Stream<NodeResult> getNodesWithLabel(@Name("virtLabel") String virtLabel) {

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("virtLabel", virtLabel);
        Result results = db.execute(sloInferenceCypher, params);
        StringBuilder sb = new StringBuilder();
        sb.append("cypher runtime=slotted ");
        boolean isFirstSubLabel = true;
        while (results.hasNext()) {
            Map<String, Object> result = results.next();
            String subLabel = (String) result.get("sublabel");
            if (!isFirstSubLabel) sb.append(" UNION "); else isFirstSubLabel = false;
            sb.append(" MATCH (x:`").append(subLabel).append("`) RETURN x as result ");
        }
        if (!sb.toString().equals("cypher runtime=slotted ")) {
            return db.execute(sb.toString()).stream().map(n -> (Node) n.get("result")).map(NodeResult::new);
        } else {
            return null;
        }
    }

    @Procedure(mode = Mode.READ)
    @Description("semantics.inference.getNodesLinkedTo('catNode') - returns all nodes connected to Node 'catNode' or its subcategories.")
    public Stream<NodeResult> getNodesLinkedTo(@Name("catNode") Node catNode,
                                               @Name(value = "props", defaultValue = "{}") Map<String, Object> props) {

        final String inCatRelName = (props.containsKey("inCatRelName")?(String)props.get("inCatRelName"):DEFAULT_IN_CAT_REL_NAME);
        final String subCatRelName = (props.containsKey("subCatRelName")?(String)props.get("subCatRelName"):DEFAULT_SCO_REL_NAME);

        List<Long> subcatIds = getSubcatIds(catNode, subCatRelName);
        Map<String,Object> params =  new HashMap<>();
        StringBuilder sb = new StringBuilder();
        sb.append("cypher runtime=slotted ");
        boolean isFirstSubLabel = true;
        for(Long catId:subcatIds) {
            if (!isFirstSubLabel) sb.append(" UNION "); else isFirstSubLabel = false;
            sb.append(" MATCH (x)-[:`").append(inCatRelName).append("`]->(cat) WHERE ID(cat)=$cat_").append(catId).append(" RETURN x as result ");
            params.put("cat_" + catId, catId);
        }
        if (!sb.toString().equals("cypher runtime=slotted ")) {
            return db.execute(sb.toString(), params).stream().map(n -> (Node) n.get("result")).map(NodeResult::new);
        } else {
            return null;
        }
    }

    private List<Long> getSubcatIds(@Name("catNode") Node catNode, @Name(value = "subCatRelName", defaultValue = "SCO") String subCatRelName) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("catId", catNode.getId());
        return (List<Long>)db.execute((subCatRelName==null?scoInferenceCypher:scoInferenceCypher.replace("SCO",subCatRelName)), params).next().get("catIds");
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
    @Description("semantics.inference.hasLabel(individual,label) - checks whether node is explicitly or implicitly labeled as 'label'.")
    public boolean hasLabel(
            @Name("individual") Node individual,
            @Name("label") String label) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("virtLabel", label);
        Result results = db.execute(sloInferenceCypher, params);
        Set<String> sublabels = new HashSet<>();
        sublabels.add(label);
        while (results.hasNext()) {
            sublabels.add((String)results.next().get("sublabel"));
        }
        Iterable<Label> labels = individual.getLabels();
        boolean is = false;
        for (Label l : labels) {
            is |= sublabels.contains(l.name());
        }

        return is;
    }


    @UserFunction
    @Description("semantics.inference.isNodeLinkedTo(individual, category, 'inCatRel','subCatRel') - checks whether node is explicitly or implicitly in a category.")
    public boolean isNodeLinkedTo(
            @Name("node") Node individual, @Name("category") Node category, @Name(value = "props", defaultValue = "{}") Map<String, Object> props ) {

        final String inCatRelName = (props.containsKey("inCatRelName")?(String)props.get("inCatRelName"):DEFAULT_IN_CAT_REL_NAME);
        final String subCatRelName = (props.containsKey("subCatRelName")?(String)props.get("subCatRelName"):DEFAULT_SCO_REL_NAME);


        List<Long> subLabels = getSubcatIds(category, subCatRelName);

        Iterator<Relationship> relIterator = individual.getRelationships(RelationshipType.withName(inCatRelName), Direction.OUTGOING).iterator();

        boolean is = false;
        while( !is && relIterator.hasNext()){
            is |= subLabels.contains(relIterator.next().getEndNode().getId());
        }

        return is;
    }

}
