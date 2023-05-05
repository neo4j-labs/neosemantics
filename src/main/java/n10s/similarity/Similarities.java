package n10s.similarity;

import n10s.graphconfig.GraphConfig;
import n10s.result.PathResult;
import n10s.result.VirtualNode;
import n10s.result.VirtualPath;
import n10s.result.VirtualRelationship;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.*;

public class Similarities {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    private static final String CLASS_LABEL_INLINE_PARAM = "classLabel";
    private static final String SUB_CLASS_OF_REL_PARAM = "subClassOfRel";

    private static final String SIMULATE_ROOT_PARAM = "simulateRoot";
    private static String shortestPathQuery =
            "MATCH (a),(b) where id(a) = id($a_cat) and id(b) = id($b_cat)" +
            "MATCH sp = ((a)-[:`%1$s`*0..]->()<-[:`%1$s`*0..]-(b)) " +
            "RETURN sp order by length(sp) desc limit 1";

    private static String shortestPathQueryIndirect =
            "MATCH (a),(b) where id(a) = id($a_cat) and id(b) = id($b_cat)" +
            "match pa = (a)-[:`%1$s`*0..]->(root_a) where not (root_a)-[:`%1$s`]->()\n" +
            "match pb = (b)-[:`%1$s`*0..]->(root_b) where not (root_b)-[:`%1$s`]->()\n" +
            "return pa, pb limit 1";

    private static String maxDepthQuery =
            "MATCH (a) where id(a) = id($a_cat) " +
                    "match pa = (leaf_a)-[:`%1$s`*0..]->(a)-[:`%1$s`*0..]->(root_a) where not (root_a)-[:`%1$s`]->() and not ()-[:`%1$s`]->(leaf_a) \n" +
                    "return length(pa) + 1 as len order by len desc limit 1";

    private static String depthQuery =
            "MATCH (a) where id(a) = id($a_cat) " +
                    "match pa = (a)-[:`%1$s`*0..]->(root_a) where not (root_a)-[:`%1$s`]->()  \n" +
                    "return length(pa) + 1 as len order by len limit 1";

    private static String lcsQuery =
            "MATCH (a),(b) where id(a) = id($a_cat) and id(b) = id($b_cat)" +
                    "MATCH (a)-[:`%1$s`*0..]->(lcs)<-[:`%1$s`*0..]-(b) with distinct lcs " +
                    "match lcs_path = (lcs)-[:`%1$s`*0..]->(root) where not (root)-[:`%1$s`]->()  \n" +
                    "RETURN lcs, length(lcs_path) + 1 as depth order by depth desc limit 1";

    @UserFunction(name = "n10s.sim.pathsim.value")
    @Description("n10s.sim.pathsim.value() - returns a numeric value representing the path similarity between two elements.")
    public Double pathSimVal(@Name("node1") Node elem1, @Name("node2") Node elem2,
                             @Name(value = "params", defaultValue = "{}") Map<String, Object> params) throws SimilarityCalculatorException {
        final GraphConfig gc = getGraphConfig();

        // We make sure the taxonomy structure is known, either via graphconfig or via inline param setting
        if(gc == null && missingParams(params, CLASS_LABEL_INLINE_PARAM, SUB_CLASS_OF_REL_PARAM)){
            throw new SimilarityCalculatorException("No GraphConfig or in-procedure params. Method cannot be run.");
        }

        Boolean simulateRoot = !params.containsKey(SIMULATE_ROOT_PARAM) || (params.containsKey(SIMULATE_ROOT_PARAM)&&((boolean)params.get(SIMULATE_ROOT_PARAM)==true));
        String classLabel = (gc != null ? gc.getClassLabelName() : (String)params.get(CLASS_LABEL_INLINE_PARAM));
        String subclassOfRel = (gc != null ? gc.getSubClassOfRelName() : (String)params.get(SUB_CLASS_OF_REL_PARAM));

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("a_cat", elem1);
        queryParams.put("b_cat", elem2);
        Result spResult = tx.execute(String.format(shortestPathQuery, subclassOfRel, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            Path shortest = (Path) spResult.next().get("sp");
            return 1.0D / (1.0D + shortest.length());
        } else if (simulateRoot){
            spResult = tx.execute(String.format(shortestPathQueryIndirect, subclassOfRel, subclassOfRel, subclassOfRel, subclassOfRel), queryParams);
            if (spResult.hasNext()) {
                Map<String, Object> next = spResult.next();
                Path a_leg = (Path) next.get("pa");
                Path b_leg = (Path) next.get("pb");
                return 1.0D / (1.0D + a_leg.length() + b_leg.length() + 2.0D);
            } else {
                return 0D; //or null
            }
        } else {
            return 0D; //or null
        }
    }

    @UserFunction(name = "n10s.sim.lchsim.value")
    @Description("n10s.sim.lchsim.value() - returns a numeric value representing the Leacock-Chodorov similarity between two elements.")
    public Double lchSimVal(@Name("node1") Node elem1, @Name("node2") Node elem2,
                             @Name(value = "params", defaultValue = "{}") Map<String, Object> params) throws SimilarityCalculatorException {
        final GraphConfig gc = getGraphConfig();

        // We make sure the taxonomy structure is known, either via graphconfig or via inline param setting
        if(gc == null && missingParams(params, CLASS_LABEL_INLINE_PARAM, SUB_CLASS_OF_REL_PARAM)){
            throw new SimilarityCalculatorException("No GraphConfig or in-procedure params. Method cannot be run.");
        }

        Boolean simulateRoot = !params.containsKey(SIMULATE_ROOT_PARAM) || (params.containsKey(SIMULATE_ROOT_PARAM)&&((boolean)params.get(SIMULATE_ROOT_PARAM)==true));
        String classLabel = (gc != null ? gc.getClassLabelName() : (String)params.get(CLASS_LABEL_INLINE_PARAM));
        String subclassOfRel = (gc != null ? gc.getSubClassOfRelName() : (String)params.get(SUB_CLASS_OF_REL_PARAM));

        Long len_a = Long.MAX_VALUE;
        Long len_b = Long.MAX_VALUE;
        Long max_depth = Long.MAX_VALUE;
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("a_cat", elem1);
        Result spResult = tx.execute(String.format(maxDepthQuery, subclassOfRel, subclassOfRel, subclassOfRel, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            len_a = (Long) spResult.next().get("len");
        }
        queryParams.put("a_cat", elem2);
        spResult = tx.execute(String.format(maxDepthQuery, subclassOfRel, subclassOfRel, subclassOfRel, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            len_b = (Long) spResult.next().get("len");
        }

        if (len_a > len_b){
            max_depth = len_a;
        } else {
            max_depth =  len_b;
        }

        queryParams.put("a_cat", elem1);
        queryParams.put("b_cat", elem2);
        spResult = tx.execute(String.format(shortestPathQuery, subclassOfRel, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            Path shortest = (Path) spResult.next().get("sp");
            return - Math.log(shortest.length()/ (2.0D * max_depth));
        } else if (simulateRoot){
            spResult = tx.execute(String.format(shortestPathQueryIndirect, subclassOfRel, subclassOfRel, subclassOfRel, subclassOfRel), queryParams);
            if (spResult.hasNext()) {
                Map<String, Object> next = spResult.next();
                Path a_leg = (Path) next.get("pa");
                Path b_leg = (Path) next.get("pb");
                return - Math.log((a_leg.length() + b_leg.length() + 2.0D)/ (2.0D * max_depth));
            } else {
                return 0D; //or null
            }
        } else {
            return 0D; //or null
        }
    }


    @UserFunction(name = "n10s.sim.wupsim.value")
    @Description("n10s.sim.wupsim.value() - returns a numeric value representing the Wu&Palmer similarity between two elements.")
    public Double wupSimVal(@Name("node1") Node elem1, @Name("node2") Node elem2,
                            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) throws SimilarityCalculatorException {
        final GraphConfig gc = getGraphConfig();

        // We make sure the taxonomy structure is known, either via graphconfig or via inline param setting
        if(gc == null && missingParams(params, CLASS_LABEL_INLINE_PARAM, SUB_CLASS_OF_REL_PARAM)){
            throw new SimilarityCalculatorException("No GraphConfig or in-procedure params. Method cannot be run.");
        }

        Boolean simulateRoot = !params.containsKey(SIMULATE_ROOT_PARAM) || (params.containsKey(SIMULATE_ROOT_PARAM)&&((boolean)params.get(SIMULATE_ROOT_PARAM)==true));
        String classLabel = (gc != null ? gc.getClassLabelName() : (String)params.get(CLASS_LABEL_INLINE_PARAM));
        String subclassOfRel = (gc != null ? gc.getSubClassOfRelName() : (String)params.get(SUB_CLASS_OF_REL_PARAM));

        Long depth_a = 0L;
        Long depth_b = 0L;

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("a_cat", elem1);
        Result spResult = tx.execute(String.format(depthQuery, subclassOfRel, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            depth_a = (Long) spResult.next().get("len");
        }
        queryParams.put("a_cat", elem2);
        spResult = tx.execute(String.format(depthQuery, subclassOfRel, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            depth_b = (Long) spResult.next().get("len");
        }

        queryParams.put("a_cat", elem1);
        queryParams.put("b_cat", elem2);
        spResult = tx.execute(String.format(lcsQuery, subclassOfRel, subclassOfRel,subclassOfRel, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            Map<String, Object> next = spResult.next();
            Node lcs = (Node) next.get("lcs");
            Long lcs_depth = (Long) next.get("depth");
            return ((2D* lcs_depth) / (depth_a + depth_b));
        } else if (simulateRoot){
                return (2D / (depth_a + depth_b));
        } else {
            return 0D; //or null
        }
    }

    @UserFunction(name = "n10s.sim.pathsim.path")
    @Description("n10s.sim.pathsim.path() - returns a numeric value representing the path similarity between two elements.")
    public Path pathSimPath(@Name("node1") Node elem1, @Name("node2") Node elem2,
                             @Name(value = "params", defaultValue = "{}") Map<String, Object> params) throws SimilarityCalculatorException {
        final GraphConfig gc = getGraphConfig();

        // We make sure the taxonomy structure is known, either via graphconfig or via inline param setting
        if(gc == null && missingParams(params, CLASS_LABEL_INLINE_PARAM, SUB_CLASS_OF_REL_PARAM)){
            throw new SimilarityCalculatorException("No GraphConfig or in-procedure params. Method cannot be run.");
        }

        Boolean simulateRoot = !params.containsKey(SIMULATE_ROOT_PARAM) || (params.containsKey(SIMULATE_ROOT_PARAM)&&((boolean)params.get(SIMULATE_ROOT_PARAM)==true));
        String classLabel = gc != null ? gc.getClassLabelName() : (String)params.get(CLASS_LABEL_INLINE_PARAM);
        String subclassOfRel = gc != null ? gc.getSubClassOfRelName() : (String)params.get(SUB_CLASS_OF_REL_PARAM);

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("a_cat", elem1);
        queryParams.put("b_cat", elem2);
        Result spResult = tx.execute(String.format(shortestPathQuery, subclassOfRel, subclassOfRel), queryParams);

        if (spResult.hasNext()) {
            Path shortest = (Path) spResult.next().get("sp");
            return shortest;
        } else if (simulateRoot){
            spResult = tx.execute(String.format(shortestPathQueryIndirect, subclassOfRel, subclassOfRel, subclassOfRel, subclassOfRel), queryParams);
            if (spResult.hasNext()) {
                Map<String, Object> next = spResult.next();
                Path a_leg = (Path) next.get("pa");
                Path b_leg = (Path) next.get("pb");
                return createVirtualPath(a_leg,b_leg,subclassOfRel).path;
            } else {
                return null; //or empty path?
            }
        }else {
            return null; //or empty path?
        }
    }


    private PathResult createVirtualPath(Path leg1, Path leg2, String scoRelType) {
        final Iterable<Relationship> leg1_rels = leg1.relationships();
        final Node first = leg1.startNode();
        VirtualPath virtualPath = new VirtualPath(new VirtualNode(first, Iterables.asList(first.getPropertyKeys())));
        Node current = first;
        for (Relationship rel : leg1_rels) {
            VirtualNode start = VirtualNode.from(rel.getStartNode());
            VirtualNode end = VirtualNode.from(rel.getEndNode());
            virtualPath.addRel(VirtualRelationship.from(start, end, rel));
            current = end;
        }

        //add intermediate node
        VirtualNode root = new VirtualNode(new Label[]{Label.label("SyntheticRoot")}, Collections.emptyMap());
        virtualPath.addRel(new VirtualRelationship(current, root,RelationshipType.withName(scoRelType)));


        final Iterable<Relationship> leg2_rels = leg2.relationships();
        List<Relationship> secondLegAsList = new ArrayList<Relationship>();

        for (Relationship rel : leg2_rels) {
            secondLegAsList.add(rel);
        }
        Collections.reverse(secondLegAsList);

        virtualPath.addRel(new VirtualRelationship(secondLegAsList.get(0).getEndNode(), root,RelationshipType.withName(scoRelType)));

        for (Relationship rel : secondLegAsList) {
            VirtualNode start = VirtualNode.from(rel.getStartNode());
            VirtualNode end = VirtualNode.from(rel.getEndNode());
            virtualPath.addRel(VirtualRelationship.from(start, end, rel));
        }
        return new PathResult(virtualPath);
    }

    private boolean missingParams(Map<String, Object> props, String... paramNames) {
        boolean missing = false;
        for (String param:paramNames) {
            missing |= !props.containsKey(param);
        }
        return  missing;
    }

    private GraphConfig getGraphConfig() {
        try {
            return new GraphConfig(tx);
        } catch (GraphConfig.GraphConfigNotFound graphConfigNotFound) {
            //no graph config
            return null;
        }
    }

}
