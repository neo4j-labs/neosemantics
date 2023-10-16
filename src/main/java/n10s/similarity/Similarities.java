package n10s.similarity;

import n10s.graphconfig.GraphConfig;
import n10s.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

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
            "MATCH sp = ((a)-[:`%1$s`*0..]->(cs)<-[:`%1$s`*0..]-(b)) " +
            "RETURN cs, sp order by length(sp) limit 1";

    private static String shortestPathSearchWithMaxDepth =
            "MATCH (a) where id(a) = id($a_cat) \n" +
                    "MATCH sp = ((a:`%1$s`)-[:`%2$s`*0..]->(cs)<-[:`%2$s`*0..]-(b:`%1$s`)) where 0 < length(sp) <= $threshold_length \n" +
                    "WITH cs, b, min(length(sp)) as dist \n" +
                    "MATCH total_depth = (leaf)-[:`%2$s`*0..]->(cs)-[:`%2$s`*0..]->(root) where not (root)-[:`%2$s`]->() and not ()-[:`%2$s`]->(leaf) \n" +
                    "WITH b, dist, max(length(total_depth) + 1) as depth \n" +
                    "RETURN b as node, - log ( 1.0 * dist / (2.0 * depth)) as sim ";
    private static String shortestPathSearch =
            "MATCH (a) where id(a) = id($a_cat)" +
                    "MATCH sp = ((a:`%1$s`)-[:`%2$s`*0..]->()<-[:`%2$s`*0..]-(b:`%1$s`)) where 0 < length(sp) <= $threshold_length " +
                    "RETURN  b as node, 1.0 / (1.0 + min(length(sp))) as sim order by sim desc ";

    private static String shortestPathQueryIndirect =
            "MATCH (a),(b) where id(a) = id($a_cat) and id(b) = id($b_cat)" +
            "match pa = (a)-[:`%1$s`*0..]->(root_a) where not (root_a)-[:`%1$s`]->()\n" +
            "match pb = (b)-[:`%1$s`*0..]->(root_b) where not (root_b)-[:`%1$s`]->()\n" +
            "return pa, root_a, pb, root_b, (length(pa) + length(pb)) as total_len order by total_len limit 1";

    private static String maxDepthQuery =
            "MATCH (a) where id(a) = id($a_cat) " +
                    "match pa = (leaf_a)-[:`%1$s`*0..]->(a)-[:`%1$s`*0..]->(root_a) where not (root_a)-[:`%1$s`]->() and not ()-[:`%1$s`]->(leaf_a) \n" +
                    "return length(pa) + 1 as len order by len desc limit 1";

    private static String globalMaxDepthQuery =
            "MATCH (a) where id(a) = id($a_cat) " +
                    "match (a)-[:`%1$s`*0..]->(root_a) where not (root_a)-[:`%1$s`]->() \n" +
                    "with distinct root_a \n" +
                    "match pa = (leaf_a)-[:`%1$s`*0..]->(root_a) where not ()-[:`%1$s`]->(leaf_a)" +
                    "return root_a, max(length(pa) + 1) as len order by len desc limit 1";
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
        Result spResult = tx.execute(String.format(shortestPathQuery, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            Path shortest = (Path) spResult.next().get("sp");
            return 1.0D / (1.0D + shortest.length());
        } else if (simulateRoot){
            spResult = tx.execute(String.format(shortestPathQueryIndirect, subclassOfRel), queryParams);
            if (spResult.hasNext()) {
                return 1.0D / (1.0D + (Long)spResult.next().get("total_len") + 2.0D);
            } else {
                return 0D; //or null
            }
        } else {
            return 0D; //or null
        }
    }

    @Procedure(name = "n10s.sim.pathsim.search")
    @Description("n10s.sim.pathsim.search() - returns the elements in the taxonomy with a path similarity equal or greater than a given threshold.")
    public Stream<SemanticSearchResult> pathSimSearch(@Name("node1") Node elem1, @Name("simThreshold") Double minSim,
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
        queryParams.put("threshold_length", (1.0 / minSim) - 1 );

        return tx.execute(String.format(shortestPathSearch, classLabel, subclassOfRel), queryParams).stream().map(SemanticSearchResult::new);
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

        Long max_depth = Long.MAX_VALUE;
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("a_cat", elem1);
        queryParams.put("b_cat", elem2);
        Result spResult = tx.execute(String.format(shortestPathQuery, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            Map<String, Object> next = spResult.next();
            Path shortest = (Path) next.get("sp");
            queryParams = new HashMap<>();
            queryParams.put("a_cat", next.get("cs"));
            max_depth = (Long)tx.execute(String.format(maxDepthQuery, subclassOfRel), queryParams).next().get("len");
            return - Math.log(shortest.length()/ (2.0D * max_depth));
        } else if (simulateRoot){
            spResult = tx.execute(String.format(shortestPathQueryIndirect, subclassOfRel), queryParams);
            if (spResult.hasNext()) {
                Map<String, Object> next = spResult.next();
                Long distance = (Long) next.get("total_len");
                queryParams = new HashMap<>();
                queryParams.put("a_cat", next.get("root_a"));
                Long max_depth_a = (Long)tx.execute(String.format(maxDepthQuery, subclassOfRel), queryParams).next().get("len");
                queryParams.put("a_cat", next.get("root_b"));
                Long max_depth_b = (Long)tx.execute(String.format(maxDepthQuery, subclassOfRel), queryParams).next().get("len");
                return - Math.log((distance + 2.0D)/ (2.0D * (max_depth_a >= max_depth_b?max_depth_a:max_depth_b)));
            } else {
                return 0D; //or null
            }
        } else {
            return 0D; //or null
        }
    }


    @Procedure(name = "n10s.sim.lchsim.search")
    @Description("n10s.sim.lchsim.search() - returns the elements in the taxonomy with a Leacock-Chodorow similarity equal or greater than a given threshold.")
    public Stream<SemanticSearchResult> lchSimSearch(@Name("node1") Node elem1, @Name("simThreshold") Double minSim,
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
        Long worst_case_total_depth = (Long) tx.execute(String.format(globalMaxDepthQuery, subclassOfRel), queryParams).next().get("len");

        queryParams.put("threshold_length", ((2.0 *  worst_case_total_depth)/ Math.pow(10,minSim)));
        //TODO: THIS IS NOT EFFICIENT: EXTRACT THE DEPTH COMPUTATION AND CACHE
        System.out.println(String.format(shortestPathSearchWithMaxDepth, classLabel, subclassOfRel));
        return tx.execute(String.format(shortestPathSearchWithMaxDepth, classLabel, subclassOfRel), queryParams).stream().map(SemanticSearchResult::new);
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
        Result spResult = tx.execute(String.format(depthQuery, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            depth_a = (Long) spResult.next().get("len");
        }
        queryParams.put("a_cat", elem2);
        spResult = tx.execute(String.format(depthQuery, subclassOfRel), queryParams);
        if (spResult.hasNext()) {
            depth_b = (Long) spResult.next().get("len");
        }

        queryParams.put("a_cat", elem1);
        queryParams.put("b_cat", elem2);
        spResult = tx.execute(String.format(lcsQuery, subclassOfRel), queryParams);
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
        Result spResult = tx.execute(String.format(shortestPathQuery, subclassOfRel), queryParams);

        if (spResult.hasNext()) {
            Path shortest = (Path) spResult.next().get("sp");
            return shortest;
        } else if (simulateRoot){
            spResult = tx.execute(String.format(shortestPathQueryIndirect, subclassOfRel), queryParams);
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
