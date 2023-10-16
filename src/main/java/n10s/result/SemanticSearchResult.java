package n10s.result;

import org.neo4j.graphdb.Node;

import java.util.Map;

public class SemanticSearchResult {

    public Double similarity;
    public Node node;

    public SemanticSearchResult(Double sim, Node node) {
        this.similarity = sim;
        this.node = node;
    }

    public SemanticSearchResult(Map<String, Object> record) {
        this.similarity = (Double)record.get("sim");
        this.node = (Node)record.get("node");
    }
}

