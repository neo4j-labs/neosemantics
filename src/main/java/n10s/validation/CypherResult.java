package n10s.validation;

import java.util.Map;

public class CypherResult {

    public final String cypher;
    public final Map<String, Object> params;

    public CypherResult(String cypher, Map<String, Object> params) {
        this.cypher = cypher;
        this.params = params;
    }

}
