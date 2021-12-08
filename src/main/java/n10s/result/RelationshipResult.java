package n10s.result;

import org.neo4j.graphdb.Relationship;

public class RelationshipResult {
    public final Relationship rel;

    public RelationshipResult(Relationship rel) {
        this.rel = rel;
    }
}