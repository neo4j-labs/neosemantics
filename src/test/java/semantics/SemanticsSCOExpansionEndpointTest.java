package semantics;

import org.junit.Test;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.server.HTTP;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;

/**
 * Created by jbarrasa on 14/09/2016.
 */
public class SemanticsSCOExpansionEndpointTest {

    @Test
    public void testMyExtensionWithFunctionFixture() throws Exception {
        // Given
        try (ServerControls server = getServerBuilder()
                .withExtension("/rdf", SemanticsSCOExpansionEndpoint.class)
                .withFixture(new Function<GraphDatabaseService, Void>() {
                    @Override
                    public Void apply(GraphDatabaseService graphDatabaseService) throws RuntimeException {
                        try (Transaction tx = graphDatabaseService.beginTx()) {
                            String ontoCreation = "MERGE (p:Category {catName: \"Person\"})\n" +
                                    "MERGE (a:Category {catName: \"Actor\"})\n" +
                                    "MERGE (d:Category {catName: \"Director\"})\n" +
                                    "MERGE (c:Category {catName: \"Critic\"})\n" +
                                    "CREATE (a)-[:SCO]->(p)\n" +
                                    "CREATE (d)-[:SCO]->(p)\n" +
                                    "CREATE (c)-[:SCO]->(p)\n" +
                                    "RETURN *";
                            graphDatabaseService.execute(ontoCreation);
                            String dataInsertion = "CREATE (Keanu:Actor {name:'Keanu Reeves', born:1964})\n" +
                                    "CREATE (Carrie:Director {name:'Carrie-Anne Moss', born:1967})\n" +
                                    "CREATE (Laurence:Director {name:'Laurence Fishburne', born:1961})\n" +
                                    "CREATE (Hugo:Critic {name:'Hugo Weaving', born:1960})\n" +
                                    "CREATE (AndyW:Actor {name:'Andy Wachowski', born:1967})";
                            graphDatabaseService.execute(dataInsertion);
                            tx.success();
                        }
                        return null;
                    }
                })
                .newServer()) {
            // When
            Result result = server.graph().execute("MATCH (n:Critic) return n");

            // Then
            assertEquals(1, count(result));

            // When
            HTTP.Response response = HTTP.POST(
                    HTTP.GET(server.httpURI().resolve("rdf").toString()).location() + "semanticypher", "MATCH(x:Person) RETURN x");

            // Then
            assertEquals(200, response.status());
            System.out.println(response.content().toString());
        }
    }

    private TestServerBuilder getServerBuilder() throws IOException {
        TestServerBuilder serverBuilder = TestServerBuilders.newInProcessBuilder();
        serverBuilder.withConfig(ServerSettings.certificates_directory.name(),
                ServerTestUtils.getRelativePath(getSharedTestTemporaryFolder(), ServerSettings.certificates_directory));
        return serverBuilder;
    }
}