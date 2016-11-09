package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.register.Register;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.server.HTTP;

import java.io.IOException;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;

/**
 * Created by jbarrasa on 14/09/2016.
 */
public class RDFEndpointTest {

    @Test
    public void testMyExtensionWithFunctionFixture() throws Exception
    {
        // Given
        try ( ServerControls server = getServerBuilder()
                .withExtension( "/rdf", RDFEndpoint.class )
                .withFixture( new Function<GraphDatabaseService, Void>()
                {
                    @Override
                    public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
                    {
                        try ( Transaction tx = graphDatabaseService.beginTx() )
                        {
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
                } )
                .newServer() )
        {
            // When
            Result result = server.graph().execute( "MATCH (n:Critic) return id(n) as id " );

            // Then
            //assertEquals( 1, count( result ) );

            Long id = (Long)result.next().get("id");

            // When
            HTTP.Response response = HTTP.withHeaders("accept","text/n3").GET(
                    HTTP.GET( server.httpURI().resolve( "rdf" ).toString() ).location() + "nodebyid?nodeid=" + id.toString());

            System.out.println(response.rawContent());
            // Then
            //assertEquals( 200, response.status() );
            //System.out.println(response.content().toString());
        }
    }

    @Test
    public void testNodeByUri() throws Exception
    {
        // Given
        try ( ServerControls server = getServerBuilder()
                .withExtension( "/rdf", RDFEndpoint.class )
                .withFixture( new Function<GraphDatabaseService, Void>()
                {
                    @Override
                    public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
                    {
                        try ( Transaction tx = graphDatabaseService.beginTx() )
                        {
                            String nsDefCreation = "CREATE (n:NamespacePrefixDefinition { `http://ont.thomsonreuters.com/mdaas/` : 'ns1' ,\n" +
                                    "`http://permid.org/ontology/organization/` : 'ns0' } ) ";
                            graphDatabaseService.execute(nsDefCreation);
                            String dataInsertion = "CREATE (Keanu:Resource:ns0_Actor {ns1_name:'Keanu Reeves', ns1_born:1964, uri: 'https://permid.org/1-21523433750' })\n" +
                                    "CREATE (Carrie:Resource:ns0_Director {ns1_name:'Carrie-Anne Moss', ns1_born:1967, uri: 'https://permid.org/1-21523433751' })\n" +
                                    "CREATE (Laurence:Resource:ns0_Director {ns1_name:'Laurence Fishburne', ns1_born:1961, uri: 'https://permid.org/1-21523433752' })\n" +
                                    "CREATE (Hugo:Resource:ns0_Critic {ns1_name:'Hugo Weaving', ns1_born:1960, uri: 'https://permid.org/1-21523433753' })\n" +
                                    "CREATE (AndyW:Resource:ns0_Actor {ns1_name:'Andy Wachowski', ns1_born:1967, uri: 'https://permid.org/1-21523433754' })\n" +
                                    "CREATE (Keanu)-[:ns0_Likes]->(Carrie) ";
                            graphDatabaseService.execute(dataInsertion);
                            tx.success();
                        }
                        return null;
                    }
                } )
                .newServer() )
        {

            Result result = server.graph().execute( "MATCH (n:ns0_Critic) return id(n) as id " );
            //assertEquals( 1, count( result ) );

            Long id = (Long)result.next().get("id");

            HTTP.Response response = HTTP.withHeaders("accept","text/n3").GET(
                    HTTP.GET(server.httpURI().resolve("rdf").toString()).location() + "nodebyuri?uri=https://permid.org/1-21523433750");

            System.out.println(response.rawContent());
            //assertEquals( 200, response.status() );
        }
    }

    private TestServerBuilder getServerBuilder( ) throws IOException
    {
        TestServerBuilder serverBuilder = TestServerBuilders.newInProcessBuilder();
        serverBuilder.withConfig( ServerSettings.certificates_directory.name(),
                ServerTestUtils.getRelativePath( getSharedTestTemporaryFolder(), ServerSettings.certificates_directory ) );
        return serverBuilder;
    }
}
