package semantics;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jbarrasa on 27/09/2016.
 */
@Path("/semanticypher")
public class SemanticsSCOExpansionEndpoint {

    @POST
    @Path("/")
    //@Consumes(MediaType.TEXT_PLAIN)
    public Response semanticypher(@Context GraphDatabaseService gds, String body) {
        return Response.ok().entity(new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                long startTimeStamp = System.currentTimeMillis();
                String query = removeQuotes(body); //in case it comes quoted (Neo4j browser)
                ObjectMapper om = new ObjectMapper();
                JsonGenerator jg = om.getJsonFactory().createJsonGenerator( os, JsonEncoding.UTF8 );
                //extract labels from query
                Set<String> labelsInQuery = new HashSet<String>();
                Pattern pattern = Pattern.compile("(?i)^\\s*(MATCH\\s*\\(\\s*\\w+)((?:\\s*:\\s*\\w+)+)(\\)\\s*(?:(<?-)|WHERE|RETURN).*)$");
                Matcher matcher = pattern.matcher(query);
                if(matcher.matches() && validQuery(matcher.group(3))) {
                    String prefix = matcher.group(1);
                    String suffix = matcher.group(3);
                    String[] labels = matcher.group(2).replace(" ", "").substring(1).split(":");
                    for (int i = 0; i < labels.length; i++) {
                        labelsInQuery.add(labels[i]);
                    }
                    //extract subcat of relationships from ontology and expand original query
                    StringBuilder sb = new StringBuilder(query);
                    if (labelsInQuery.size() > 1) {
                        outputError(jg, startTimeStamp, "Multiple labels in match", "Type of query not supported by extension. Multiple labels in match.");
                    } else if(labelsInQuery.size() == 0) {
                        outputError(jg, startTimeStamp, "No labels in match", "Type of query not supported by extension. No labels in match. Probably best using the transactional cypher endpoint");
                    }
                    else {
                        //there is only one label so no need to iterate, right? //TODO
                        try (Transaction tx = gds.beginTx()) {
                            for (String labelName : labelsInQuery) {
                                Map<String, Object> params = new HashMap<String, Object>();
                                params.put("catname", labelName);
                                Result subcats = gds.execute("MATCH (:Category { catName: {catname}})<-[:SCO*]-(subcat) RETURN subcat.catName AS subcat",
                                        params);
                                int subcatCount = 0;
                                while (subcats.hasNext()) {
                                    Map<String, Object> result = subcats.next();
                                    String subcat = (String) result.get("subcat");
                                    sb.append(" UNION " + prefix + ":" + subcat + suffix);
                                    subcatCount++;
                                }
                                Result result = gds.execute(sb.toString());

                                serializeResults(result, jg, startTimeStamp, subcatCount);
                            }
                        }
                    }
                } else {
                    outputError(jg, startTimeStamp, "Unsupported query type", "Type of query not supported by extension.");
                }
                jg.flush();
                jg.close();
            }
        }).build();
    }


    private boolean validQuery(String queryPart) {
        // check rest of query does not contain other label usage
        if (Pattern.compile("(?i)^.*\\([^\\{\\)]*:.*\\).*").matcher(queryPart).matches()) {
            return false;
        }
        //or uses the labels function
        if (Pattern.compile("(?i)^.*labels\\s*\\(.*").matcher(queryPart).matches()) {
            return false;
        }
        //other cases to exclude?
        return true;
    }

    private void serializeResults(Result result, JsonGenerator jg, long startTimeStamp, int subcatCount) throws IOException {

        jg.writeStartObject();
        jg.writeFieldName( "results" );
        jg.writeStartArray();
        jg.writeStartObject();
        jg.writeFieldName( "columns" );
        jg.writeStartArray();
        for(String col:result.columns()){
            jg.writeString(col);
        }
        jg.writeEndArray();
        jg.writeFieldName( "data" );
        jg.writeStartArray();
        //here comes the data
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            jg.writeStartObject();
            jg.writeFieldName("row");
            jg.writeStartArray();
            for (String col : result.columns()) {
                Object value = row.get(col);
                if (value instanceof Node) {
                    jg.writeStartObject();
                    Map<String, Object> allProperties = ((Node) value).getAllProperties();
                    for (String fieldName : allProperties.keySet()) {
                        jg.writeFieldName(fieldName);
                        if(allProperties.get(fieldName) instanceof Number){
                            jg.writeNumber(allProperties.get(fieldName).toString());
                        } else{
                            //TODO treat types correctly. Arrays are being stringified!
                            jg.writeString(allProperties.get(fieldName).toString());
                        }
                    }
                    jg.writeEndObject();
                } else {
                    if (value != null) {
                        if(value instanceof Number){
                            jg.writeNumber(value.toString());
                        } else{
                            //TODO treat types correctly. Arrays are being stringified!
                            jg.writeString(value.toString());
                        }
                    } else{
                        jg.writeNull();
                    }
                }
            }
            jg.writeEndArray();
            jg.writeEndObject();
        }
        jg.writeEndArray();
        jg.writeFieldName("stats");
        jg.writeStartObject();
        jg.writeNumberField("subclass_expansion_count", subcatCount);
        jg.writeEndObject();
        jg.writeEndObject();
        jg.writeEndArray();
        jg.writeFieldName("errors");
        jg.writeStartArray();
        jg.writeEndArray();
        jg.writeNumberField("responseTimeMillis",(System.currentTimeMillis() - startTimeStamp));
        jg.writeEndObject();

    }

    private void outputError(JsonGenerator jg, long startTimeStamp, String code, String message) throws IOException {
        jg.writeStartObject();
        jg.writeFieldName( "results" );
        jg.writeStartArray();
        jg.writeEndArray();
        jg.writeFieldName( "errors" );
        jg.writeStartArray();
        jg.writeStartObject();
        jg.writeStringField("code", code);
        jg.writeStringField("message", message);
        jg.writeEndObject();
        jg.writeEndArray();
        jg.writeNumberField("responseTimeMillis",(System.currentTimeMillis() - startTimeStamp) );
        jg.writeEndObject();
    }

    private String removeQuotes(String str) {
        if (str.matches("^(\"|').*(\"|')$")){
            return str.substring(1,str.length()-1);
        }
        return str;
    }

}
