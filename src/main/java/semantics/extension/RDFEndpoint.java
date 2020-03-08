package semantics.extension;

import static semantics.config.Params.BASE_INDIV_NS;
import static semantics.config.Params.BASE_VOCAB_NS;
import static semantics.mapping.MappingUtils.getExportMappingsFromDB;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.JSONLDMode;
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import semantics.LPGRDFToRDFProcesssor;
import semantics.LPGToRDFProcesssor;
import semantics.config.GraphConfig;
import semantics.config.GraphConfig.GraphConfigNotFound;

/**
 * Created by jbarrasa on 08/09/2016.
 */
@Path("/n10s")
public class RDFEndpoint {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String DEFAULT_DB_NAME = "neo4j";
  private static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD,
      RDFFormat.TURTLE, RDFFormat.NTRIPLES, RDFFormat.TRIG, RDFFormat.NQUADS};

  @Context
  public Log log;


  @GET
  @Path("/ping")
  public Response ping() throws IOException {
    Map<String, String> results = new HashMap<String, String>() {{
      put("ping", "here!");
    }};
    return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
  }

  @GET
  @Path("{dbname}/describe/{nodeidentifier}")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response nodebyIdOrUri(@Context DatabaseManagementService gds,
                           @PathParam("dbname") String dbNameParam,
                           @PathParam("nodeidentifier") String nodeIdentifier,
                           @QueryParam("graphuri") String namedGraphId,
                           @QueryParam("excludeContext") String excludeContextParam,
                           @QueryParam("mappedElemsOnly") String onlyMappedInfo,
                           @QueryParam("format") String format,
                           @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity((StreamingOutput) outputStream -> {

      RDFWriter writer = startRdfWriter(getFormat(acceptHeaderParam, format), outputStream, false);
      GraphDatabaseService neo4j = gds.database(dbNameParam);
      try (Transaction tx = neo4j.beginTx()) {

        if(getGraphConfig(tx) ==  null) {
          LPGToRDFProcesssor proc = new LPGToRDFProcesssor(neo4j, tx,
              getExportMappingsFromDB(neo4j), onlyMappedInfo != null);

          proc.streamNodeById(Long.parseLong(nodeIdentifier), excludeContextParam == null)
              .forEach(writer::handleStatement);
        } else {
          //it's rdf
          LPGRDFToRDFProcesssor proc = new LPGRDFToRDFProcesssor(neo4j, tx);
          proc.streamNodeByUri(nodeIdentifier, namedGraphId, excludeContextParam != null).forEach(
              writer::handleStatement);
        }
        endRDFWriter(writer);
      } catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam, format);
      }
    }).build();
  }

  private GraphConfig getGraphConfig(Transaction tx) {
    GraphConfig result =  null;
    try {
      result = new GraphConfig(tx);
    } catch (GraphConfigNotFound graphConfigNotFound) {
      //it's an LPG (no RDF import config)
    }
    return result;
  }


  @GET
  @Path("{dbname}/describe/find/{label}/{property}/{propertyValue}")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response nodefind(@Context DatabaseManagementService gds,
                           @PathParam("dbname") String dbNameParam,
                           @PathParam("label") String label,
      @PathParam("property") String property, @PathParam("propertyValue") String propVal,
      @QueryParam("valType") String valType,
      @QueryParam("excludeContext") String excludeContextParam,
      @QueryParam("mappedElemsOnly") String onlyMappedInfo,
      @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity((StreamingOutput) outputStream -> {

      GraphDatabaseService neo4j = gds.database(dbNameParam);
      RDFWriter writer = startRdfWriter(getFormat(acceptHeaderParam, format), outputStream, false);
      try (Transaction tx = neo4j.beginTx()) {

        LPGToRDFProcesssor proc = new LPGToRDFProcesssor(neo4j, tx,
            getExportMappingsFromDB(neo4j), onlyMappedInfo != null);
        proc.streamNodesBySearch(label, property, propVal, valType, excludeContextParam == null)
            .forEach(
                writer::handleStatement);
        endRDFWriter(writer);
      } catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam, format);
      }
    }).build();
  }

  @POST
  @Path("{dbname}/cypher")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response cypher(@Context DatabaseManagementService gds,
                                   @PathParam("dbname") String dbNameParam,
      @HeaderParam("accept") String acceptHeaderParam, String body) {
    return Response.ok().entity((StreamingOutput) outputStream -> {
      Map<String, Object> jsonMap = objectMapper
          .readValue(body,
              new TypeReference<Map<String, Object>>() {
              });
      GraphDatabaseService neo4j = gds.database(dbNameParam);
      try (Transaction tx = neo4j.beginTx()) {
        RDFWriter writer = startRdfWriter(
            getFormat(acceptHeaderParam, (String) jsonMap.get("format")), outputStream, false);

        if(getGraphConfig(tx) == null) {

          LPGToRDFProcesssor proc = new LPGToRDFProcesssor(neo4j, tx,
              getExportMappingsFromDB(neo4j), jsonMap.containsKey("mappedElemsOnly"));
          proc.streamTriplesFromCypher((String) jsonMap.get("cypher"),
              (Map<String, Object>) jsonMap
                  .getOrDefault("cypherParams", new HashMap<String, Object>())).forEach(
              writer::handleStatement);
        } else {
          LPGRDFToRDFProcesssor proc = new LPGRDFToRDFProcesssor(neo4j, tx);
          proc.streamTriplesFromCypher((String) jsonMap.get("cypher"),
              (Map<String, Object>) jsonMap
                  .getOrDefault("cypherParams", new HashMap<String, Object>())).forEach(
              writer::handleStatement);
        }
        endRDFWriter(writer);
      } catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam,
            (String) jsonMap.get("format"));
      }
    }).build();
  }

  @GET
  @Path("{dbname}/onto")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response exportOnto(@Context DatabaseManagementService gds,
                             @PathParam("dbname") String dbNameParam,
                             @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {

    return Response.ok().entity((StreamingOutput) outputStream -> {
      GraphDatabaseService neo4j = gds.database(dbNameParam);
      RDFWriter writer = startRdfWriter(getFormat(acceptHeaderParam, format), outputStream, true);
      try (Transaction tx = neo4j.beginTx()) {

        if(getGraphConfig(tx) == null) {
          LPGToRDFProcesssor proc = new LPGToRDFProcesssor(neo4j, tx);
          proc.streamLocalImplicitOntology().forEach(writer::handleStatement);
        } else {
          LPGRDFToRDFProcesssor proc = new LPGRDFToRDFProcesssor(neo4j, tx);
          proc.streamLocalImplicitOntology().forEach(writer::handleStatement);
        }
        endRDFWriter(writer);
      } catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam, format);
      }
    }).build();
  }

  private RDFWriter startRdfWriter(RDFFormat format, OutputStream os, boolean addVocNamespaces) {
    RDFWriter writer = Rio.createWriter(format, os);
    //some general config (valid for specific serialisations)
    writer.set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);
    writer.set(JSONLDSettings.OPTIMIZE, true);

    writer.handleNamespace("rdf", RDF.NAMESPACE);
    writer.handleNamespace("neovoc", BASE_VOCAB_NS);
    writer.handleNamespace("neoind", BASE_INDIV_NS);
    if (addVocNamespaces) {
      writer.handleNamespace("owl", OWL.NAMESPACE);
      writer.handleNamespace("rdfs", RDFS.NAMESPACE);
    }
    writer.startRDF();
    return writer;
  }

  private void endRDFWriter(RDFWriter writer) {
    writer.endRDF();
  }

  private void handleSerialisationError(OutputStream outputStream, Exception e,
      @HeaderParam("accept") String acceptHeaderParam, @QueryParam("format") String format) {
    //output the error message using the right serialisation
    //TODO: maybe serialise all that can be serialised and just comment the offending triples?
    RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
    writer.startRDF();
    writer.handleComment(e.getMessage());
    writer.endRDF();
  }


  private RDFFormat getFormat(String mimetype, String formatParam) {
    // format request param overrides the one defined in the accept header param
    if (formatParam != null) {
      log.info("serialization from param in request: " + formatParam);
      for (RDFFormat parser : availableParsers) {
        if (parser.getName().contains(formatParam)) {
          log.info("parser to be used: " + parser.getDefaultMIMEType());
          return parser;
        }
      }
    } else {
      if (mimetype != null) {
        log.info("serialization from media type in request: " + mimetype);
        for (RDFFormat parser : availableParsers) {
          if (parser.getMIMETypes().contains(mimetype)) {
            log.info("parser to be used: " + parser.getDefaultMIMEType());
            return parser;
          }
        }
      }
    }

    log.info("Unrecognized or undefined serialization. Defaulting to Turtle serialization");

    return RDFFormat.TURTLE;

  }

}
