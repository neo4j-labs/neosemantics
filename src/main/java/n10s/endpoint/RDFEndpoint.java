package n10s.endpoint;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_IGNORE;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_MAP;
import static n10s.graphconfig.Params.BASE_INDIV_NS;
import static n10s.graphconfig.Params.BASE_SCH_NS;
import static n10s.mapping.MappingUtils.*;

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
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import n10s.rdf.export.ExportProcessor;
import n10s.rdf.export.LPGRDFToRDFProcesssor;
import n10s.rdf.export.LPGToRDFProcesssor;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.JSONLDMode;
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

/**
 * Created by jbarrasa on 08/09/2016.
 *
 *  Imported RDF ( graph config present )
 *    * ignore -> apply default schema namespacing and use ids prefixed with default base for uris
 *    * map -> apply mapping when present and for unmapped elements apply 'ignore' logic
 *    * shorten -> namespaces present (apply)
 *    * keep -> no namespaces present. Just serialise (eventually generate them dynamically)
 *
 *  Not imported RDF (no GraphConfig)
 *    * apply default schema namespacing and use ids prefixed with default base for uris
 *      [behavior is very similar to the 'IGNORE']
 *
 *
 *  Mixed cases. Imported RDF (ignore or map) on existing graph: Some nodes will have uris, others won't
 *
 *
 *  Requests by uri? or by nodeid? maybe always by uri. in case of not imported, still generate it with the prefix on
 *  the requester side?
 *
 *
 */
@Path("/")
public class RDFEndpoint {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String DEFAULT_DB_NAME = "neo4j";
  private static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD,
      RDFFormat.TURTLE, RDFFormat.NTRIPLES, RDFFormat.TRIG, RDFFormat.NQUADS, RDFFormat.TURTLESTAR,
      RDFFormat.TRIGSTAR};

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
  @Path("/{dbname}/describe/{nodeidentifier}")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads", "text/x-turtlestar",
      "application/x-trigstar"})
  public Response nodebyIdOrUri(@Context DatabaseManagementService gds,
      @PathParam("dbname") String dbNameParam,
      @PathParam("nodeidentifier") String nodeIdentifier,
      @QueryParam("graphuri") String namedGraphId,
      @QueryParam("excludeContext") String excludeContextParam,
      @QueryParam("mappedElemsOnly") String onlyMappedInfo,
      @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity((StreamingOutput) outputStream -> {

      RDFWriter writer = startRdfWriter(getFormat(acceptHeaderParam, format), outputStream);
      GraphDatabaseService neo4j = gds.database(dbNameParam);
      try (Transaction tx = neo4j.beginTx()) {

        GraphConfig gc = getGraphConfig(tx);

        if ( gc == null || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_IGNORE
                || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP) {
          getPrefixesFromMappingDefinitions(neo4j).forEach( (pref,ns) -> writer.handleNamespace(pref,ns));
          writer.handleNamespace("n4sch", BASE_SCH_NS);
          if (gc == null) {
            // needed to serialise nodes without uri -> base + nodeid
            writer.handleNamespace("n4ind", BASE_INDIV_NS);
          }

          LPGToRDFProcesssor proc = new LPGToRDFProcesssor(neo4j, tx, gc,
              getExportMappingsFromDB(neo4j), onlyMappedInfo != null,
              isRdfStarSerialisation(writer.getRDFFormat()));
          try{
            long nodeid = Long.parseLong(nodeIdentifier);
            proc.streamNodeById(nodeid, excludeContextParam == null)
                    .forEach(writer::handleStatement);
          } catch(NumberFormatException e){
            //it's a uri
            proc.streamNodeByUri(nodeIdentifier, excludeContextParam == null)
                    .forEach(writer::handleStatement);
          }
        } else {
          //it's rdf
          getPrefixesInUse(neo4j).forEach( (pref,ns) -> writer.handleNamespace(pref,ns));
          LPGRDFToRDFProcesssor proc = new LPGRDFToRDFProcesssor(neo4j, tx, gc, isRdfStarSerialisation(writer.getRDFFormat()));
          proc.streamNodeByUri(nodeIdentifier, namedGraphId, excludeContextParam != null).forEach(
                  writer::handleStatement);
        }

        endRDFWriter(writer);
      } catch (NotFoundException e) {
        //Node not found. Not an error, just return empty RDF fragment
        writer.endRDF();
      }catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam, format);
      }
    }).build();
  }

  private GraphConfig getGraphConfig(Transaction tx) {
    GraphConfig result = null;
    try {
      result = new GraphConfig(tx);
    } catch (GraphConfigNotFound graphConfigNotFound) {
      //it's an LPG (no RDF import config)
    }
    return result;
  }


  @GET
  @Path("/{dbname}/describe/find/{label}/{property}/{propertyValue}")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads", "text/x-turtlestar",
      "application/x-trigstar"})
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

      RDFWriter writer = startRdfWriter(getFormat(acceptHeaderParam, format), outputStream);
      GraphDatabaseService neo4j = gds.database(dbNameParam);
      try (Transaction tx = neo4j.beginTx()) {

        GraphConfig gc = getGraphConfig(tx);
        ExportProcessor proc;
        if ( gc == null || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_IGNORE
                || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP) {
          getPrefixesFromMappingDefinitions(neo4j).forEach((pref, ns) -> writer.handleNamespace(pref, ns));
          writer.handleNamespace("n4sch", BASE_SCH_NS);
          if (gc == null) {
            // needed to serialise nodes without uri -> base + nodeid
            writer.handleNamespace("n4ind", BASE_INDIV_NS);
          }

          proc = new LPGToRDFProcesssor(neo4j, tx, gc,
                  getExportMappingsFromDB(neo4j), onlyMappedInfo != null,
                  isRdfStarSerialisation(writer.getRDFFormat()));
        } else {
          getPrefixesFromMappingDefinitions(neo4j).forEach((pref, ns) -> writer.handleNamespace(pref, ns));
          proc = new LPGRDFToRDFProcesssor(neo4j, tx, gc, isRdfStarSerialisation(writer.getRDFFormat()));
        }
        proc.streamNodesBySearch(label, property, propVal, valType, excludeContextParam == null)
                .forEach(writer::handleStatement);
        endRDFWriter(writer);
      } catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam, format);
      }
    }).build();
  }


  @POST
  @Path("/{dbname}/cypher")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads", "text/x-turtlestar",
      "application/x-trigstar"})
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
            getFormat(acceptHeaderParam, (String) jsonMap.get("format")), outputStream);

        GraphConfig gc = getGraphConfig(tx);
        ExportProcessor proc;
        if (gc == null || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_IGNORE
            || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP) {
          getPrefixesFromMappingDefinitions(neo4j).forEach((pref, ns) -> writer.handleNamespace(pref, ns));
          writer.handleNamespace("n4sch", BASE_SCH_NS);
          if (gc == null) {
            // needed to serialise nodes without uri -> base + nodeid
            writer.handleNamespace("n4ind", BASE_INDIV_NS);
          }
          proc = new LPGToRDFProcesssor(neo4j, tx, gc,
              getExportMappingsFromDB(neo4j), jsonMap.containsKey("mappedElemsOnly"),
              isRdfStarSerialisation(writer.getRDFFormat()));
        } else {
          getPrefixesInUse(neo4j).forEach((pref, ns) -> writer.handleNamespace(pref, ns));
          proc = new LPGRDFToRDFProcesssor(neo4j, tx, gc,
              isRdfStarSerialisation(writer.getRDFFormat()));
        }
        proc.streamTriplesFromCypher((String) jsonMap.get("cypher"),
                (Map<String, Object>) jsonMap
                        .getOrDefault("cypherParams", new HashMap<String, Object>())).forEach(
                writer::handleStatement);
        endRDFWriter(writer);
      } catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam,
            (String) jsonMap.get("format"));
      }
    }).build();
  }

  private boolean isRdfStarSerialisation(RDFFormat rdfFormat) {
    return rdfFormat.equals(RDFFormat.TURTLESTAR) ||  rdfFormat.equals(RDFFormat.TRIGSTAR);
  }

  @GET
  @Path("/{dbname}/onto")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads", "text/x-turtlestar",
      "application/x-trigstar"})
  public Response exportOnto(@Context DatabaseManagementService gds,
      @PathParam("dbname") String dbNameParam,
      @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {

    return Response.ok().entity((StreamingOutput) outputStream -> {
      GraphDatabaseService neo4j = gds.database(dbNameParam);
      RDFWriter writer = startRdfWriter(getFormat(acceptHeaderParam, format), outputStream);
      //Needed to stream the non-explicit ontology
      writer.handleNamespace("owl", OWL.NAMESPACE);
      writer.handleNamespace("rdfs", RDFS.NAMESPACE);
      try (Transaction tx = neo4j.beginTx()) {
        GraphConfig gc = getGraphConfig(tx);
        ExportProcessor proc;
        if ( gc == null || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_IGNORE
            || gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP) {
          proc = new LPGToRDFProcesssor(neo4j, tx, gc, null, false, false);
        } else {
          proc = new LPGRDFToRDFProcesssor(neo4j, tx, gc,
              isRdfStarSerialisation(writer.getRDFFormat()));
        }
        proc.streamLocalImplicitOntology().forEach(writer::handleStatement);

        endRDFWriter(writer);
      } catch (Exception e) {
        handleSerialisationError(outputStream, e, acceptHeaderParam, format);
      }
    }).build();
  }


  private RDFWriter startRdfWriter(RDFFormat format, OutputStream os) {
    RDFWriter writer = Rio.createWriter(format, os);
    //some general config (valid for specific serialisations)
    writer.set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);
    writer.set(JSONLDSettings.OPTIMIZE, true);
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
      log.debug("serialization from param in request: " + formatParam);
      for (RDFFormat parser : availableParsers) {
        if (parser.getName().contains(formatParam)) {
          log.debug("parser to be used: " + parser.getDefaultMIMEType());
          return parser;
        }
      }
    } else {
      if (mimetype != null) {
        log.debug("serialization from media type in request: " + mimetype);
        for (RDFFormat parser : availableParsers) {
          if (parser.getMIMETypes().contains(mimetype)) {
            log.debug("parser to be used: " + parser.getDefaultMIMEType());
            return parser;
          }
        }
      }
    }

    log.debug("Unrecognized or undefined serialization. Defaulting to Turtle serialization");

    return RDFFormat.TURTLE;

  }

}
