package n10s.validation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import n10s.CommonProcedures;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.UriUtils.UriNamespaceHasNoAssociatedPrefix;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class ValidationProcedures extends CommonProcedures {

  @Procedure(name = "n10s.validation.shacl.validateTransaction", mode = Mode.READ)
  @Description("n10s.validation.shacl.validateTransaction(createdNodes,createdRelationships,...) - runs SHACL validation in trigger context.")
  public Stream<ValidationResult> shaclValidateTxForTrigger(
      @Name("createdNodes") Object createdNodes,
      @Name("createdRelationships") Object createdRelationships,
      @Name("assignedLabels") Object assignedLabels, @Name("removedLabels") Object removedLabels,
      @Name("assignedNodeProperties") Object assignedNodeProperties,
      @Name("removedNodeProperties") Object removedNodeProperties,
      @Name("deletedRelationships") Object deletedRelationships,
      @Name("deletedNodes") Object deletedNodes) {

    //we may want to add additional params to this method like the max duration of the validation?

    if (tx.execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN id(vc) as id").hasNext()) {
      Map<String, Object> params = new HashMap<>();
      params.put("createdNodes", createdNodes);
      params.put("createdRelationships", createdRelationships);
      params.put("assignedLabels", assignedLabels);
      params.put("removedLabels", removedLabels);
      params.put("assignedNodeProperties", assignedNodeProperties);
      params.put("removedNodeProperties", removedNodeProperties);
      params.put("deletedRelationships", deletedRelationships);
      params.put("deletedNodes", deletedNodes);

      Result validationResults = tx.execute(MAP_APOC_TRIGGER_PARAMS_TO_VALIDATION, params);

      if (validationResults.hasNext()) {
        throw new SHACLValidationException(validationResults.next().toString());
      }
    }

    return Stream.empty();
  }

  @Procedure(name = "n10s.validation.shacl.import.inline", mode = Mode.WRITE)
  @Description("Imports a SHACL shapes snippet passed as parameter and compiles a validator into neo4j")
  public Stream<ConstraintComponent> importInlineSHACL(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws IOException, RDFImportBadParams, InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {

    return doLoad(format, null, rdfFragment, props).stream();
  }

  @Procedure(name = "n10s.validation.shacl.import.fetch", mode = Mode.WRITE)
  @Description("Imports SHACL shapes from a URL and compiles a validator into neo4j")
  public Stream<ConstraintComponent> importSHACLFromURL(@Name("url") String url,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws IOException, RDFImportBadParams, InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {

    return doLoad(format, url, null, props).stream();

  }

  private List<ConstraintComponent> doLoad(String format, String url, String rdfFragment,
      Map<String, Object> props)
      throws IOException, RDFImportBadParams, InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {

    InputStream is;
    if (rdfFragment != null) {
      is = new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset()));
    } else {
      is = getInputStream(url, props);
    }

    SHACLValidator validator = new SHACLValidator(tx, log);
    ValidatorConfig validatorConfig = validator
        .compileValidations(validator.parseConstraints(is, getFormat(format), props));

    validatorConfig.writeToDB(tx);

    return validatorConfig.getConstraintList();
  }


  @Procedure(name = "n10s.validation.shacl.listShapes", mode = Mode.READ)
  @Description("n10s.validation.listShapes() - list SHACL shapes loaded in the Graph")
  public Stream<ConstraintComponent> listShapes() throws IOException, ClassNotFoundException {

    return new ValidatorConfig(tx).getConstraintList().stream();
  }

  @Procedure(name = "n10s.validation.shacl.dropShapes", mode = Mode.WRITE)
  @Description("n10s.validation.dropShapes() - list SHACL shapes loaded in the Graph")
  public Stream<ConstraintComponent> dropShapes() throws IOException, ClassNotFoundException {

    tx.execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) DELETE vc ");

    return Stream.empty();
  }


  @Procedure(name = "n10s.validation.shacl.validate", mode = Mode.READ)
  @Description("n10s.validation.shacl.validate() - runs SHACL validation on the whole graph.")
  public Stream<ValidationResult> validateFromCompiled()
      throws IOException, ClassNotFoundException {

    ValidatorConfig vc = new ValidatorConfig(tx);
    return vc.generateRunnableQueries(tx, true, null).parallelStream()
        .flatMap(x -> tx.execute(x, vc.getAllParams()).stream()).map(ValidationResult::new);

  }


  @Procedure(name = "n10s.validation.shacl.validateSet", mode = Mode.READ)
  @Description("n10s.validation.shacl.validateSet([nodeList]) - runs SHACL validation on selected nodes")
  public Stream<ValidationResult> validateSetFromCompiled(
      @Name(value = "nodeList", defaultValue = "[]") List<Node> nodeList)
      throws IOException, ClassNotFoundException {

    ValidatorConfig vc = new ValidatorConfig(tx);

    //add touched nodes to params
    vc.getAllParams().put("touchedNodes", nodeList);

    return vc.generateRunnableQueries(tx, false, nodeList).parallelStream()
        .flatMap(x -> tx.execute(x, vc.getAllParams()).stream()).map(ValidationResult::new);

  }
  
  private static final String MAP_APOC_TRIGGER_PARAMS_TO_VALIDATION =
      "UNWIND reduce(nodes = [], x IN keys($removedLabels) | nodes + $removedLabels[x]) AS rln "
          + " MATCH (rln)<--(x) WITH collect(DISTINCT x) AS sn "
          //the direction makes it valid for both direct and  inverse
          + " UNWIND sn + $createdNodes + [x IN $createdRelationships | startNode(x)] + [x IN $createdRelationships | endNode(x)] + " +
              " [x IN $deletedRelationships | startNode(x)] + [x IN $deletedRelationships | endNode(x)] +"
          //end node is also for inverse rels
          + "  reduce( nodes = [] , x IN keys($assignedLabels) | nodes + $assignedLabels[x]) + "
          + "  reduce( nodes = [] , x IN keys($assignedNodeProperties) | nodes + "
          + "  [ item IN $assignedNodeProperties[x] | item.node] ) +"
          + "  reduce( nodes = [] , x IN keys($removedNodeProperties) | nodes + "
          + "  [ item IN $removedNodeProperties[x] | item.node] ) AS nd "
          + " WITH reduce (minus = [], x in collect( DISTINCT nd) |  case when not (x in $deletedNodes) then minus + x else minus end )  AS touchedNodes\n"
          //+ " WITH apoc.coll.subtract(collect( DISTINCT nd), $deletedNodes) AS touchedNodes\n"
          + "CALL n10s.validation.shacl.validateSet(touchedNodes) YIELD focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage\n"
          + "RETURN {focusNode: focusNode, nodeType: nodeType, shapeId: shapeId, propertyShape: propertyShape, offendingValue: offendingValue, resultPath:resultPath, severity:severity, resultMessage:resultMessage } AS validationResult ";


}


