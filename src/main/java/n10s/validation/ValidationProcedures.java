package n10s.validation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.GraphConfig.GraphConfigNotFound;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class ValidationProcedures {

  @Context
  public Log log;

  @Context
  public Transaction tx;


  @Procedure(name="n10s.experimental.validation.shaclValidateTx", mode = Mode.READ)
  @Description("n10s.experimental.validation.shaclValidateTx() - runs SHACL validation on selected nodes")
  public Stream<ValidationResult> shaclValidateTx(@Name("nodeList") List<Node> touchedNodes,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    //TODO: check if passing ids is any better
    SHACLValidator validator = new SHACLValidator(tx, log);
    return validator.runValidations(touchedNodes);
  }


  @Procedure(name="n10s.experimental.validation.shaclValidate", mode = Mode.READ)
  @Description("n10s.experimental.validation.shaclValidate() - runs SHACL validation on the whole graph.")
  public Stream<ValidationResult> shacl(
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    SHACLValidator validator = new SHACLValidator(tx, log);
    return validator.runValidations(null);
  }

  @Procedure(name= "n10s.experimental.validation.triggerSHACLValidateTx", mode = Mode.READ)
  @Description("n10s.experimental.validation.triggerSHACLValidateTx() - runs SHACL validation in trigger context.")
  public Stream<ValidationResult> shaclValidateTxForTrigger(
      @Name("createdNodes") Object createdNodes,
      @Name("createdRelationships") Object createdRelationships,
      @Name("assignedLabels") Object assignedLabels, @Name("removedLabels") Object removedLabels,
      @Name("assignedNodeProperties") Object assignedNodeProperties,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    Map<String, Object> params = new HashMap<>();
    params.put("createdNodes", createdNodes);
    params.put("createdRelationships", createdRelationships);
    params.put("assignedLabels", assignedLabels);
    params.put("removedLabels", removedLabels);
    params.put("assignedNodeProperties", assignedNodeProperties);
    Result validationResults = tx.execute(
        "UNWIND reduce(nodes = [], x IN keys($removedLabels) | nodes + $removedLabels[x]) AS rln MATCH (rln)<--(x) WITH collect(DISTINCT x) AS sn UNWIND sn + $createdNodes + [x IN $createdRelationships | startNode(x)] + reduce( nodes = [] , x IN keys($assignedLabels) | nodes + $assignedLabels[x]) + reduce( nodes = [] , x IN keys($assignedNodeProperties) | nodes + [ item IN $assignedNodeProperties[x] | item.node] ) AS nd WITH collect( DISTINCT nd) AS touchedNodes\n"
            + "CALL semantics.validation.shaclValidateTx(touchedNodes) YIELD nodeId, nodeType, shapeId, propertyShape, offendingValue, propertyName\n"
            + "RETURN {nodeId: nodeId, nodeType: nodeType, shapeId: shapeId, propertyShape: propertyShape, offendingValue: offendingValue, propertyName:propertyName} AS validationResult ",
        params);
    if (validationResults.hasNext()) {
      throw new SHACLValidationException(validationResults.next().toString());
    }

    return Stream.empty();
  }


  @Procedure(name="n10s.experimental.validation.listShapes", mode = Mode.READ)
  @Description("n10s.experimental.validation.listShapes() - list SHACL shapes loaded  in the Graph")
  public Stream<ConstraintComponent> listShapes() {

    SHACLValidator validator = new SHACLValidator(tx, log);
    return tx.execute(validator.getListConstraintsQuery()).stream().map(ConstraintComponent::new);
  }

  @Procedure(name="n10s.experimental.validation.dropAllShapes", mode = Mode.WRITE)
  @Description("n10s.experimental.validation.dropAllShapes() - deletes all SHACL shapes loaded in the Graph")
  public Stream<ConstraintComponent> dropAllShapes() {
    String DROP_SHAPES =" MATCH path = (:sh__NodeShape)-[*]->()\n"
        + "UNWIND nodes(path) as node DETACH DELETE node ";

    tx.execute(DROP_SHAPES);

    return Stream.empty();
  }

}
