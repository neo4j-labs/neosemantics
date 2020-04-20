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


  @Procedure(name="n10s.experimental.validation.shacl.validateSet", mode = Mode.READ)
  @Description("n10s.experimental.validation.shacl.validateSet([nodeList]) - runs SHACL validation on selected nodes")
  public Stream<ValidationResult> shaclValidateNodeList(@Name(value = "nodeList", defaultValue = "[]") List<Node> nodeList) {
    if(nodeList.isEmpty()){
      return Stream.empty();
    } else {
      SHACLValidator validator = new SHACLValidator(tx, log);
      //TODO: question: would passing ids be any better??
      return validator.runValidations(nodeList);
    }
  }


  @Procedure(name="n10s.experimental.validation.shacl.validate", mode = Mode.READ)
  @Description("n10s.experimental.validation.shacl.validate() - runs SHACL validation on the whole graph.")
  public Stream<ValidationResult> shaclValidateOnAllGraph() {
    SHACLValidator validator = new SHACLValidator(tx, log);
    return validator.runValidations(null);
  }

  @Procedure(name= "n10s.experimental.validation.shacl.validateTransaction", mode = Mode.READ)
  @Description("n10s.experimental.validation.shacl.validateTransaction(createdNodes,createdRelationships,...) - runs SHACL validation in trigger context.")
  public Stream<ValidationResult> shaclValidateTxForTrigger(
      @Name("createdNodes") Object createdNodes,
      @Name("createdRelationships") Object createdRelationships,
      @Name("assignedLabels") Object assignedLabels, @Name("removedLabels") Object removedLabels,
      @Name("assignedNodeProperties") Object assignedNodeProperties,
      @Name("removedNodeProperties") Object removedNodeProperties) {

    //we may need to pass additional params to this method?

    Map<String, Object> params = new HashMap<>();
    params.put("createdNodes", createdNodes);
    params.put("createdRelationships", createdRelationships);
    params.put("assignedLabels", assignedLabels);
    params.put("removedLabels", removedLabels);
    params.put("assignedNodeProperties", assignedNodeProperties);
    params.put("removedNodeProperties", removedNodeProperties);

    //removing a label cannot  make any constraint fail.  //TODO: HERE
    String newQuery = "UNWIND reduce(nodes = [], x IN keys($removedLabels) | nodes + $removedLabels[x]) AS rln "
                    + " MATCH (rln)<--(x) WITH collect(DISTINCT x) AS sn " //the direction makes it valid for both direct and  inverse
                    + " UNWIND sn + $createdNodes + [x IN $createdRelationships | startNode(x)] + [x IN $createdRelationships | endNode(x)] +" //end node is also for inverse rels
                    + "  reduce( nodes = [] , x IN keys($assignedLabels) | nodes + $assignedLabels[x]) + "
                    + "  reduce( nodes = [] , x IN keys($assignedNodeProperties) | nodes + "
                    + "  [ item IN $assignedNodeProperties[x] | item.node] ) +"
                    + "  reduce( nodes = [] , x IN keys($removedNodeProperties) | nodes + "
                    + "  [ item IN $removedNodeProperties[x] | item.node] ) AS nd "  //removed properties can cause violations too
                    + " WITH collect( DISTINCT nd) AS touchedNodes\n"
                    + "CALL n10s.experimental.validation.shacl.validateSet(touchedNodes) YIELD focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage\n"
                    + "RETURN {focusNode: focusNode, nodeType: nodeType, shapeId: shapeId, propertyShape: propertyShape, offendingValue: offendingValue, resultPath:resultPath, severity:severity, resultMessage:resultMessage } AS validationResult ";

    Result validationResults = tx.execute(newQuery, params);
    if (validationResults.hasNext()) {
      throw new SHACLValidationException(validationResults.next().toString());
    }

    return Stream.empty();
  }


  @Procedure(name="n10s.experimental.validation.shacl.listShapes", mode = Mode.READ)
  @Description("n10s.experimental.validation.listShapes() - list SHACL shapes loaded  in the Graph")
  public Stream<ConstraintComponent> listShapes() {

    SHACLValidator validator = new SHACLValidator(tx, log);
    return tx.execute(validator.getListConstraintsQuery()).stream().map(ConstraintComponent::new);
  }

  @Procedure(name="n10s.experimental.validation.shacl.dropAllShapes", mode = Mode.WRITE)
  @Description("n10s.experimental.validation.dropAllShapes() - deletes all SHACL shapes loaded in the Graph")
  public Stream<ConstraintComponent> dropAllShapes() {
    String DROP_SHAPES =" MATCH path = (:sh__NodeShape)-[*]->()\n"
        + "UNWIND nodes(path) as node DETACH DELETE node ";

    tx.execute(DROP_SHAPES);

    return Stream.empty();
  }

}
