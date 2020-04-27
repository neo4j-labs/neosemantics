package n10s.validation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import n10s.CommonProcedures;
import n10s.graphconfig.GraphConfig.InvalidParamException;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.validation.SHACLValidator.ShapesUsingNamespaceWithUndefinedPrefix;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class ValidationProcedures extends CommonProcedures {

//  @Procedure(name="n10s.validation.shacl.validateSet", mode = Mode.READ)
//  @Description("n10s.validation.shacl.validateSet([nodeList]) - runs SHACL validation on selected nodes")
//  public Stream<ValidationResult> shaclValidateNodeList(@Name(value = "nodeList", defaultValue = "[]") List<Node> nodeList)
//      throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {
//    if(nodeList.isEmpty()){
//      return Stream.empty();
//    } else {
//      SHACLValidator validator = new SHACLValidator(tx, log);
//      //TODO: question: would passing ids be any better??
//      return validator.runValidations(nodeList);
//    }
//  }


//  @Procedure(name="n10s.validation.shacl.validate", mode = Mode.READ)
//  @Description("n10s.validation.shacl.validate() - runs SHACL validation on the whole graph.")
//  public Stream<ValidationResult> shaclValidateOnAllGraph()
//      throws ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {
//    SHACLValidator validator = new SHACLValidator(tx, log);
//    return validator.runValidations(null);
//  }

  @Procedure(name= "n10s.validation.shacl.validateTransaction", mode = Mode.READ)
  @Description("n10s.validation.shacl.validateTransaction(createdNodes,createdRelationships,...) - runs SHACL validation in trigger context.")
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

    //TODO: removing a label cannot  make any constraint fail, no?? -> check
    String newQuery = "UNWIND reduce(nodes = [], x IN keys($removedLabels) | nodes + $removedLabels[x]) AS rln "
                    + " MATCH (rln)<--(x) WITH collect(DISTINCT x) AS sn " //the direction makes it valid for both direct and  inverse
                    + " UNWIND sn + $createdNodes + [x IN $createdRelationships | startNode(x)] + [x IN $createdRelationships | endNode(x)] +" //end node is also for inverse rels
                    + "  reduce( nodes = [] , x IN keys($assignedLabels) | nodes + $assignedLabels[x]) + "
                    + "  reduce( nodes = [] , x IN keys($assignedNodeProperties) | nodes + "
                    + "  [ item IN $assignedNodeProperties[x] | item.node] ) +"
                    + "  reduce( nodes = [] , x IN keys($removedNodeProperties) | nodes + "
                    + "  [ item IN $removedNodeProperties[x] | item.node] ) AS nd "  //removed properties can cause violations too
                    + " WITH collect( DISTINCT nd) AS touchedNodes\n"
                    + "CALL n10s.validation.shacl.validateSet(touchedNodes) YIELD focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage\n"
                    + "RETURN {focusNode: focusNode, nodeType: nodeType, shapeId: shapeId, propertyShape: propertyShape, offendingValue: offendingValue, resultPath:resultPath, severity:severity, resultMessage:resultMessage } AS validationResult ";

    Result validationResults = tx.execute(newQuery, params);
    if (validationResults.hasNext()) {
      throw new SHACLValidationException(validationResults.next().toString());
    }

    return Stream.empty();
  }

  @Procedure(name = "n10s.validation.shacl.import.inline", mode = Mode.WRITE)
  @Description("Imports a SHACL shapes snippet passed as parameter and compiles a validator into neo4j")
  public Stream<ConstraintComponent> importInlineSHACL(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws IOException, RDFImportBadParams, ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

    return doLoad(format, null, rdfFragment, props).stream();
  }

  @Procedure(name = "n10s.validation.shacl.import.fetch", mode = Mode.WRITE)
  @Description("Imports SHACL shapes from a URL and compiles a validator into neo4j")
  public Stream<ConstraintComponent> importSHACLFromURl(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws IOException, RDFImportBadParams, ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

    return doLoad(format, url, null, props).stream();

  }

  private List<ConstraintComponent> doLoad(String format, String url, String rdfFragment, Map<String, Object> props)
      throws IOException, RDFImportBadParams, ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

    InputStream is;
    if (rdfFragment != null) {
      is = new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset()));
    } else {
      is = getInputStream(url, props);
    }

    SHACLValidator validator = new SHACLValidator(tx, log);
    ValidatorConfig validatorConfig = validator.compileValidations(validator.parseConstraints(is, getFormat(format)));

    Map<String,Object> params =  new HashMap<>();
    params.put("eg", validatorConfig.getEngineGlobal().toString().getBytes(Charset.defaultCharset()));
    params.put("ens", validatorConfig.getEngineForNodeSet().toString().getBytes(Charset.defaultCharset()));
    params.put("cl", serialiseObject(validatorConfig.getConstraintList()));
    params.put("params", serialiseObject(validatorConfig.getAllParams()));

    tx.execute("MERGE (vc:_n10sValidatorConfig { _id: 1}) "
        + "SET vc._engineGlobal = $eg, vc._engineForNodeSet = $ens, vc._params = $params, "
        + " vc._constraintList = $cl  ", params);

    return validatorConfig.getConstraintList();
  }

  private byte[] serialiseObject(Object o) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream
        = new ObjectOutputStream(baos);
    objectOutputStream.writeObject(o);
    objectOutputStream.flush();
    objectOutputStream.close();
    return baos.toByteArray();
  }

  private Object deserialiseObject(byte[] bytes) throws IOException, ClassNotFoundException {
    ObjectInputStream objectInputStream
        = new ObjectInputStream(new ByteArrayInputStream(bytes));
    return objectInputStream.readObject();
    //objectInputStream.close();
  }


  @Procedure(name="n10s.validation.shacl.validateSet", mode = Mode.READ)
  @Description("n10s.validation.shacl.validateSet([nodeList]) - runs SHACL validation on selected nodes")
  public Stream<ValidationResult> validateSetFromCompiled(@Name(value = "nodeList", defaultValue = "[]") List<Node> nodeList)
      throws IOException, ClassNotFoundException {


    Result loadValidatorFromDBResult = tx.execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN vc");
    if(!loadValidatorFromDBResult.hasNext()) {
      throw new SHACLValidationException("No shapes compiled");
    }
    else {
      Node validationConfigNode = (Node)loadValidatorFromDBResult.next().get("vc");
      Map<String,Object> params = (Map<String,Object>) deserialiseObject((byte[])validationConfigNode.getProperty("_params"));
      params.put("touchedNodes", nodeList);
      String engineForNodeSet = new String((byte[])validationConfigNode.getProperty("_engineForNodeSet"));
      return tx.execute(engineForNodeSet, params).stream().map(ValidationResult::new);

    }
  }

  @Procedure(name="n10s.validation.shacl.listShapes", mode = Mode.READ)
  @Description("n10s.validation.listShapes() - list SHACL shapes loaded in the Graph")
  public Stream<ConstraintComponent> listShapes() throws IOException, ClassNotFoundException {

    Result loadValidatorFromDBResult = tx.execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN vc");
    if(!loadValidatorFromDBResult.hasNext()) {
      throw new SHACLValidationException("No shapes compiled");
    }
    else {
      Node validationConfigNode = (Node)loadValidatorFromDBResult.next().get("vc");
      List<ConstraintComponent> ccList = (List<ConstraintComponent>)
          deserialiseObject((byte[])validationConfigNode.getProperty("_constraintList"));

      return ccList.stream();

    }
  }


  @Procedure(name="n10s.validation.shacl.validate", mode = Mode.READ)
  @Description("n10s.validation.shacl.validate() - runs SHACL validation on the whole graph.")
  public Stream<ValidationResult> validateFromCompiled()
      throws IOException, ClassNotFoundException {

    Result loadValidatorFromDBResult = tx.execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN vc");
    if(!loadValidatorFromDBResult.hasNext()) {
      throw new SHACLValidationException("No shapes compiled");
    }
    else {

      Node validationConfigNode = (Node)loadValidatorFromDBResult.next().get("vc");
      Map<String,Object> params = (Map<String,Object>) deserialiseObject((byte[])validationConfigNode.getProperty("_params"));
      String engineGlobal = new String((byte[])validationConfigNode.getProperty("_engineGlobal"));
      return tx.execute(engineGlobal, params).stream().map(ValidationResult::new);

    }
  }

}
