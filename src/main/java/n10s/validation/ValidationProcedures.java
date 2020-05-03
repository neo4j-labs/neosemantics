package n10s.validation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  @Procedure(name= "n10s.validation.shacl.validateTransaction", mode = Mode.READ)
  @Description("n10s.validation.shacl.validateTransaction(createdNodes,createdRelationships,...) - runs SHACL validation in trigger context.")
  public Stream<ValidationResult> shaclValidateTxForTrigger(
      @Name("createdNodes") Object createdNodes,
      @Name("createdRelationships") Object createdRelationships,
      @Name("assignedLabels") Object assignedLabels, @Name("removedLabels") Object removedLabels,
      @Name("assignedNodeProperties") Object assignedNodeProperties,
      @Name("removedNodeProperties") Object removedNodeProperties) {

    //we may need to pass additional params to this method?

    //If there are no shapes compiled there's no point in running the validation
    if(tx.execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN id(vc) as id").hasNext()) {
      Map<String, Object> params = new HashMap<>();
      params.put("createdNodes", createdNodes);
      params.put("createdRelationships", createdRelationships);
      params.put("assignedLabels", assignedLabels);
      params.put("removedLabels", removedLabels);
      params.put("assignedNodeProperties", assignedNodeProperties);
      params.put("removedNodeProperties", removedNodeProperties);

      // TODO: need to add deleted labels and deleted nodes (basically everything but relationship properties)
      // TODO: rethink which  ones are needed   <--
      String newQuery =
          "UNWIND reduce(nodes = [], x IN keys($removedLabels) | nodes + $removedLabels[x]) AS rln "
              + " MATCH (rln)<--(x) WITH collect(DISTINCT x) AS sn "
              //the direction makes it valid for both direct and  inverse
              + " UNWIND sn + $createdNodes + [x IN $createdRelationships | startNode(x)] + [x IN $createdRelationships | endNode(x)] +"
              //end node is also for inverse rels
              + "  reduce( nodes = [] , x IN keys($assignedLabels) | nodes + $assignedLabels[x]) + "
              + "  reduce( nodes = [] , x IN keys($assignedNodeProperties) | nodes + "
              + "  [ item IN $assignedNodeProperties[x] | item.node] ) +"
              + "  reduce( nodes = [] , x IN keys($removedNodeProperties) | nodes + "
              + "  [ item IN $removedNodeProperties[x] | item.node] ) AS nd "
              + " WITH collect( DISTINCT nd) AS touchedNodes\n"
              + "CALL n10s.validation.shacl.validateSet(touchedNodes) YIELD focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage\n"
              + "RETURN {focusNode: focusNode, nodeType: nodeType, shapeId: shapeId, propertyShape: propertyShape, offendingValue: offendingValue, resultPath:resultPath, severity:severity, resultMessage:resultMessage } AS validationResult ";

      Result validationResults = tx.execute(newQuery, params);

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
      throws IOException, RDFImportBadParams, ShapesUsingNamespaceWithUndefinedPrefix, InvalidNamespacePrefixDefinitionInDB {

    return doLoad(format, null, rdfFragment, props).stream();
  }

  @Procedure(name = "n10s.validation.shacl.import.fetch", mode = Mode.WRITE)
  @Description("Imports SHACL shapes from a URL and compiles a validator into neo4j")
  public Stream<ConstraintComponent> importSHACLFromURL(@Name("url") String url, @Name("format") String format,
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
//    params.put("rqg", serialiseObject(validatorConfig.getRunnableQueries(true)));
//    params.put("rqns", serialiseObject(validatorConfig.getRunnableQueries(false)));
    params.put("gq", serialiseObject(validatorConfig.getIndividualGlobalQueries()));
    params.put("nsq", serialiseObject(validatorConfig.getIndividualNodeSetQueries()));
    params.put("tl", serialiseObject(validatorConfig.getTriggerList()));
    params.put("cl", serialiseObject(validatorConfig.getConstraintList()));
    params.put("params", serialiseObject(validatorConfig.getAllParams()));

    tx.execute("MERGE (vc:_n10sValidatorConfig { _id: 1}) "
        + "SET vc._gq = $gq, vc._nsq = $nsq, vc._tl = $tl, vc._params = $params, "
        + " vc._constraintList = $cl ", params);

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

      ValidatorConfig vc = deserialiseValidatorConfig();

      return generateRunnableQueryList(vc, true,null).parallelStream().flatMap(x->tx.execute(x,vc.getAllParams()).stream()).map(ValidationResult::new);

  }

  private ValidatorConfig deserialiseValidatorConfig () throws IOException, ClassNotFoundException {

    Result loadValidatorFromDBResult = tx
        .execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN vc");
    if (!loadValidatorFromDBResult.hasNext()) {
      throw new SHACLValidationException("No shapes compiled");
    } else {

      Node validationConfigNode = (Node) loadValidatorFromDBResult.next().get("vc");
      Map<String, Object> params = (Map<String, Object>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_params"));
      Map<String, String> globalQueries = (Map<String, String>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_gq"));
      Map<String, String> nodeSetQueries = (Map<String, String>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_nsq"));
      Map<String, Set<String>> triggerList = (Map<String, Set<String>>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_tl"));
      //return tx.execute(engineGlobal, params).stream().map(ValidationResult::new);
      return new ValidatorConfig(globalQueries, nodeSetQueries,
          triggerList, params);
    }
  }

    private List<String> generateRunnableQueryList(ValidatorConfig vc, boolean global, List<Node> nodeSet) {

      List<String> labels;

      if(global) {
        labels = (List<String>) tx
            .execute("call db.labels() yield label return collect(label) as labelsInUse").next()
            .get("labelsInUse");
      } else {
        Map<String, Object> params = new HashMap<>();
        params.put("nodeList", nodeSet);
        labels = (List<String>) tx
            .execute("unwind $nodeList as node\n"
                + "with collect(distinct labels(node)) as nodeLabelSet \n"
                + "return reduce(res=[], x in nodeLabelSet | res + x) as fullNodeLabelWithDuplicates", params).next()
            .get("fullNodeLabelWithDuplicates");
      }

      return vc.getRunnableQueries(global,  new HashSet<>(labels));
  }


  @Procedure(name="n10s.validation.shacl.validateSet", mode = Mode.READ)
  @Description("n10s.validation.shacl.validateSet([nodeList]) - runs SHACL validation on selected nodes")
  public Stream<ValidationResult> validateSetFromCompiled(@Name(value = "nodeList", defaultValue = "[]") List<Node> nodeList)
      throws IOException, ClassNotFoundException {


      ValidatorConfig vc = deserialiseValidatorConfig();

      //add touched nodes to params
      vc.getAllParams().put("touchedNodes", nodeList);

      return generateRunnableQueryList(vc,false,nodeList).parallelStream().flatMap(x->tx.execute(x,vc.getAllParams()).stream()).map(ValidationResult::new);

  }

}


