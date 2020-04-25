package n10s.validation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig.InvalidParamException;
import n10s.rdf.RDFProcedures.ImportResults;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.neo4j.cypher.internal.ir.HasHeaders;
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


  @Procedure(name="n10s.validation.shacl.validateSet", mode = Mode.READ)
  @Description("n10s.validation.shacl.validateSet([nodeList]) - runs SHACL validation on selected nodes")
  public Stream<ValidationResult> shaclValidateNodeList(@Name(value = "nodeList", defaultValue = "[]") List<Node> nodeList) {
    if(nodeList.isEmpty()){
      return Stream.empty();
    } else {
      SHACLValidator validator = new SHACLValidator(tx, log);
      //TODO: question: would passing ids be any better??
      return validator.runValidations(nodeList);
    }
  }


  @Procedure(name="n10s.validation.shacl.validate", mode = Mode.READ)
  @Description("n10s.validation.shacl.validate() - runs SHACL validation on the whole graph.")
  public Stream<ValidationResult> shaclValidateOnAllGraph() {
    SHACLValidator validator = new SHACLValidator(tx, log);
    return validator.runValidations(null);
  }

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
                    + "CALL n10s.validation.shacl.validateSet(touchedNodes) YIELD focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage\n"
                    + "RETURN {focusNode: focusNode, nodeType: nodeType, shapeId: shapeId, propertyShape: propertyShape, offendingValue: offendingValue, resultPath:resultPath, severity:severity, resultMessage:resultMessage } AS validationResult ";

    Result validationResults = tx.execute(newQuery, params);
    if (validationResults.hasNext()) {
      throw new SHACLValidationException(validationResults.next().toString());
    }

    return Stream.empty();
  }


  @Procedure(name="n10s.validation.shacl.listShapes", mode = Mode.READ)
  @Description("n10s.validation.listShapes() - list SHACL shapes loaded  in the Graph")
  public Stream<ConstraintComponent> listShapes() {

    SHACLValidator validator = new SHACLValidator(tx, log);
    return tx.execute(validator.getListConstraintsQuery()).stream().map(ConstraintComponent::new);
  }

  @Procedure(name="n10s.validation.shacl.drop", mode = Mode.WRITE)
  @Description("n10s.validation.shacl.drop() - deletes all SHACL shapes loaded in the Graph")
  public Stream<ConstraintComponent> dropShapes(@Name(value = "nodeShapeUri", defaultValue = "") String nodeShapeUri) {
    String DROP_SHAPES;
    Map<String,Object> params = new HashMap<>();
    if(nodeShapeUri==null || nodeShapeUri.equals("")){
      //Delete all node shapes
      DROP_SHAPES =" MATCH path = (:sh__NodeShape)-[*]->()\n"
          + "UNWIND nodes(path) as node DETACH DELETE node ";
    } else {
      params.put("nodeShapeUri",nodeShapeUri);
      DROP_SHAPES =" MATCH path = (root:sh__NodeShape { uri: $nodeShapeUri})-[*]->() where all(node IN nodes(path) WHERE (not node:sh__NodeShape) or (node.uri = $nodeShapeUri)) \n"
          + "UNWIND nodes(path) as node DETACH DELETE node ";
    }


    tx.execute(DROP_SHAPES, params);

    return Stream.empty();
  }


  @Procedure(name = "n10s.validation.shacl.load.inline", mode = Mode.WRITE)
  @Description("Imports an RDF snippet passed as parameter and stores it in Neo4j as a property "
      + "graph. Requires a unique constraint on :Resource(uri)")
  public Stream<ImportResults> loadInlineSHACL(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws InvalidParamException {


    Repository repo = new SailRepository(new MemoryStore());
    try (RepositoryConnection conn = repo.getConnection()) {
      conn.begin();

      Reader shaclRules = new InputStreamReader(new ByteArrayInputStream(rdfFragment.getBytes(Charset.defaultCharset())));
      //getInputStream(url, props) [in CommonProcedures]

      conn.add(shaclRules, "", RDFFormat.TURTLE);
      conn.commit();
      String sparqlQuery= "prefix sh: <http://www.w3.org/ns/shacl#>  \n"
          + "SELECT ?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype "
          + "?severity ?targetClass ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc "
          + "?minStrLen ?maxStrLen ?hasValue (GROUP_CONCAT (?in; separator=\"---\") AS ?ins) "
          + "(isLiteral(?inFirst) as ?isliteralIns)\n"
          + "{ ?ns a sh:NodeShape ;\n"
          + "     sh:property ?ps .\n"
          + "\n"
          + "  optional { ?ps sh:path/sh:inversePath ?invPath }\n"
          + "  optional { ?ps sh:path  ?path }\n"
          + "  optional { ?ps sh:class  ?rangeClass }\n"
          + "  optional { ?ps sh:nodeKind  ?rangeKind }  \n"
          + "  optional { ?ps sh:datatype  ?datatype }\n"
          + "  optional { ?ps sh:severity  ?severity }\n"
          + "  optional { \n"
          + "    { ?ns sh:targetClass  ?targetClass }\n"
          + "    union\n"
          + "    { ?targetClass sh:property ?ps;\n"
          + "          a rdfs:Class . }\n"
          + "  }\n"
          + "  optional { ?ps sh:pattern  ?pattern }\n"
          + "  optional { ?ps sh:maxCount  ?maxCount }\n"
          + "  \n"
          + "    optional { ?ps sh:minCount  ?minCount }\n"
          + "    optional { ?ps sh:minInclusive  ?minInc }\n"
          + "  \n"
          + "    optional { ?ps sh:maxInclusive  ?maxInc }\n"
          + "    optional { ?ps sh:minExclusive  ?minExc }\n"
          + "    optional { ?ps sh:maxExclusive  ?maxExc }  \n"
          + "  optional { ?ps sh:minLength  ?minStrLen }\n"
          + "  \n"
          + "    optional { ?ps sh:minLength  ?minStrLen }\n"
          + "    optional { ?ps sh:maxLength  ?maxStrLen }\n"
          + "  \n"
          + "    optional { ?ps sh:hasValue  ?hasValue } #hasValueUri and hasValueLiteral\n"
          + "  \n"
          + "    optional { ?ps sh:in/rdf:rest*/rdf:first ?in } \n"
          + "    optional { ?ps sh:in/rdf:first ?inFirst }\n"
          + "    optional { ?ps sh:minLength  ?minStrLen }\n"
          + "  \n"
          + "} group by \n"
          + "?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity ?targetClass ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxInc ?maxExc ?minStrLen ?maxStrLen ?hasValue ?inFirst";

      TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
      TupleQueryResult queryResult = tupleQuery.evaluate();
      List<Map<String,Object>>  constraints = new ArrayList<>();
      while(queryResult.hasNext()){
        Map<String,Object> record = new HashMap<>();
        BindingSet next = queryResult.next();
//        System.out.println(next.getBindingNames());
//        [path, ps, ns, datatype, targetClass, minInc, rangeKind, ins]
//        ?ns ?ps ?path ?invPath ?rangeClass  ?rangeKind ?datatype ?severity ?targetClass ?pattern ?maxCount ?minCount ?minInc ?minExc ?maxExc ?minStrLen ?maxStrLen ?hasValue (GROUP_CONCAT (?in; separator="---") AS ?ins) (isLiteral(?inFirst) as ?isliteralIns)


        record.put("item", next.hasBinding("invPath")?next.getValue("invPath").stringValue():next.getValue("path").stringValue());
        record.put("inverse", next.hasBinding("invPath"));
        record.put("appliesToCat", next.getValue("targetClass").stringValue());
        record.put("rangeType", next.hasBinding("rangeClass")?next.getValue("rangeClass").stringValue():null);
        record.put("rangeKind", next.hasBinding("rangeKind")?next.getValue("rangeKind").stringValue():null);
        record.put("dataType", next.hasBinding("datatype")?next.getValue("datatype").stringValue():null);
        record.put("pattern", next.hasBinding("pattern")?next.getValue("pattern").stringValue():null);
        record.put("maxCount", next.hasBinding("maxCount")?((Literal)next.getValue("maxCount")).intValue():null);
        record.put("minCount", next.hasBinding("minCount")?((Literal)next.getValue("minCount")).intValue():null);
        record.put("minInc", next.hasBinding("minInc")?((Literal)next.getValue("minInc")).intValue():null);
        record.put("minExc", next.hasBinding("minExc")?((Literal)next.getValue("minExc")).intValue():null);
        record.put("maxInc", next.hasBinding("maxInc")?((Literal)next.getValue("maxInc")).intValue():null);
        record.put("maxExc", next.hasBinding("maxExc")?((Literal)next.getValue("maxExc")).intValue():null);

        if(next.hasBinding("hasValue")) {
          Value val = next.getValue("hasValue");
          if(val instanceof Literal) {
            record.put("hasValueLiteral", val.stringValue());
          } else {
            record.put("hasValueUri", val.stringValue());
          }
        }

        if(next.hasBinding("isliteralIns")) {
          String[] insArray = next.getValue("ins").stringValue().split("---");
          List<String> inVals = new ArrayList<>();
          int i;
          for (i = 0; i < insArray.length; i++) {
            inVals.add(insArray[i]);
          }
          Literal val = (Literal)next.getValue("isliteralIns");
          if(val.booleanValue()) {
            record.put("hasValueLiteral", inVals);
           } else {
            record.put("hasValueUri", inVals);
          }
        }

        record.put("minStrLen", next.hasBinding("minStrLen")?((Literal)next.getValue("minStrLen")).intValue():null);
        record.put("maxStrLen", next.hasBinding("maxStrLen")?((Literal)next.getValue("maxStrLen")).intValue():null);
        record.put("propShapeUid", next.hasBinding("ps")?next.getValue("ps").stringValue():null);
        record.put("severity", next.hasBinding("severity")?next.getValue("severity").stringValue():"http://www.w3.org/ns/shacl#Violation");

        constraints.add(record);

      }

      SHACLValidator validator = new SHACLValidator(tx, log);
      ValidatorConfig validatorConfig = validator.compileValidations(constraints.iterator());

      Map<String,Object> params =  new HashMap<>();
      params.put("eg", validatorConfig.getEngineGlobal().toString().getBytes());
      params.put("ens", validatorConfig.getEngineForNodeSet().toString().getBytes());


      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream
          = new ObjectOutputStream(baos);
      objectOutputStream.writeObject(validatorConfig.getAllParams());
      objectOutputStream.flush();
      objectOutputStream.close();

      params.put("params", baos.toByteArray());
      tx.execute("MERGE (vc:_n10sValidatorConfig { _id: 1}) SET vc._engineGlobal = $eg, vc._engineForNodeSet = $ens, vc._params = $params ", params);

      return Stream.empty();

    } catch (IOException e) {
      //TODO: deal  with this properly
      e.printStackTrace();
    }

    //TODO: think what this should return
    return Stream.empty();
  }

  @Procedure(name = "n10s.validation.shacl.va", mode = Mode.READ)
  @Description("Runs the SHACL validation previously loaded and compiled")
  public Stream<ValidationResult> validateFromCompiled()
      throws InvalidParamException, IOException, ClassNotFoundException {

    Result loadValidatorFromDBResult = tx.execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN vc");
    if(!loadValidatorFromDBResult.hasNext()) {
      throw new SHACLValidationException("No shapes compiled");
    }
    else {
      Node validationConfigNode = (Node)loadValidatorFromDBResult.next().get("vc");
      byte[] byteArray = (byte[])validationConfigNode.getProperty("_params");

      ObjectInputStream objectInputStream
          = new ObjectInputStream(new ByteArrayInputStream(byteArray));
      Map<String,Object> params = (Map<String,Object>) objectInputStream.readObject();

      objectInputStream.close();

      String engineGlobal = new String((byte[])validationConfigNode.getProperty("_engineGlobal"));

      return tx.execute(engineGlobal, params).stream().map(ValidationResult::new);

    }
  }

}
