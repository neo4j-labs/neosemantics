package n10s.validation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class ValidatorConfig {

  private static final int UNION_BATCH_SIZE = 4;

  private final Map<String, Object> allParams;
  private final List<ConstraintComponent> constraintList;
  private final Map<String, String>  individualGlobalQueries;
  private final Map<String, String>  individualNodeSetQueries;
  private final Map<String, Set<String>> triggerList;

  public ValidatorConfig(){

    this.allParams = new HashMap<>();

    this.constraintList = new ArrayList<>();

    this.individualGlobalQueries = new HashMap<>();

    this.individualNodeSetQueries = new HashMap<>();

    this.triggerList = new HashMap<>();

  }

  public ValidatorConfig(Map<String, String> globalQueries, Map<String, String> nodeSetQueries, Map<String, Set<String>> triggerList, Map<String, Object> params) {
    this.individualGlobalQueries = globalQueries;
    this.individualNodeSetQueries = nodeSetQueries;
    this.triggerList = triggerList;
    this.allParams = params;
    this.constraintList = null;
  }

  public ValidatorConfig(Transaction tx) throws IOException, ClassNotFoundException {
    Result loadValidatorFromDBResult = tx
        .execute("MATCH (vc:_n10sValidatorConfig { _id: 1}) RETURN vc");
    if (!loadValidatorFromDBResult.hasNext()) {
      throw new SHACLValidationException("No shapes compiled");
    } else {

      Node validationConfigNode = (Node) loadValidatorFromDBResult.next().get("vc");

      this.individualGlobalQueries = (Map<String, String>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_gq"));
      this.individualNodeSetQueries = (Map<String, String>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_nsq"));
      this.triggerList = (Map<String, Set<String>>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_tl"));
      this.allParams = (Map<String, Object>) deserialiseObject(
          (byte[]) validationConfigNode.getProperty("_params"));
      this.constraintList = (List<ConstraintComponent>)
          deserialiseObject((byte[])validationConfigNode.getProperty("_constraintList"));
    }
  }

  public Map<String, Object> getAllParams() { return allParams; }

  public List<ConstraintComponent> getConstraintList() { return  constraintList; }

  public Map<String,String> getIndividualGlobalQueries(){  return individualGlobalQueries; }

  public Map<String,String> getIndividualNodeSetQueries(){  return individualNodeSetQueries; }

  public Map<String,Set<String>> getTriggerList(){  return triggerList; }

  public void addConstraintToList(ConstraintComponent cc) {
    constraintList.add(cc);
  }

  public void addQueryAndTriggers(String queryId, String queryGlobal, String queryOnNodeSet,
      List<String> triggers){
    individualGlobalQueries.put(queryId,queryGlobal);
    individualNodeSetQueries.put(queryId,queryOnNodeSet);
    for(String trigger:triggers){
      if(triggerList.containsKey(trigger)) {
        triggerList.get(trigger).add(queryId);
      } else {
        Set<String> queryIdSet = new HashSet<>();
        queryIdSet.add(queryId);
        triggerList.put(trigger, queryIdSet);
      }
    }
  }

  public List<String> selectQueriesAndBatchFromTriggerList(boolean global, Set<String> triggerers) {
    final Set<String> queries = new HashSet<>();

    Set<String> queryIds = new HashSet<>();
    for (String triggerer:triggerers) {
      Set<String> querySet = triggerList.get(triggerer);
      if (querySet!= null && querySet.size()>0){
        queryIds.addAll(querySet);
      }
    }

    queryIds.forEach(x -> queries.add(global?individualGlobalQueries.get(x):individualNodeSetQueries.get(x)));

    List<String> runnableQueries = new ArrayList<>();
    int i = 0;
    StringBuilder sb = newInitialisedStringBuilder();
    for (String q :queries) {
      sb.append("\n UNION \n").append(q);
      i++;
      if (i >= UNION_BATCH_SIZE){
        runnableQueries.add(sb.toString());
        i = 0;
        sb = newInitialisedStringBuilder();
      }
    }
    if(sb.length()>0){
      runnableQueries.add(sb.toString());
    }
    return runnableQueries;
  }

  public List<String> generateRunnableQueries(Transaction tx, boolean global, List<Node> nodeSet) {

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

    return selectQueriesAndBatchFromTriggerList(global,  new HashSet<>(labels));
  }

  private StringBuilder newInitialisedStringBuilder() {
    return new StringBuilder().append("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity , '' as message ");
  }

  public void writeToDB(Transaction tx) throws IOException {
    Map<String,Object> params =  new HashMap<>();
    params.put("gq", serialiseObject(individualGlobalQueries));
    params.put("nsq", serialiseObject(individualNodeSetQueries));
    params.put("tl", serialiseObject(triggerList));
    params.put("cl", serialiseObject(constraintList));
    params.put("params", serialiseObject(allParams));

    tx.execute("MERGE (vc:_n10sValidatorConfig { _id: 1}) "
        + "SET vc._gq = $gq, vc._nsq = $nsq, vc._tl = $tl, vc._params = $params, "
        + " vc._constraintList = $cl ", params);
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
  }
}
