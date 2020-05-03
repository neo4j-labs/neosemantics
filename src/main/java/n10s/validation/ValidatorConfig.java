package n10s.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

  public List<String> getRunnableQueries(boolean global, Set<String> triggerers) {
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

  private StringBuilder newInitialisedStringBuilder() {
    return new StringBuilder().append("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity , '' as message ");
  }

}
