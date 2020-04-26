package n10s.validation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ValidatorConfig {

  private final StringBuilder engineGlobal;
  private final StringBuilder engineForNodeSet;
  private final Map<String, Object> allParams;
  private final List<ConstraintComponent> constraintList;

  public ValidatorConfig(){

    this.engineGlobal = new StringBuilder("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity , '' as message ");

    this.engineForNodeSet = new StringBuilder("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity , '' as message ");

    this.allParams = new HashMap<>();

    this.constraintList = new ArrayList<>();

  }

  public StringBuilder getEngineGlobal() {
    return engineGlobal;
  }

  public StringBuilder getEngineForNodeSet() {
    return engineForNodeSet;
  }

  public Map<String, Object> getAllParams() {
    return allParams;
  }

  public List<ConstraintComponent> getConstraintList() { return  constraintList; }

  public void addConstraintToList(ConstraintComponent cc) {
    constraintList.add(cc);
  }
}
