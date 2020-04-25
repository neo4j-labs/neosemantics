package n10s.validation;

import java.util.HashMap;
import java.util.Map;

public class ValidatorConfig {

  private final StringBuilder engineGlobal;
  private final StringBuilder engineForNodeSet;
  private final Map<String, Object> allParams;

  public ValidatorConfig(){

    this.engineGlobal = new StringBuilder("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity , '' as message ");
    this.engineForNodeSet = new StringBuilder("UNWIND [] as row RETURN '' as nodeId, " +
        "'' as nodeType, '' as shapeId, '' as propertyShape, '' as offendingValue, '' as propertyName"
        + ", '' as severity , '' as message ");
    this.allParams = new HashMap<>();

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
}
