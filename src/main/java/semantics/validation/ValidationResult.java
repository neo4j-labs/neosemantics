package semantics.validation;

import java.util.Map;

public class ValidationResult {


  public final Object nodeId;
  public final String nodeType;
  public final String shapeId;
  public final String propertyShape;
  public final Object offendingValue;
  public final String propertyName;
  public final String severity;

  public ValidationResult(Map<String, Object> map) {
    this.nodeId = (Long) map.get("nodeId");
    this.nodeType = (String) map.get("nodeType");
    this.shapeId = (String) map.get("shapeId");
    this.propertyShape = (String) map.get("propertyShape");
    this.offendingValue = map.get("offendingValue");
    this.propertyName = (String) map.get("propertyName");
    this.severity = (String) map.get("severity");
  }

  public ValidationResult(String focusNode, String nodeType, String propertyName, String severity,
      String constraint) {
    this.nodeId = focusNode;
    this.nodeType = nodeType;
    this.propertyName = propertyName;
    this.severity = severity;
    this.offendingValue = "";
    this.propertyShape = constraint;
    this.shapeId = "";
  }

  @Override
  public String toString() {
    return "ValidationResult{" +
        "nodeId='" + nodeId + '\'' +
        ", nodeType='" + nodeType + '\'' +
        ", shapeId='" + shapeId + '\'' +
        ", propertyShape='" + propertyShape + '\'' +
        ", offendingValue='" + offendingValue + '\'' +
        ", propertyName='" + propertyName + '\'' +
        ", severity='" + severity + '\'' +
        '}';
  }
}
