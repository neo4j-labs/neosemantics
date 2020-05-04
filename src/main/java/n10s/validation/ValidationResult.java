package n10s.validation;

import java.util.Map;

public class ValidationResult {


  public final Object focusNode;
  public final String nodeType;
  public final String shapeId;
  public final String propertyShape;
  public final Object offendingValue;
  public final String resultPath;
  public final String severity;
  public final String resultMessage;

  public ValidationResult(Map<String, Object> map) {
    this.focusNode = map.get("nodeId");
    this.nodeType = (String) map.get("nodeType");
    this.shapeId = (String) map.get("shapeId");
    this.propertyShape = (String) map.get("propertyShape");
    this.offendingValue = map.get("offendingValue");
    this.resultPath = (String) map.get("propertyName");
    this.severity = (String) map.get("severity");
    this.resultMessage = (String) map.get("message");
  }

  public ValidationResult(Object focusNode, String nodeType, String resultPath, String severity,
      String constraint, String shapeId, String message, Object ov) {
    this.focusNode = focusNode;
    this.nodeType = nodeType;
    this.resultPath = resultPath;
    this.severity = severity;
    this.offendingValue = ov;
    this.propertyShape = constraint;
    this.shapeId = shapeId;
    this.resultMessage = message;
  }

  @Override
  public String toString() {
    return "ValidationResult{" +
        "focusNode='" + focusNode + '\'' +
        ", nodeType='" + nodeType + '\'' +
        ", shapeId='" + shapeId + '\'' +
        ", propertyShape='" + propertyShape + '\'' +
        ", offendingValue='" + offendingValue + '\'' +
        ", resultPath='" + resultPath + '\'' +
        ", resultMessage='" + resultMessage + '\'' +
        ", severity='" + severity + '\'' +
        '}';
  }
}
