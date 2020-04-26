package n10s.validation;

import java.io.Serializable;
import java.util.Map;

public class ConstraintComponent implements Serializable {

  public String target;
  public String propertyOrRelationshipPath;
  public String param;
  public Object value;

  public ConstraintComponent(Map<String, Object> record) {
    this.target = record.get("category").toString();
    this.propertyOrRelationshipPath = (record.get("propertyOrRelationshipPath")==null?null:record.get("propertyOrRelationshipPath").toString());
    this.param = record.get("param").toString();
    this.value = record.get("value");
  }

  public ConstraintComponent(String cat, String porp, String p,
      Object v) {
    this.target = cat;
    this.propertyOrRelationshipPath = porp;
    this.param = p;
    this.value = v;
  }
}
