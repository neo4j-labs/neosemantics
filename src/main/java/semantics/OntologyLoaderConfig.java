package semantics;

import java.util.Map;

public class OntologyLoaderConfig extends RDFParserConfig {

  private static final String DEFAULT_CLASS_LABEL_NAME = "Class";
  private static final String DEFAULT_SCO_REL_NAME = "SCO";
  private static final String DEFAULT_DATATYPEPROP_LABEL_NAME = "Property";
  private static final String DEFAULT_OBJECTPROP_LABEL_NAME = "Relationship";
  private static final String DEFAULT_SPO_REL_NAME = "SPO";
  private static final String DEFAULT_DOMAIN_REL_NAME = "DOMAIN";
  private static final String DEFAULT_RANGE_REL_NAME = "RANGE";
  private final String classLabelName;
  private final String subClassOfRelName;
  private final String dataTypePropertyLabelName;
  private final String objectPropertyLabelName;
  private final String subPropertyOfRelName;
  private final String domainRelName;
  private final String rangeRelName;

  public OntologyLoaderConfig(Map<String, Object> props) {
    super(props);

    classLabelName = props.containsKey("classLabel") ? (String) props.get("classLabel")
        : DEFAULT_CLASS_LABEL_NAME;
    subClassOfRelName =
        props.containsKey("subClassOfRel") ? (String) props.get("subClassOfRel")
            : DEFAULT_SCO_REL_NAME;
    dataTypePropertyLabelName =
        props.containsKey("dataTypePropertyLabel") ? (String) props.get("dataTypePropertyLabel")
            : DEFAULT_DATATYPEPROP_LABEL_NAME;
    objectPropertyLabelName =
        props.containsKey("objectPropertyLabel") ? (String) props.get("objectPropertyLabel")
            : DEFAULT_OBJECTPROP_LABEL_NAME;
    subPropertyOfRelName =
        props.containsKey("subPropertyOfRel") ? (String) props.get("subPropertyOfRel")
            : DEFAULT_SPO_REL_NAME;
    domainRelName =
        props.containsKey("domainRel") ? (String) props.get("domainRel") : DEFAULT_DOMAIN_REL_NAME;
    rangeRelName =
        props.containsKey("rangeRel") ? (String) props.get("rangeRel") : DEFAULT_RANGE_REL_NAME;

  }

  public String getClassLabelName() {
    return classLabelName;
  }

  public String getSubClassOfRelName() {
    return subClassOfRelName;
  }

  public String getDataTypePropertyLabelName() {
    return dataTypePropertyLabelName;
  }

  public String getObjectPropertyLabelName() {
    return objectPropertyLabelName;
  }

  public String getSubPropertyOfRelName() {
    return subPropertyOfRelName;
  }

  public String getDomainRelName() {
    return domainRelName;
  }

  public String getRangeRelName() {
    return rangeRelName;
  }

  public Map<String, Object> getConfigSummary() {
    Map<String, Object> summary = super.getConfigSummary();

    if (classLabelName != DEFAULT_CLASS_LABEL_NAME) {
      summary.put("classLabelName", classLabelName);
    }

    if (subClassOfRelName != DEFAULT_SCO_REL_NAME) {
      summary.put("subClassOfRelName", subClassOfRelName);
    }

    if (dataTypePropertyLabelName != DEFAULT_DATATYPEPROP_LABEL_NAME) {
      summary.put("dataTypePropertyLabelName", dataTypePropertyLabelName);
    }

    if (objectPropertyLabelName != DEFAULT_OBJECTPROP_LABEL_NAME) {
      summary.put("objectPropertyLabelName", objectPropertyLabelName);
    }

    if (subPropertyOfRelName != DEFAULT_SPO_REL_NAME) {
      summary.put("subPropertyOfRelName", subPropertyOfRelName);
    }

    if (domainRelName != DEFAULT_DOMAIN_REL_NAME) {
      summary.put("domainRelName", domainRelName);
    }

    if (rangeRelName != DEFAULT_RANGE_REL_NAME) {
      summary.put("rangeRelName", rangeRelName);
    }

    return summary;
  }

}
