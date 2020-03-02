package semantics.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RDFParserConfig {

  private static final boolean DEFAULT_TYPES_TO_LABELS = true;
  private static final boolean DEFAULT_KEEP_CUSTOM_DATA_TYPES = false;
  //numbre of statements parsed between partial commits
  private static final long DEFAULT_COMMIT_SIZE = 25000;
  //nodes kept in the cache when writing to disk
  private static final long DEFAULT_NODE_CACHE_SIZE = 10000;
  private final boolean applyNeo4jNaming;
  private final Set<String> multivalPropList;
  private final Set<String> predicateExclusionList;
  private final Set<String> customDataTypedPropList;
  private final boolean typesToLabels;
  private final boolean keepCustomDataTypes;
  private final boolean verifyUriSyntax;
  private final long nodeCacheSize;
  private final String languageFilter;
  private long commitSize;
  private GraphConfig graphConf;


  public RDFParserConfig(Map<String, Object> props, GraphConfig gc) {
    this.graphConf = gc;
    applyNeo4jNaming = (props.containsKey("applyNeo4jNaming") && (boolean) props
        .get("applyNeo4jNaming"));
    multivalPropList = (props.containsKey("multivalPropList")
        ? ((List<String>) props.get("multivalPropList")).stream().collect(Collectors.toSet())
        : null);
    predicateExclusionList = (props.containsKey("predicateExclusionList")
        ? ((List<String>) props.get("predicateExclusionList")).stream().collect(Collectors.toSet())
        : null);
    customDataTypedPropList = (props.containsKey("customDataTypedPropList")
        ? ((List<String>) props.get("customDataTypedPropList")).stream().collect(Collectors.toSet())
        : null);
    typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props
        .get("typesToLabels") : DEFAULT_TYPES_TO_LABELS);
    keepCustomDataTypes = (props.containsKey("keepCustomDataTypes") ? (boolean) props
        .get("keepCustomDataTypes") : DEFAULT_KEEP_CUSTOM_DATA_TYPES);
    commitSize = (props.containsKey("commitSize") ? ((long) props.get("commitSize") > 0
        ? (long) props.get("commitSize") : DEFAULT_COMMIT_SIZE)
        : DEFAULT_COMMIT_SIZE);
    nodeCacheSize = (props.containsKey("nodeCacheSize") ? (long) props
        .get("nodeCacheSize") : DEFAULT_NODE_CACHE_SIZE);
    languageFilter = (props.containsKey("languageFilter") ? (String) props
        .get("languageFilter") : null);
    verifyUriSyntax = props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true;
  }

  public Set<String> getMultivalPropList() {
    return multivalPropList;
  }

  public Set<String> getPredicateExclusionList() {
    return predicateExclusionList;
  }

  public Set<String> getCustomDataTypedPropList() {
    return customDataTypedPropList;
  }

  public boolean isVerifyUriSyntax() {
    return verifyUriSyntax;
  }

  public boolean isTypesToLabels() {
    return typesToLabels;
  }

  public boolean isKeepCustomDataTypes() {
    return keepCustomDataTypes;
  }

  public long getCommitSize() {
    return commitSize;
  }

  public void setCommitSize(long commitSize) {
    this.commitSize = commitSize;
  }

  public long getNodeCacheSize() {
    return nodeCacheSize;
  }

  public String getLanguageFilter() {
    return languageFilter;
  }

  public GraphConfig  getGraphConf() { return  graphConf; }

  public Map<String, Object> getConfigSummary() {
    Map<String, Object> summary = new HashMap<>();

    //GET the DB config from the DB config node
//    if (handleVocabUris != 0) {
//      summary.put("handleVocabUris", getHandleVocabUrisAsString());
//    }

    if (applyNeo4jNaming) {
      summary.put("applyNeo4jNaming", applyNeo4jNaming);
    }

//    if (handleMultival != 0) {
//      summary.put("handleMultival", getHandleMultivalAsString());
//    }

    if (multivalPropList != null) {
      summary.put("multivalPropList", multivalPropList);
    }

    if (predicateExclusionList != null) {
      summary.put("predicateExclusionList", predicateExclusionList);
    }

    if (customDataTypedPropList != null) {
      summary.put("customDataTypedPropList", customDataTypedPropList);
    }

    if (typesToLabels != DEFAULT_TYPES_TO_LABELS) {
      summary.put("typesToLabels", typesToLabels);
    }

    if (keepCustomDataTypes != DEFAULT_KEEP_CUSTOM_DATA_TYPES) {
      summary.put("keepCustomDataTypes", keepCustomDataTypes);
    }

    if (commitSize != DEFAULT_COMMIT_SIZE) {
      summary.put("commitSize", commitSize);
    }

    if (nodeCacheSize != DEFAULT_NODE_CACHE_SIZE) {
      summary.put("nodeCacheSize", nodeCacheSize);
    }

    if (languageFilter != null) {
      summary.put("languageFilter", languageFilter);
    }

    if (!verifyUriSyntax) {
      summary.put("verifyUriSyntax", verifyUriSyntax);
    }

    return summary;
  }

  public boolean isApplyNeo4jNaming() {
    return  this.applyNeo4jNaming;
  }
}
