package semantics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RDFParserConfig {

  static final int URL_SHORTEN = 0;
  static final int URL_IGNORE = 1;
  static final int URL_MAP = 2;
  static final int URL_KEEP = 3;
  static final int PROP_OVERWRITE = 0;
  static final int PROP_ARRAY = 1;
  static final int PROP_REIFY = 2;

  private final int handleVocabUris;
  private final boolean applyNeo4jNaming;
  private final int handleMultival;
  private final Set<String> multivalPropList;
  private final Set<String> predicateExclusionList;
  private final Set<String> customDataTypedPropList;
  private final boolean typesToLabels;
  private final boolean keepLangTag;
  private final boolean keepCustomDataTypes;
  private final boolean verifyUriSyntax;
  private long commitSize;
  private final long nodeCacheSize;
  private final String languageFilter;

  private static final boolean DEFAULT_TYPES_TO_LABELS = true;
  private static final boolean DEFAULT_KEEP_CUSTOM_DATA_TYPES = false;
  //numbre of statements parsed between partial commits
  private static final long DEFAULT_COMMIT_SIZE = 25000;
  //nodes kept in the cache when writing to disk
  private static final long DEFAULT_NODE_CACHE_SIZE = 10000;


  public RDFParserConfig(Map<String, Object> props) {
    handleVocabUris = (props.containsKey("handleVocabUris") ? getHandleVocabUrisAsInt(
        (String) props.get("handleVocabUris")) : 0);
    applyNeo4jNaming = (props.containsKey("applyNeo4jNaming") ? (boolean) props
        .get("applyNeo4jNaming") : false);
    handleMultival = (props.containsKey("handleMultival") ? getHandleMultivalAsInt(
        (String) props.get("handleMultival")) : 0);
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
    keepLangTag = (props.containsKey("keepLangTag") ? (boolean) props
        .get("keepLangTag") : false);
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


  private int getHandleVocabUrisAsInt(String handleVocUrisAsText) {
    if (handleVocUrisAsText.equals("SHORTEN")) {
      return 0;
    } else if (handleVocUrisAsText.equals("IGNORE")) {
      return 1;
    } else if (handleVocUrisAsText.equals("MAP")) {
      return 2;
    } else { //KEEP
      return 3;
    }
  }

  private String getHandleVocabUrisAsString() {
    if (handleVocabUris == 0) {
      return "SHORTEN";
    } else if (handleVocabUris == 1) {
      return "IGNORE";
    } else if (handleVocabUris == 2) {
      return "MAP";
    } else {
      return "KEEP";
    }
  }

  private int getHandleMultivalAsInt(String multivalAsText) {
    if (multivalAsText.equals("OVERWRITE")) {
      return 0;
    } else if (multivalAsText.equals("ARRAY")) {
      return 1;
    }
    // Not in use at the moment
    else if (multivalAsText.equals("REIFY")) {
      return 2;
    } else {
      return 3;
    }
  }

  private String getHandleMultivalAsString() {
    if (handleMultival == 0) {
      return "OVERWRITE";
    } else if (handleMultival == 1) {
      return "ARRAY";
    }
    // Not in use at the moment
    else if (handleMultival == 2) {
      return "REIFY";
    } else {
      return "HYBRID";
    }
  }

  public int getHandleVocabUris() {
    return handleVocabUris;
  }

  public boolean isApplyNeo4jNaming() {
    return applyNeo4jNaming;
  }

  public int getHandleMultival() {
    return handleMultival;
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

  public boolean isKeepLangTag() {
    return keepLangTag;
  }

  public boolean isKeepCustomDataTypes() {
    return keepCustomDataTypes;
  }

  public long getCommitSize() {
    return commitSize;
  }

  public long getNodeCacheSize() {
    return nodeCacheSize;
  }

  public String getLanguageFilter() {
    return languageFilter;
  }

  public void setCommitSize(long commitSize) {
    this.commitSize = commitSize;
  }

  public Map<String, Object> getConfigSummary() {
    Map<String, Object> summary = new HashMap<>();

    if (handleVocabUris != 0) {
      summary.put("handleVocabUris", getHandleVocabUrisAsString());
    }

    if (applyNeo4jNaming != false) {
      summary.put("applyNeo4jNaming", applyNeo4jNaming);
    }

    if (handleMultival != 0) {
      summary.put("handleMultival", getHandleMultivalAsString());
    }

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

    if (keepLangTag != false) {
      summary.put("keepLangTag", keepLangTag);
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

    if (verifyUriSyntax != true) {
      summary.put("verifyUriSyntax", verifyUriSyntax);
    }

    return summary;
  }
}
