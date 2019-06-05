package semantics;

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
        ? ((List<String>) props.get("multivalPropList")).stream().collect(Collectors.toSet()) : null);
    predicateExclusionList = (props.containsKey("predicateExclusionList")
        ? ((List<String>) props.get("predicateExclusionList")).stream().collect(Collectors.toSet()) : null);
    customDataTypedPropList = (props.containsKey("customDataTypedPropList")
        ? ((List<String>) props.get("customDataTypedPropList")).stream().collect(Collectors.toSet()) : null);
    typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props
        .get("typesToLabels") : DEFAULT_TYPES_TO_LABELS);
    keepLangTag = (props.containsKey("keepLangTag") ? (boolean) props
        .get("keepLangTag") : false);
    keepCustomDataTypes = (props.containsKey("keepCustomDataTypes") ? (boolean) props
        .get("keepCustomDataTypes") : DEFAULT_KEEP_CUSTOM_DATA_TYPES);
    commitSize = (props.containsKey("commitSize") ? ((long) props.get("commitSize") > 0 ? (long) props.get("commitSize") : DEFAULT_COMMIT_SIZE)
        : DEFAULT_COMMIT_SIZE);
    nodeCacheSize = (props.containsKey("nodeCacheSize") ? (long) props
        .get("nodeCacheSize") : DEFAULT_NODE_CACHE_SIZE);
    languageFilter = (props.containsKey("languageFilter") ? (String) props
        .get("languageFilter") : null);
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

  private int getHandleMultivalAsInt(String ignoreUrlsAsText) {
    if (ignoreUrlsAsText.equals("OVERWRITE")) {
      return 0;
    } else if (ignoreUrlsAsText.equals("ARRAY")) {
      return 1;
    } else if (ignoreUrlsAsText.equals("REIFY")) {
      return 2;
    } else { //HYBRID
      return 3;
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
}
