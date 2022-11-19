package n10s.graphconfig;

import n10s.RDFImportException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RDFParserConfig {

  //number of statements parsed between partial commits
  private static final long DEFAULT_COMMIT_SIZE = 25000;
  //nodes kept in the cache when writing to disk
  private static final long DEFAULT_NODE_CACHE_SIZE = 10000;
  //number of triples streamed by default
  private static final int DEFAULT_STREAM_TRIPLE_LIMIT = 1000;
  private final Set<String> predicateExclusionList;
  private final boolean verifyUriSyntax;
  private final long nodeCacheSize;
  private final String languageFilter;
  private long commitSize;
  private long streamTripleLimit;
  private boolean abortOnError;
  private GraphConfig graphConf;
  private boolean strictDataTypeCheck;

  private boolean singleTx;

  public RDFParserConfig(Map<String, Object> props, GraphConfig gc) {
    this.graphConf = gc;

    predicateExclusionList = (props.containsKey("predicateExclusionList")
        ? ((List<String>) props.get("predicateExclusionList")).stream().collect(Collectors.toSet())
        : null);
    commitSize = (props.containsKey("commitSize") ? ((long) props.get("commitSize") > 0
        ? (long) props.get("commitSize") : DEFAULT_COMMIT_SIZE)
        : DEFAULT_COMMIT_SIZE);
    nodeCacheSize = (props.containsKey("nodeCacheSize") ? (long) props
        .get("nodeCacheSize") : DEFAULT_NODE_CACHE_SIZE);
    languageFilter = (props.containsKey("languageFilter") ? (String) props
        .get("languageFilter") : null);
    verifyUriSyntax = props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true;
    streamTripleLimit =
        props.containsKey("limit") ? (Long) props.get("limit") : DEFAULT_STREAM_TRIPLE_LIMIT;
    abortOnError = props.containsKey("abortOnError") ? (Boolean) props
            .get("abortOnError") : true;
    strictDataTypeCheck = props.containsKey("strictDataTypeCheck") ? (Boolean) props
            .get("strictDataTypeCheck") : true;
    singleTx = props.containsKey("singleTx") ? (Boolean) props
              .get("singleTx") : false;
  }

  public Set<String> getPredicateExclusionList() {
    return predicateExclusionList;
  }

  public boolean isVerifyUriSyntax() {
    return verifyUriSyntax;
  }

  public boolean isUseSingleTx() {
    return singleTx;
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

  public GraphConfig getGraphConf() {
    return graphConf;
  }

  public long getStreamTripleLimit() {
    return streamTripleLimit;
  }

  public boolean isAbortOnError() { return abortOnError; }

  public boolean isStrictDataTypeCheck() { return strictDataTypeCheck;  }

  public Map<String, Object> getConfigSummary() {
    Map<String, Object> summary = new HashMap<>();

    if (predicateExclusionList != null) {
      summary.put("predicateExclusionList", predicateExclusionList);
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

    if (!abortOnError) {
      summary.put("abortOnError", abortOnError);
    }

    if (streamTripleLimit != DEFAULT_STREAM_TRIPLE_LIMIT) {
      summary.put("limit", streamTripleLimit);
    }

    return summary;
  }

}
