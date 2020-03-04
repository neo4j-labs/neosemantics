package semantics.config;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.*;
import java.util.stream.Collectors;

public class GraphConfig {

    public static final int GRAPHCONF_VOC_URI_SHORTEN = 0;
    public static final int GRAPHCONF_VOC_URI_SHORTEN_STRICT = 1;
    public static final int GRAPHCONF_VOC_URI_IGNORE = 2;
    public static final int GRAPHCONF_VOC_URI_MAP = 3;
    public static final int GRAPHCONF_VOC_URI_KEEP = 4;

    static final int GRAPHCONF_VOC_URI_DEFAULT = GRAPHCONF_VOC_URI_SHORTEN;

    public static final String GRAPHCONF_VOC_URI_SHORTEN_STR = "SHORTEN";
    public static final String GRAPHCONF_VOC_URI_SHORTEN_STRICT_STR = "SHORTEN_STRICT";
    public static final String GRAPHCONF_VOC_URI_IGNORE_STR = "IGNORE";
    public static final String GRAPHCONF_VOC_URI_MAP_STR = "MAP";
    public static final String GRAPHCONF_VOC_URI_KEEP_STR = "KEEP";

    public static final int GRAPHCONF_MULTIVAL_PROP_OVERWRITE = 0;
    public static final int GRAPHCONF_MULTIVAL_PROP_ARRAY = 1;
    //static final int GRAPHCONF_MULTIVAL_PROP_REIFY = 2; //not in use

    public static final String GRAPHCONF_MULTIVAL_PROP_OVERWRITE_STR = "OVERWRITE";
    public static final String GRAPHCONF_MULTIVAL_PROP_ARRAY_STR = "ARRAY";

    public static final int GRAPHCONF_RDFTYPES_AS_LABELS = 0;
    public static final int GRAPHCONF_RDFTYPES_AS_NODES = 1;
    public static final int GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES = 2;

    public static final String GRAPHCONF_RDFTYPES_AS_LABELS_STR = "LABELS";
    public static final String GRAPHCONF_RDFTYPES_AS_NODES_STR = "NODES";
    public static final String GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES_STR = "LABELS AND NODES";

    private final int handleVocabUris;
    private final int handleMultival;
    private final int handleRDFTypes;
    private final boolean keepLangTag;
    private final boolean applyNeo4jNaming;
    private final boolean keepCustomDataTypes;
    private final Set<String> multivalPropList;
    private final Set<String> customDataTypePropList;

    public GraphConfig(Map<String, Object> props) throws InvalidParamException {
        this.handleVocabUris = (props.containsKey("handleVocabUris") ? parseHandleVocabUrisValue(
                (String) props.get("handleVocabUris")) : GRAPHCONF_VOC_URI_SHORTEN);
        this.handleMultival = (props.containsKey("handleMultival") ? parseHandleMultivalValue(
                (String) props.get("handleMultival")) : GRAPHCONF_MULTIVAL_PROP_OVERWRITE);
        // this is the old typesToLabels
        this.handleRDFTypes = (props.containsKey("handleRDFTypes") ? parseHandleRDFTypesValue( (String)props
                .get("handleRDFTypes")) : GRAPHCONF_RDFTYPES_AS_LABELS);
        this.keepLangTag = (props.containsKey("keepLangTag") && (boolean) props
                .get("keepLangTag"));
        this.applyNeo4jNaming = (props.containsKey("applyNeo4jNaming") && (boolean) props
                .get("applyNeo4jNaming"));
        this.keepCustomDataTypes = (props.containsKey("keepCustomDataTypes") && (boolean) props
                .get("keepCustomDataTypes"));
        this.multivalPropList = (props.containsKey("multivalPropList")
                ? ((List<String>) props.get("multivalPropList")).stream().collect(Collectors.toSet())
                : null);
        this.customDataTypePropList = (props.containsKey("customDataTypePropList")
                ? ((List<String>) props.get("customDataTypePropList")).stream().collect(Collectors.toSet())
                : null);
    }

    public GraphConfig(Transaction tx) throws GraphConfigNotFound {
        Result gcResult = tx.execute("MATCH (gc:_GraphConfig {_id: 1 }) RETURN gc ");
        if(gcResult.hasNext()){
            Map<String, Object> singleRecord = gcResult.next();
            Map<String, Object> graphConfigProperties = ((Node) singleRecord.get("gc")).getAllProperties();
            this.handleVocabUris = (int)graphConfigProperties.get("_handleVocabUris");
            this.handleMultival = (int)graphConfigProperties.get("_handleMultival");
            this.handleRDFTypes = (int)graphConfigProperties.get("_handleRDFTypes");
            this.keepLangTag = (boolean)graphConfigProperties.get("_keepLangTag");
            this.keepCustomDataTypes = (boolean)graphConfigProperties.get("_keepCustomDataTypes");
            this.applyNeo4jNaming = (boolean)graphConfigProperties.get("_applyNeo4jNaming");
            this.multivalPropList = getListOfStringsOrNull(graphConfigProperties, "_multivalPropList");
            this.customDataTypePropList = getListOfStringsOrNull(graphConfigProperties,"_customDataTypePropList");
        } else {
            throw new GraphConfigNotFound();
        }
    }

    private Set<String> getListOfStringsOrNull(Map<String, Object> gcp, String key) {
        if(gcp.containsKey(key)){
            Set<String> resultSet = new HashSet<>();
            String[] arrayOfStrings = (String[]) gcp.get(key);
            for (String str : arrayOfStrings) {
                resultSet.add(str);
            }
            return resultSet;
        } else {
            return null;
        }
    }

    public int parseHandleVocabUrisValue(String handleVocUrisAsText) throws InvalidParamException {
        if (handleVocUrisAsText.equals(GRAPHCONF_VOC_URI_SHORTEN_STR)) {
            return GRAPHCONF_VOC_URI_SHORTEN;
        } else if (handleVocUrisAsText.equals(GRAPHCONF_VOC_URI_SHORTEN_STRICT_STR)){
                return GRAPHCONF_VOC_URI_SHORTEN_STRICT;
        } else if (handleVocUrisAsText.equals(GRAPHCONF_VOC_URI_IGNORE_STR)){
                return GRAPHCONF_VOC_URI_IGNORE;
        } else if (handleVocUrisAsText.equals(GRAPHCONF_VOC_URI_MAP_STR)){
                return GRAPHCONF_VOC_URI_MAP;
        } else if (handleVocUrisAsText.equals(GRAPHCONF_VOC_URI_KEEP_STR)) {
            return GRAPHCONF_VOC_URI_KEEP;
        } else {
            throw new InvalidParamException(handleVocUrisAsText  + " is not a valid option for param 'handleVocabUris'" );
        }
    }

    public int parseHandleMultivalValue(String multivalAsText) throws InvalidParamException {
        if(multivalAsText.equals(GRAPHCONF_MULTIVAL_PROP_OVERWRITE_STR)) {
            return GRAPHCONF_MULTIVAL_PROP_OVERWRITE;
        } else if(multivalAsText.equals(GRAPHCONF_MULTIVAL_PROP_ARRAY_STR)) {
            return GRAPHCONF_MULTIVAL_PROP_ARRAY;
        } else {
            throw new InvalidParamException(multivalAsText  + " is not a valid option for param 'handleMultival'" );
        }
    }


    private int parseHandleRDFTypesValue(String handleRDFTypesAsText) throws InvalidParamException {
        if(handleRDFTypesAsText.equals(GRAPHCONF_RDFTYPES_AS_LABELS_STR)) {
            return GRAPHCONF_RDFTYPES_AS_LABELS;
        } else if(handleRDFTypesAsText.equals(GRAPHCONF_RDFTYPES_AS_NODES_STR)) {
            return GRAPHCONF_RDFTYPES_AS_NODES;
        } else if(handleRDFTypesAsText.equals(GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES_STR)) {
            return GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES;
        } else {
            throw new InvalidParamException(handleRDFTypesAsText  + " is not a valid option for param 'handleRDFTypes'" );
        }
    }

    public String getHandleVocabUrisAsString() {
        switch (this.handleVocabUris) {
            case GRAPHCONF_VOC_URI_SHORTEN:
                return GRAPHCONF_VOC_URI_SHORTEN_STR;
            case GRAPHCONF_VOC_URI_SHORTEN_STRICT:
                return GRAPHCONF_VOC_URI_SHORTEN_STRICT_STR;
            case GRAPHCONF_VOC_URI_IGNORE:
                return GRAPHCONF_VOC_URI_IGNORE_STR;
            case GRAPHCONF_VOC_URI_MAP:
                return GRAPHCONF_VOC_URI_MAP_STR;
            default:
                return GRAPHCONF_VOC_URI_KEEP_STR;
        }
    }

    public String getHandleMultivalAsString() {
        switch (this.handleMultival) {
            case GRAPHCONF_MULTIVAL_PROP_OVERWRITE:
                return GRAPHCONF_MULTIVAL_PROP_OVERWRITE_STR;
            case GRAPHCONF_MULTIVAL_PROP_ARRAY:
                return GRAPHCONF_MULTIVAL_PROP_ARRAY_STR;
            default:
                return GRAPHCONF_MULTIVAL_PROP_OVERWRITE_STR;
        }
    }

    public Map<String, Object> serialiseConfig() {
        Map<String, Object> configAsMap = new HashMap<>();

        //ONLY ADD IF NOT DEFAULT
        // if (this.handleVocabUris != 0) {
        configAsMap.put("_handleVocabUris", this.handleVocabUris);
        configAsMap.put("_handleMultival", this.handleMultival);
        configAsMap.put("_handleRDFTypes", this.handleRDFTypes);
        configAsMap.put("_keepLangTag", this.keepLangTag);
        configAsMap.put("_keepCustomDataTypes", this.keepCustomDataTypes);
        configAsMap.put("_applyNeo4jNaming", this.applyNeo4jNaming);
        if (this.multivalPropList != null ) {
            configAsMap.put("_multivalPropList", this.multivalPropList);
        }
        if (this.customDataTypePropList != null ) {
            configAsMap.put("_customDataTypePropList", this.customDataTypePropList);
        }

        return  configAsMap;
    }

    public int getHandleVocabUris() {
        return handleVocabUris;
    }

    public int getHandleMultival() {
        return handleMultival;
    }

    public int getHandleRDFTypes() {
        return handleRDFTypes;
    }

    public boolean isKeepLangTag() {
        return keepLangTag;
    }

    public boolean isApplyNeo4jNaming() {
        return applyNeo4jNaming;
    }

    public boolean isKeepCustomDataTypes(){
        return keepCustomDataTypes;
    }

    public Set<String> getMultivalPropList() { return multivalPropList; }

    public Set<String> getCustomDataTypePropList() { return customDataTypePropList; }

    public class InvalidParamException extends Throwable {
        public InvalidParamException(String msg) {
            super(msg);
        }
    }

    public class GraphConfigNotFound extends Throwable {
    }
}
