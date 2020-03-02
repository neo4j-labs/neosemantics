package semantics.config;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import semantics.result.NodeResult;

import java.util.HashMap;
import java.util.Map;

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

    private final int handleVocabUris;
    private final int handleMultival;
    private final boolean keepLangTag;

    public GraphConfig(Map<String, Object> props) throws InvalidParamException {
        this.handleVocabUris = (props.containsKey("handleVocabUris") ? parseHandleVocabUrisValue(
                (String) props.get("handleVocabUris")) : GRAPHCONF_VOC_URI_SHORTEN);
        this.handleMultival = (props.containsKey("handleMultival") ? parseHandleMultivalValue(
                (String) props.get("handleMultival")) : GRAPHCONF_MULTIVAL_PROP_OVERWRITE);
        this.keepLangTag = (props.containsKey("keepLangTag") && (boolean) props
                .get("keepLangTag"));
    }

    public GraphConfig(Transaction tx) throws GraphConfigNotFound {
        Result gcResult = tx.execute("MATCH (gc:_GraphConfig {_id: 1 }) RETURN gc ");
        if(gcResult.hasNext()){
            Map<String, Object> singleRecord = gcResult.next();
            Map<String, Object> graphConfigProperties = ((Node) singleRecord.get("gc")).getAllProperties();
            this.handleVocabUris = (int)graphConfigProperties.get("_handleVocabUris");
            this.handleMultival = (int)graphConfigProperties.get("_handleMultival");
            this.keepLangTag = (boolean)graphConfigProperties.get("_keepLangTag");
        } else {
            throw new GraphConfigNotFound();
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
        configAsMap.put("_keepLangTag", this.keepLangTag);

        return  configAsMap;
    }

    public int getHandleVocabUris() {
        return handleVocabUris;
    }

    public int getHandleMultival() {
        return handleMultival;
    }

    public boolean isKeepLangTag() {
        return keepLangTag;
    }

    public class InvalidParamException extends Throwable {
        public InvalidParamException(String msg) {
            super(msg);
        }
    }

    public class GraphConfigNotFound extends Throwable {
    }
}
