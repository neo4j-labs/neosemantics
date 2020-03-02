package semantics.config;

import java.util.regex.Pattern;

public class Params {

  public static final String BASE_INDIV_NS = "neo4j://individuals#";
  public static final String BASE_VOCAB_NS = "neo4j://vocabulary#";
  public static final String PREFIX_SEPARATOR = "__";
  public static final String CUSTOM_DATA_TYPE_SEPERATOR = "^^";
  public static final Pattern DATATYPE_SHORTENED_PATTERN = Pattern.compile(
      "(.+)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "((\\w+)" +
          Pattern.quote(PREFIX_SEPARATOR) + "(.+))$");
  public static final Pattern DATATYPE_REGULAR_PATTERN = Pattern.compile(
      "(.+?)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "([a-zA-Z]+:(.+))");

  public static final Pattern SHORTENED_URI_PATTERN =
      Pattern.compile("^(\\w+)__(\\w+)$");

  public static final Pattern LANGUAGE_TAGGED_VALUE_PATTERN =
      Pattern.compile("^(.*)@([a-zA-Z\\-]+)$");
}
