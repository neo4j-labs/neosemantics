package org.eclipse.rdf4j.rio.jsonld;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.JSONSettings;


// Code from org.eclipse.rdf4j.rio.jsonld.JSONLDParser extended to make it possible to deal with any generic
// JSON data by injecting context
public class GenericJSONParser extends JSONLDParser {

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  public void parse(InputStream in, String baseURI, String hookUri, String relName) throws IOException, RDFParseException, RDFHandlerException {
    this.clear();

    try {
      JSONLDInternalTripleCallback callback = new JSONLDInternalTripleCallback(this.getRDFHandler(), this.valueFactory, this.getParserConfig(), this.getParseErrorListener(), (nodeID) -> {
        return this.createNode(nodeID);
      }, () -> {
        return this.createNode();
      });
      JsonLdOptions options = new JsonLdOptions(baseURI);
      options.useNamespaces = true;
      Map<String, Object> expandedContext = new HashMap<String, Object>();
      Map<String, Object> childMap = new HashMap<String, Object>();
      childMap.put("@vocab",baseURI);
      expandedContext.put("@context",childMap);
      options.setExpandContext(expandedContext);
      JsonFactory nextJsonFactory = this.configureNewJsonFactory();
      JsonParser nextParser = nextJsonFactory.createParser(in);
      Object parsedJson = JsonUtils.fromJsonParser(nextParser);
      LinkedHashMap<String, Object> wrappedParsedJson = new LinkedHashMap<>();
      wrappedParsedJson.put("@id", hookUri);
      wrappedParsedJson.put(relName, parsedJson);
      JsonLdProcessor.toRDF(wrappedParsedJson, callback, options);
    } catch (JsonLdError var13) {
      throw new RDFParseException("Could not parse JSONLD", var13);
    } catch (JsonProcessingException var14) {
      throw new RDFParseException("Could not parse JSONLD", var14, (long)var14.getLocation().getLineNr(), (long)var14.getLocation().getColumnNr());
    } catch (RuntimeException var15) {
      if (var15.getCause() != null && var15.getCause() instanceof RDFParseException) {
        throw (RDFParseException)var15.getCause();
      }

      throw var15;
    } finally {
      this.clear();
    }

  }

  private JsonFactory configureNewJsonFactory() {
    JsonFactory nextJsonFactory = new JsonFactory(JSON_MAPPER);
    if (this.getParserConfig().isSet(JSONSettings.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)) {
      nextJsonFactory.configure(
          Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_COMMENTS)) {
      nextJsonFactory.configure(Feature.ALLOW_COMMENTS, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_COMMENTS)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_NON_NUMERIC_NUMBERS)) {
      nextJsonFactory.configure(Feature.ALLOW_NON_NUMERIC_NUMBERS, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_NON_NUMERIC_NUMBERS)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_NUMERIC_LEADING_ZEROS)) {
      nextJsonFactory.configure(Feature.ALLOW_NUMERIC_LEADING_ZEROS, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_NUMERIC_LEADING_ZEROS)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_SINGLE_QUOTES)) {
      nextJsonFactory.configure(Feature.ALLOW_SINGLE_QUOTES, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_SINGLE_QUOTES)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_UNQUOTED_CONTROL_CHARS)) {
      nextJsonFactory.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_UNQUOTED_CONTROL_CHARS)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_UNQUOTED_FIELD_NAMES)) {
      nextJsonFactory.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_UNQUOTED_FIELD_NAMES)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_YAML_COMMENTS)) {
      nextJsonFactory.configure(Feature.ALLOW_YAML_COMMENTS, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_YAML_COMMENTS)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.ALLOW_TRAILING_COMMA)) {
      nextJsonFactory.configure(Feature.ALLOW_TRAILING_COMMA, ((Boolean)this.getParserConfig().get(JSONSettings.ALLOW_TRAILING_COMMA)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.INCLUDE_SOURCE_IN_LOCATION)) {
      nextJsonFactory.configure(Feature.INCLUDE_SOURCE_IN_LOCATION, ((Boolean)this.getParserConfig().get(JSONSettings.INCLUDE_SOURCE_IN_LOCATION)).booleanValue());
    }

    if (this.getParserConfig().isSet(JSONSettings.STRICT_DUPLICATE_DETECTION)) {
      nextJsonFactory.configure(Feature.STRICT_DUPLICATE_DETECTION, ((Boolean)this.getParserConfig().get(JSONSettings.STRICT_DUPLICATE_DETECTION)).booleanValue());
    }

    return nextJsonFactory;
  }

}
