package n10s.rdf;

import static n10s.graphconfig.Params.DATATYPE_REGULAR_PATTERN;
import static n10s.graphconfig.Params.DATATYPE_SHORTENED_PATTERN;
import static n10s.graphconfig.Params.LANGUAGE_TAGGED_VALUE_PATTERN;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static n10s.graphconfig.Params.SHORTENED_URI_PATTERN;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import n10s.CommonProcedures;
import n10s.RDFImportException;
import n10s.StatementPreviewer;
import n10s.rdf.stream.StatementStreamer;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.RDFParserConfig;
import n10s.rdf.stream.StatementStreamer.StreamerLimitReached;
import n10s.result.GraphResult;
import n10s.result.StreamedStatement;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.NsPrefixMap;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

/**
 * Created by jbarrasa on 21/03/2016. <p> RDF importer based on: 1. Instancdes of DatatypeProperties
 * become node attributes 2. rdf:type relationships are transformed either into labels or
 * relationships to nodes representing the class 3. Instances of ObjectProperties become
 * relationships ( See https://jbarrasa.com/2016/06/07/importing-rdf-data-into-neo4j/ )
 */
public class RDFProcedures extends CommonProcedures {

  protected ImportResults doImport(String format, String url,
      String rdfFragment, Map<String, Object> props, GraphConfig overrideGC) {

    DirectStatementLoader statementLoader = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    ImportResults importResults = new ImportResults();
    try {
      checkConstraintExist();
      conf = new RDFParserConfig(props, (overrideGC!=null?overrideGC:new GraphConfig(tx)));
      rdfFormat = getFormat(format);
      statementLoader = new DirectStatementLoader(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e) {
      importResults.setTerminationKO(e.getMessage());
    } catch (GraphConfig.GraphConfigNotFound e) {
      importResults
          .setTerminationKO("A Graph Config is required for RDF importing procedures to run");
    } catch (RDFImportBadParams e) {
      importResults.setTerminationKO(e.getMessage());
    }

    if (statementLoader != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementLoader);
        importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
        importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
        importResults.setNamespaces(statementLoader.getNamespaces());
        importResults.setConfigSummary(props);

      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        importResults.setTerminationKO(e.getMessage());
        importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
        importResults.setTriplesParsed(statementLoader.totalTriplesParsed);
        importResults.setConfigSummary(props);
      }
    }
    return importResults;
  }

  protected GraphResult doPreview(@Name("url") String url, @Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    StatementPreviewer statementViewer = null;
    Map<String, Node> virtualNodes = new HashMap<>();
    List<Relationship> virtualRels = new ArrayList<>();

    try {
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      statementViewer = new StatementPreviewer(db, tx, conf, virtualNodes, virtualRels, log);
    } catch (RDFImportBadParams e) {
      throw new RDFImportException(e);
    } catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException(
          "A Graph Config is required for RDF importing procedures to run");
    }

    if (statementViewer != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementViewer);
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        throw new RDFImportException(e.getMessage());
      }
    }
    return new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
  }

  protected Stream<StreamedStatement> doStream(@Name("url") String url,
      @Name("rdfFragment") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {
    StatementStreamer statementStreamer = null;
    RDFFormat rdfFormat = null;
    RDFParserConfig conf = null;
    try {
      rdfFormat = getFormat(format);
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      statementStreamer = new StatementStreamer(conf);
    } catch (RDFImportBadParams e) {
      throw new RDFImportException(e);
    } catch (GraphConfig.GraphConfigNotFound e) {
      throw new RDFImportException(
          "A Graph Config is required for RDF importing procedures to run");
    }

    try {
      parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementStreamer);

    }catch (StreamerLimitReached e) {
      //streaming interrupted when limit reached. This is fine.
    } catch (IOException | QueryExecutionException | RDFParseException e) {
      throw new RDFImportException(e.getMessage());
    }
    return statementStreamer.getStatements().stream();
  }

  protected DeleteResults doDelete(String format, String url, String rdfFragment,
      Map<String, Object> props) {

    DirectStatementDeleter statementDeleter = null;
    RDFParserConfig conf = null;
    RDFFormat rdfFormat = null;
    DeleteResults deleteResults = new DeleteResults();

    try {
      checkConstraintExist();
      conf = new RDFParserConfig(props, new GraphConfig(tx));
      rdfFormat = getFormat(format);
      statementDeleter = new DirectStatementDeleter(db, tx, conf, log);
    } catch (RDFImportPreRequisitesNotMet e) {
      deleteResults.setTerminationKO(e.getMessage());
    } catch (GraphConfig.GraphConfigNotFound e) {
      deleteResults
          .setTerminationKO("A Graph Config is required for RDF importing procedures to run");
    } catch (RDFImportBadParams e) {
      deleteResults.setTerminationKO(e.getMessage());
    }

    if (statementDeleter != null) {
      try {
        parseRDFPayloadOrFromUrl(rdfFormat, url, rdfFragment, props, statementDeleter);
      } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException e) {
        deleteResults.setTerminationKO(e.getMessage());
        e.printStackTrace();
      } finally {
        deleteResults.setTriplesDeleted(
            statementDeleter.totalTriplesMapped - statementDeleter.getNotDeletedStatementCount());
        deleteResults.setExtraInfo(statementDeleter.getbNodeInfo());
        deleteResults.setNamespaces(statementDeleter.getNamespaces());
      }
    }
    return deleteResults;
  }

  @UserFunction
  @Description("Returns the XMLSchema or custom datatype of a property when present")
  public String getDataType(@Name("literal") Object literal) {

    String result;

    if (literal instanceof String) {
      Matcher matcherShortened = DATATYPE_SHORTENED_PATTERN.matcher((String) literal);
      Matcher matcherRegular = DATATYPE_REGULAR_PATTERN.matcher((String) literal);
      if (matcherShortened.matches()) {
        result = matcherShortened.group(2);
      } else if (matcherRegular.matches()) {
        result = matcherRegular.group(2);
      } else {
        result = XMLSchema.STRING.stringValue();
      }
    } else if (literal instanceof Long) {
      result = XMLSchema.LONG.stringValue();
    } else if (literal instanceof Double) {
      result = XMLSchema.DOUBLE.stringValue();
    } else if (literal instanceof Boolean) {
      result = XMLSchema.BOOLEAN.stringValue();
    } else if (literal instanceof LocalDateTime) {
      result = XMLSchema.DATETIME.stringValue();
    } else if (literal instanceof LocalDate) {
      result = XMLSchema.DATE.stringValue();
    } else {
      result = null;
    }

    return result;
  }

  @UserFunction
  @Description("Returns the value of a datatype of a property after stripping out the datatype "
      + "information when present")
  public String getValue(@Name("literal") String literal) {

    Matcher matcherShortened = DATATYPE_SHORTENED_PATTERN.matcher(literal);
    Matcher matcherRegular = DATATYPE_REGULAR_PATTERN.matcher(literal);
    Matcher matcherLanguageTagged = LANGUAGE_TAGGED_VALUE_PATTERN.matcher(literal);
    String result = literal;
    if (matcherShortened.matches()) {
      result = matcherShortened.group(1);
    } else if (matcherRegular.matches()) {
      result = matcherRegular.group(1);
    } else if (matcherLanguageTagged.matches()) {
      result = matcherLanguageTagged.group(1);
    }
    return result;
  }

  @UserFunction
  @Description("Returns the local part of an IRI")
  public String getIRILocalName(@Name("url") String url) {
    return url.substring(URIUtil.getLocalNameIndex(url));
  }

  @UserFunction
  @Description("Returns the namespace part of an IRI")
  public String getIRINamespace(@Name("url") String url) {
    return url.substring(0, URIUtil.getLocalNameIndex(url));
  }

  @UserFunction
  @Description("Returns the first value with the language tag passed as first argument or null if "
      + "there's not a value for the provided tag")
  public String getLangValue(@Name("lang") String lang, @Name("values") Object values) {

    if (values instanceof List) {
      if (((List) values).get(0) instanceof String) {
        for (Object val : (List<String>) values) {
          Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) val);
          if (m.matches() && m.group(2).equals(lang)) {
            return m.group(1);
          }
        }
      }
    } else if (values instanceof String[]) {
      String[] valuesAsArray = (String[]) values;
      for (int i = 0; i < valuesAsArray.length; i++) {
        Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher(valuesAsArray[i]);
        if (m.matches() && m.group(2).equals(lang)) {
          return m.group(1);
        }
      }
    } else if (values instanceof String) {
      Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) values);
      if (m.matches() && m.group(2).equals(lang)) {
        return m.group(1);
      }
    }
    return null;
  }

  @UserFunction
  @Description("Returns the language tag of a value. Returns null if the value is not a string or"
      + "if the string has no language tag")
  public String getLangTag(@Name("value") Object value) {

    if (value instanceof String) {
      Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) value);
      if (m.matches()) {
        return m.group(2);
      }
    }
    return null;
  }

  @UserFunction
  @Description("Returns false if the value is not a string or if the string is not tagged with the "
      + " given language tag")
  public Boolean hasLangTag(@Name("lang") String lang, @Name("value") Object value) {

    if (value instanceof String) {
      Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) value);
      return m.matches() && m.group(2).equals(lang);
    }
    return false;
  }

  @UserFunction
  @Description("Returns the expanded (full) IRI given a shortened one created in the load process "
      + "with semantics.importRDF")
  public String fullUriFromShortForm(@Name("short") String str)
      throws InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName {

    Matcher m = SHORTENED_URI_PATTERN.matcher(str);
    if (!m.matches()) {
      throw new InvalidShortenedName(
          "Wrong Syntax: " + str + " is not a valid n10s shortened schema name.");
    }
    NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
    if (!prefixDefs.hasPrefix(m.group(1))) {
      throw new InvalidShortenedName("Prefix Undefined: " + str + " is using an undefined prefix.");
    }

    return prefixDefs.getNsForPrefix(m.group(1)) + m.group(2);
  }

  @UserFunction
  @Description("Returns the shortened version of an IRI using the existing namespace definitions")
  public String shortFormFromFullUri(@Name("uri") String str)
      throws InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName {

    IRI iri = SimpleValueFactory.getInstance().createIRI(str);
    NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
    if (!prefixDefs.hasNs(iri.getNamespace())) {
      throw new InvalidShortenedName(
          "Prefix Undefined: No prefix defined for this namespace <" + str + "> .");
    }
    return prefixDefs.getPrefixForNs(iri.getNamespace()) + PREFIX_SEPARATOR + iri.getLocalName();

  }

  public static class ImportResults {

    public String terminationStatus = "OK";
    public long triplesLoaded = 0;
    public long triplesParsed = 0;
    public Map<String, String> namespaces;
    public String extraInfo = "";
    public Map<String, Object> callParams;

    public void setTriplesLoaded(long count) {
      this.triplesLoaded = count;
    }

    public void setTriplesParsed(long count) {
      this.triplesParsed = count;
    }

    public void setConfigSummary(Map<String, Object> summary) {
      this.callParams = summary;
    }

    public void setNamespaces(Map<String, String> namespaces) {
      this.namespaces = namespaces;
    }

    public void setTerminationKO(String message) {
      this.terminationStatus = "KO";
      this.extraInfo = message;
    }

  }

  public static class DeleteResults {

    public String terminationStatus = "OK";
    public long triplesDeleted = 0;
    public Map<String, String> namespaces;
    public String extraInfo = "";

    public void setTriplesDeleted(long triplesDeleted) {
      this.triplesDeleted = triplesDeleted;
    }

    public void setExtraInfo(String extraInfo) {
      this.extraInfo = extraInfo;
    }

    public void setNamespaces(Map<String, String> namespaces) {
      this.namespaces = namespaces;
    }

    public void setTerminationKO(String message) {
      this.terminationStatus = "KO";
      this.extraInfo = message;
    }

  }

}
