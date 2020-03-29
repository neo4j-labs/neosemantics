package semantics.nsprefixes;

import static semantics.config.Params.PREFIX_SEPARATOR;
import static semantics.config.Params.SHORTENED_URI_PATTERN;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import semantics.result.NamespacePrefixesResult;
import semantics.utils.InvalidNamespacePrefixDefinitionInDB;
import semantics.utils.NamespacePrefixConflictException;
import semantics.utils.NsPrefixMap;

public class NsPrefixDefProcedures {

  @Context
  public GraphDatabaseService db;

  @Context
  public Transaction tx;

  @Context
  public Log log;

  @Procedure(mode = Mode.WRITE)
  @Description("Adds namespace - prefix pair definition")
  public Stream<NamespacePrefixesResult> add(@Name("prefix") String prefix,
      @Name("ns") String ns)
      throws InvalidNamespacePrefixDefinitionInDB, NamespacePrefixConflictException {
    NsPrefixMap map = new NsPrefixMap(tx);
    map.add(prefix, ns);
    map.flushToDB(tx);

    return map.getPrefixToNs().entrySet().stream()
        .map(n -> new NamespacePrefixesResult(n.getKey(), n.getValue()));

  }

  @Procedure(mode = Mode.WRITE)
  @Description("removes namespace prefix (by prefix)")
  public Stream<NamespacePrefixesResult> remove(@Name("prefix") String prefix)
      throws InvalidNamespacePrefixDefinitionInDB, NsPrefixOperationNotAllowed {
    if (!graphIsEmpty()) {
      throw new NsPrefixOperationNotAllowed("A namespace prefix definition cannot be removed "
          + "when the graph is non-empty.");
    }
    NsPrefixMap map = new NsPrefixMap(tx);
    map.removePrefix(prefix);
    map.flushToDB(tx);

    return map.getPrefixToNs().entrySet().stream()
        .map(n -> new NamespacePrefixesResult(n.getKey(), n.getValue()));

  }

  @UserFunction
  @Description("Returns the expanded (full) IRI given a shortened one created in the load process "
      + "with semantics.importRDF")
  public String nsForPrefix(@Name("short") String str) {

    Matcher m = SHORTENED_URI_PATTERN.matcher(str);
    if (m.matches()) {
      ResourceIterator<Node> nspd = tx.findNodes(Label.label("NamespacePrefixDefinition"));
      if (nspd.hasNext()) {
        Map<String, Object> namespaces = nspd.next().getAllProperties();
        Iterator<Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
        while (nsIterator.hasNext()) {
          Map.Entry<String, Object> kv = nsIterator.next();
          if (m.group(1).equals(kv.getValue())) {
            return kv.getKey() + m.group(2);
          }
        }
      }
    }
    //default return original value
    return str;
  }

  @UserFunction
  @Description("Returns the shortened version of an IRI using the existing namespace definitions")
  public String prefixForNs(@Name("uri") String str) {
    try {
      IRI iri = SimpleValueFactory.getInstance().createIRI(str);
      ResourceIterator<Node> nspd = tx.findNodes(Label.label("NamespacePrefixDefinition"));
      if (nspd.hasNext()) {
        Map<String, Object> namespaces = nspd.next().getAllProperties();
        Iterator<Map.Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
        while (nsIterator.hasNext()) {
          Map.Entry<String, Object> kv = nsIterator.next();
          if (kv.getKey().equals(iri.getNamespace())) {
            return kv.getValue() + PREFIX_SEPARATOR + iri.getLocalName();
          }
        }
      }
      return str;
    } catch (Exception e) {
      return str;
    }
  }


  @Procedure(mode = Mode.WRITE)
  @Description("Adds namespaces from a prefix declaration header fragment")
  public Stream<NamespacePrefixesResult> addFromText(
      @Name("prefix") String textFragment)
      throws InvalidNamespacePrefixDefinitionInDB, NamespacePrefixConflictException {

    //Try Turtle fragment
    Pattern turtleNamespaceDefinitionRegex =
        Pattern.compile("(?i)@prefix (\\S+)\\:\\s+<(\\S*)>", Pattern.MULTILINE);
    if (tryExtractNsDefinitions(textFragment, turtleNamespaceDefinitionRegex)) {
      return list();
    }

    //Try RDF/XML fragment
    Pattern rdfxmlNamespaceDefinitionRegex =
        Pattern.compile("xmlns:(\\S+)\\s*=\\s*\\\"(\\S*)\\\"", Pattern.MULTILINE);
    if (tryExtractNsDefinitions(textFragment, rdfxmlNamespaceDefinitionRegex)) {
      return list();
    }
    //try sparql
    Pattern sparqlNamespaceDefinitionRegex =
        Pattern.compile("(?i)prefix\\s+(\\S+)\\:\\s+<(\\S*)>", Pattern.MULTILINE);
    if (tryExtractNsDefinitions(textFragment, sparqlNamespaceDefinitionRegex)) {
      return list();
    }

    // unclear how to make it safe with jsonld while keeping it simple

    return list();

  }

  private boolean tryExtractNsDefinitions(@Name("prefix") String textFragment,
      Pattern pattern)
      throws InvalidNamespacePrefixDefinitionInDB, NamespacePrefixConflictException {
    Matcher m;
    m = pattern.matcher(textFragment);
    while (m.find()) {
      add(m.group(1).replace("-", "_"), m.group(2));
    }
    return m.matches();
  }

  @Procedure(mode = Mode.READ)
  @Description("Lists all existing namespace prefix definitions")
  public Stream<NamespacePrefixesResult> list() {

    return tx
        .execute("MATCH (n:NamespacePrefixDefinition) \n" +
            "UNWIND keys(n) AS prefix\n" +
            "RETURN prefix, n[prefix] AS namespace").stream().map(
            n -> new NamespacePrefixesResult((String) n.get("prefix"),
                (String) n.get("namespace")));

  }

  private boolean graphIsEmpty() {
    return !tx.execute("match (r:Resource) return id(r) limit 1").hasNext();
  }

  private class NsPrefixOperationNotAllowed extends Exception {
    public  NsPrefixOperationNotAllowed(String message){
      super(message);
    }
  }

}
