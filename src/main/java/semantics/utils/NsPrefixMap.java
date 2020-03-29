package semantics.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.lang.String;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class NsPrefixMap {

  private static Map<String, String> standardPrefixes = createStandardPrefixesMap();

  private static Map<String, String> createStandardPrefixesMap() {
    Map<String,String> ns = new HashMap<>();
    ns.put("http://schema.org/", "sch");
    ns.put("http://purl.org/dc/elements/1.1/", "dc");
    ns.put("http://purl.org/dc/terms/", "dct");
    ns.put("http://www.w3.org/2004/02/skos/core#", "skos");
    ns.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs");
    ns.put("http://www.w3.org/2002/07/owl#", "owl");
    ns.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
    ns.put("http://www.w3.org/ns/shacl#", "sh");
    return ns;
  };

  Map<String,String> prefixToNs = new HashMap<>();
  Map<String,String> nsToPrefix = new HashMap<>();

  public NsPrefixMap(Transaction tx) throws InvalidNamespacePrefixDefinitionInDB {
    try {
      Node nsPrefDefNode;

      ResourceIterator<Node> namespacePrefixDefinitionNodes = tx
          .findNodes(Label.label("NamespacePrefixDefinition"));

      if(namespacePrefixDefinitionNodes.hasNext()) {
        nsPrefDefNode = namespacePrefixDefinitionNodes.next();
        tx.acquireWriteLock(nsPrefDefNode);
      } else {
        nsPrefDefNode = (Node) tx.execute("MERGE (n:NamespacePrefixDefinition) RETURN n ")
            .next().get("n");
      }

      for (Entry<String, Object> entry: nsPrefDefNode.getAllProperties().entrySet()) {
        add(entry.getKey(), (String) entry.getValue());
      }

    } catch(NamespacePrefixConflictException e){
      throw new InvalidNamespacePrefixDefinitionInDB("The namespace prefix definition in the DB "
          + "is invalid. Check the 'NamespacePrefixDefinition' node. Detail: " + e.getMessage());
    }
  }

  public String getNsForPrefix(String prefix){
    return prefixToNs.get(prefix);
  }

  public String getPrefixForNs(String ns){
    return nsToPrefix.get(ns);
  }

  public boolean hasPrefix(String prefix){
    return prefixToNs.containsKey(prefix);
  }

  public boolean hasNs(String ns){
    return nsToPrefix.containsKey(ns);
  }

  public String getPrefixOrAdd(String ns) throws NamespacePrefixConflictException {
    if (nsToPrefix.containsKey(ns)) {
      return nsToPrefix.get(ns);
    } else {
      //is it a standard? use std  prefix
      if(standardPrefixes.containsKey(ns)) {
        add(standardPrefixes.get(ns), ns);
        return standardPrefixes.get(ns);
      } else {
        //it's not a standard, we need to generate next in sequence
        String nextNsPrefix = "ns" + prefixToNs.keySet().stream().filter(x -> x.startsWith("ns")).count();
        add(nextNsPrefix, ns);
        return nextNsPrefix;
      }
    }
  }

  public void add(String prefix, String ns) throws NamespacePrefixConflictException {
    if (!prefixToNs.containsKey(prefix) && !nsToPrefix.containsKey(ns)){
      prefixToNs.put(prefix,ns);
      nsToPrefix.put(ns,prefix);
    } else if (prefixToNs.containsKey(prefix) && !prefixToNs.get(prefix).equals(ns)){
      throw new NamespacePrefixConflictException("prefix " + prefix + " is in use for namespace <" + prefixToNs.get(prefix) + ">");
    }else if (nsToPrefix.containsKey(ns) && ! nsToPrefix.get(ns).equals(prefix)){
      throw new NamespacePrefixConflictException("namespace <" + ns + "> has already an associated prefix (" + nsToPrefix.get(ns)+ ")");
    }
  }

  public void removePrefix(String prefix) {
    if(prefixToNs.containsKey(prefix)) {
      nsToPrefix.remove(prefixToNs.get(prefix));
      prefixToNs.remove(prefix);
    }
  }

  public void removeNamespace(String ns) {
    if(nsToPrefix.containsKey(ns)) {
      prefixToNs.remove(nsToPrefix.get(ns));
      nsToPrefix.remove(ns);
    }
  }

  public Set<String> getPrefixes() {
    return prefixToNs.keySet();
  }

  public Set<String> getNamespaces() {
    return nsToPrefix.keySet();
  }

  public Map<String,String> getPrefixToNs(){
    return prefixToNs;
  }

  public Map<String,String> getNsToPrefix(){
    return nsToPrefix;
  }

  public void flushToDB(Transaction tx){
    Node nsPrefDefNode;

    ResourceIterator<Node> namespacePrefixDefinitionNodes = tx
        .findNodes(Label.label("NamespacePrefixDefinition"));

    if(namespacePrefixDefinitionNodes.hasNext()) {
      nsPrefDefNode = namespacePrefixDefinitionNodes.next();
    } else {
      nsPrefDefNode = (Node) tx.execute("MERGE (n:NamespacePrefixDefinition) RETURN n ")
          .next().get("n");
    }
    //remove all properties
    //TODO: should  we delta them? We probably should. Remember it's a bijection
    nsPrefDefNode.getAllProperties().forEach((k,v) -> nsPrefDefNode.removeProperty(k));
    //and reset them
    for (Entry<String,String> entry : prefixToNs.entrySet()) {
      nsPrefDefNode.setProperty(entry.getKey(),entry.getValue());
    }

  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, String> pair: prefixToNs.entrySet()) {
      sb.append(pair.getKey() + ": <" + pair.getValue() + ">");
    }
    return sb.toString();
  }

}
