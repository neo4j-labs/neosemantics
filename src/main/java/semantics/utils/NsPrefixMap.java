package semantics.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.lang.String;
import java.util.concurrent.Callable;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class NsPrefixMap implements Callable<Integer> {

  private static Map<String,String> standardNamespaces = createStandardNamespacesMap();

  private static Map<String,String> createStandardNamespacesMap() {
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

  private static Map<String,String> standardPrefixes = createStandardPrefixesMap();

  private static Map<String,String> createStandardPrefixesMap() {
    Map<String,String> ns = new HashMap<>();
    ns.put("sch","http://schema.org/");
    ns.put("dc","http://purl.org/dc/elements/1.1/");
    ns.put("dct","http://purl.org/dc/terms/");
    ns.put("skos","http://www.w3.org/2004/02/skos/core#");
    ns.put("rdfs","http://www.w3.org/2000/01/rdf-schema#");
    ns.put("owl","http://www.w3.org/2002/07/owl#");
    ns.put("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    ns.put("sh","http://www.w3.org/ns/shacl#");
    return ns;
  };

  private Transaction tx;
  Map<String,String> prefixToNs = new HashMap<>();
  Map<String,String> nsToPrefix = new HashMap<>();
  Lock lock;

  public NsPrefixMap(Transaction tx, boolean acquireLock) throws InvalidNamespacePrefixDefinitionInDB {
    try {

      this.tx = tx;
      ResourceIterator<Node> namespacePrefixDefinitionNodes = tx
          .findNodes(Label.label("NamespacePrefixDefinition"));

      if(namespacePrefixDefinitionNodes.hasNext()) {
        Node nspd = namespacePrefixDefinitionNodes.next();
        if(acquireLock) {
          lock = tx.acquireWriteLock(nspd);
        }

        for (Entry<String, Object> entry: nspd.getAllProperties().entrySet()) {
          add(entry.getKey(), (String) entry.getValue());
        }

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
      if(standardNamespaces.containsKey(ns)) {
        add(standardNamespaces.get(ns), ns);
        return standardNamespaces.get(ns);
      } else {
        //it's not a standard, we need to generate next in sequence
        String nextNsPrefix = "ns" + prefixToNs.keySet().stream().filter(x -> x.startsWith("ns")).count();
        add(nextNsPrefix, ns);
        return nextNsPrefix;
      }
    }
  }

  public void add(String prefix, String ns) throws NamespacePrefixConflictException {
    if (standardPrefixes.containsKey(prefix) && !standardPrefixes.get(prefix).equals(ns)){
      throw new NamespacePrefixConflictException( "Invalid prefix + namespace combination: "
          + prefix + " is a reserved namespace prefix for <" + standardPrefixes.get(prefix) + ">");
    } else if (standardNamespaces.containsKey(ns) && !standardNamespaces.get(ns).equals(prefix)){
      throw new NamespacePrefixConflictException( "Invalid prefix + namespace combination: "
          + ns + " is a standard namespace that has the associated standard prefix " + standardNamespaces.get(ns));
    } else if (!prefixToNs.containsKey(prefix) && !nsToPrefix.containsKey(ns)){
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

  public void flushToDB(){
    Node nsPrefDefNode;

    ResourceIterator<Node> namespacePrefixDefinitionNodes = tx
        .findNodes(Label.label("NamespacePrefixDefinition"));

    if(namespacePrefixDefinitionNodes.hasNext()) {
      nsPrefDefNode = namespacePrefixDefinitionNodes.next();
      tx.acquireWriteLock(nsPrefDefNode);
      // get the latest from the DB and update it.
      Map<String, String> nsPrefDefInDB = new HashMap<>();

      for (Entry<String, Object> entry : nsPrefDefNode.getAllProperties().entrySet()) {
        if (!prefixToNs.containsKey(entry.getKey()) || !prefixToNs.get(entry.getKey())
            .equals(entry.getValue())) {
          //it's been removed or replaced, we remove it from the DB
          nsPrefDefNode.removeProperty(entry.getKey());
        } else{
          nsPrefDefInDB.put(entry.getKey(), (String) entry.getValue());
        }
      }

      //and add the new values
      for (Entry<String,String> entry : prefixToNs.entrySet()) {
        //if  it's not in the latest from the DB then we add it.
        if(!nsPrefDefInDB.containsKey(entry.getKey())) {
          nsPrefDefNode.setProperty(entry.getKey(), entry.getValue());
        }
      }

    } else if (!prefixToNs.isEmpty()){
      nsPrefDefNode = (Node) tx.execute("MERGE (n:NamespacePrefixDefinition) RETURN n ")
          .next().get("n");
      for (Entry<String,String> entry : prefixToNs.entrySet()) {
          nsPrefDefNode.setProperty(entry.getKey(), entry.getValue());
      }

    }



  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, String> pair: prefixToNs.entrySet()) {
      sb.append(pair.getKey() + ": <" + pair.getValue() + ">");
    }
    return sb.toString();
  }

  private boolean reloadFromDB() throws DynamicNamespacePrefixConflict {
    Node nsPrefDefNode;

    ResourceIterator<Node> namespacePrefixDefinitionNodes = tx
        .findNodes(Label.label("NamespacePrefixDefinition"));

    if(namespacePrefixDefinitionNodes.hasNext()) {
      nsPrefDefNode = namespacePrefixDefinitionNodes.next();

      // get the latest from the DB and update it.
      for (Entry<String,Object> entry :nsPrefDefNode.getAllProperties().entrySet()) {
        if (!prefixToNs.containsKey(entry.getKey()) &&  !nsToPrefix.containsKey(entry.getValue())){
          //it's a new entry. We get it.
          add(entry.getKey(), (String) entry.getValue());
        } else if (prefixToNs.containsKey(entry.getKey()) &&  !nsToPrefix.containsKey(entry.getValue())) {
          throw new DynamicNamespacePrefixConflict(
              "Prefix " + entry.getKey() + " is already in use for namespace <" +
                  entry.getValue() + ">");
        } else if (!prefixToNs.containsKey(entry.getKey()) &&  nsToPrefix.containsKey(entry.getValue())){
          throw new DynamicNamespacePrefixConflict("An alternative prefix (" + entry.getKey() + ") is already in use for namespace <" +
              entry.getValue() + ">");
        }
      }
    }

    return true;
  }


  @Override
  public Integer call() throws Exception {

    if(reloadFromDB()) {
      flushToDB();
      return 0;
    }
    return 1;
  }


}
