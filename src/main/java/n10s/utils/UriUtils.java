package n10s.utils;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_MODE_LPG;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_MAP;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN_STRICT;
import static n10s.graphconfig.Params.*;


import n10s.graphconfig.GraphConfig;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Collections;

public class UriUtils {

  public static String translateUri(String uri, Transaction tx, GraphConfig gc)
      throws InvalidNamespacePrefixDefinitionInDB, UriNamespaceHasNoAssociatedPrefix {
    // MAP mode must be checked before the LPG mode check because getGraphMode() returns LPG for MAP.
    if (gc != null && gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP) {
      return translateUriForMapMode(uri, tx);
    } else if (gc == null || gc.getGraphMode() == GRAPHCONF_MODE_LPG) {
      if ((gc!=null?gc.getBaseSchemaNamespace():DEFAULT_BASE_SCH_NS).equals(uri.substring(0,URIUtil.getLocalNameIndex(uri)))){
        return uri.substring(URIUtil.getLocalNameIndex(uri));
      } else if(uri.equals(RDF.TYPE.stringValue())){
        return RDF.TYPE.getLocalName();
      } else {
        throw new UriNamespaceHasNoAssociatedPrefix(
                "The graph is not namespace aware. Use <neo4j://graph.schema#> instead of namespaces like " +
                        "<" + uri.substring(0,URIUtil.getLocalNameIndex(uri)) + "> that will be stripped off. " +
                        "Alternatively, use a namespace aware mode of operation.");
        //return NOT_MATCHING_NS;
      }

    } else if (gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN ||
        gc.getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT) {
      return getShortForm(uri, tx);
    } else {
      //it's GRAPHCONF_VOC_URI_KEEP
      return uri;
    }
  }
  public static String getShortForm(String str, Transaction tx)
      throws UriNamespaceHasNoAssociatedPrefix, InvalidNamespacePrefixDefinitionInDB {
    IRI iri = SimpleValueFactory.getInstance().createIRI(str);
    NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
    if (!prefixDefs.hasNs(iri.getNamespace())) {
      throw new UriNamespaceHasNoAssociatedPrefix(
          "Prefix Undefined: No prefix defined for namespace <" + str
              + ">. Use n10s.nsprefixes.add(...) procedure.");
    }
    return prefixDefs.getPrefixForNs(iri.getNamespace()) + PREFIX_SEPARATOR + iri.getLocalName();
  }

  private static String translateUriForMapMode(String uri, Transaction tx) {
    // Mirror the importer's mapElement() logic: check explicit mappings first, fall back to local name.
    Result mappingResult = tx.execute(
        "MATCH (mp:_MapDef)-[:_IN]->(mns:_MapNs) WHERE mns._ns + mp._local = $uri RETURN mp._key AS key",
        Collections.singletonMap("uri", uri));
    if (mappingResult.hasNext()) {
      return (String) mappingResult.next().get("key");
    }
    return SimpleValueFactory.getInstance().createIRI(uri).getLocalName();
  }

  public static class UriNamespaceHasNoAssociatedPrefix extends Exception {

    public UriNamespaceHasNoAssociatedPrefix(String message) {
      super(message);
    }
  }
}
