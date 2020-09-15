package n10s.rdf.export;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public abstract class ExportProcessor {

  protected Transaction tx;
  protected GraphDatabaseService graphdb;
  protected final ValueFactory vf = SimpleValueFactory.getInstance();
  protected boolean exportPropertiesInRels;
  protected GraphConfig graphConfig;

  public ExportProcessor(Transaction tx, GraphDatabaseService graphdb, GraphConfig gc) {
    this.tx = tx;
    this.graphdb = graphdb;
    this.graphConfig = gc;
  }

  public Stream<Statement> streamTriplesFromCypher(String cypher, Map<String, Object> params) {

    final Result result = this.tx.execute(cypher, params);
    Map<Long, IRI> ontologyEntitiesUris = new HashMap<>();

    Set<Long> serializedNodeIds = new HashSet<>();
    return result.stream().flatMap(row -> {
      Set<Statement> rowResult = new HashSet<>();
      Set<Entry<String, Object>> entries = row.entrySet();

      List<Node> nodes = new ArrayList<>();
      List<Relationship> rels = new ArrayList<>();
      List<Path> paths = new ArrayList<>();

      for (Entry<String, Object> entry : entries) {
        Object o = entry.getValue();
        if (o instanceof Node) {
          nodes.add((Node) o);
        } else if (o instanceof Relationship) {
          rels.add((Relationship) o);
        } else if (o instanceof Path) {
          paths.add((Path) o);
        } else if (o instanceof List){
          // This is ugly. Only processes list but not list of lists... or maps... etc...
          // that said, it should be good enough for an export feature.
          ((List) o).stream().forEach( x ->  { if (x instanceof Node) {
            nodes.add((Node) x);
          } else if (x instanceof Relationship) {
            rels.add((Relationship) x);
          } else if (x instanceof Path) {
            paths.add((Path) x);
          } } );
        }
        //  if it's not a node, a  rel or a path or a collection thereof...
        //  then it cannot be converted to triples so we ignore it
      }

      for (Node node : nodes) {
        //Do we need to keep a list of serializednodeids?
        if (!serializedNodeIds.contains(node.getId()) && !filterNode(node, ontologyEntitiesUris)) {
          serializedNodeIds.add(node.getId());
          rowResult.addAll(processNode(node, ontologyEntitiesUris, null));
        }
      }

      for (Relationship rel : rels) {
        if( !filterRelationship(rel,ontologyEntitiesUris)) {
          Statement baseStatement = processRelationship(rel, ontologyEntitiesUris);
          rowResult.add(baseStatement);
          if(this.exportPropertiesInRels) {
            rel.getAllProperties()
                .forEach((k, v) -> processPropOnRel(rowResult, baseStatement, k, v));
          }
        }
      }

      for (Path p : paths) {
        p.iterator().forEachRemaining(propertyContainer -> {
              if (propertyContainer instanceof Node) {
                Node node = (Node) propertyContainer;
                if (!serializedNodeIds.contains(node.getId())&&!filterNode(node,  ontologyEntitiesUris)) {
                  serializedNodeIds.add(node.getId());
                  rowResult.addAll(processNode(node, ontologyEntitiesUris, null));
                }
              } else if (propertyContainer instanceof Relationship &&
                  !filterRelationship((Relationship) propertyContainer, ontologyEntitiesUris)) {
                Statement baseStatement = processRelationship((Relationship) propertyContainer, ontologyEntitiesUris);
                rowResult.add(baseStatement);
                if(this.exportPropertiesInRels) {
                  propertyContainer.getAllProperties()
                      .forEach((k, v) -> processPropOnRel(rowResult, baseStatement, k, v));
                }
              }
            }
        );
      }

      return rowResult.stream();

    });
  }

  protected Value getValueFromTriplePatternObject(TriplePattern tp) {
    Value object;
    if (tp.getLiteral()) {
      if (tp.getLiteralLang() != null) {
        object = vf.createLiteral(tp.getObject(), tp.getLiteralLang());
      } else {
        object = vf.createLiteral(tp.getObject(), vf.createIRI(tp.getLiteralType()));
      }
    } else {
      object = vf.createIRI(tp.getObject());
    }
    return object;
  }

  protected abstract boolean filterRelationship(Relationship rel, Map<Long, IRI> ontologyEntitiesUris);

  protected abstract boolean filterNode(Node node, Map<Long, IRI> ontologyEntitiesUris);

  protected abstract void processPropOnRel(Set<Statement> rowResult, Statement baseStatement, String key, Object val);

  protected abstract Statement processRelationship(Relationship rel, Map<Long, IRI> ontologyEntitiesUris);

  protected abstract Set<Statement> processNode(Node node, Map<Long, IRI> ontologyEntitiesUris, String propNameFilter);

  public abstract Stream<Statement> streamTriplesFromTriplePattern(TriplePattern tp)
      throws InvalidNamespacePrefixDefinitionInDB;
}
