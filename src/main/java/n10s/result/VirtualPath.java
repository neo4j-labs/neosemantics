package n10s.result;

import org.apache.commons.collections.CollectionUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Paths;

import java.util.*;

import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicReference;


//import static org.apache.commons.collections4.IterableUtils.reversedIterable;


public class VirtualPath implements Path {
  private final Node start;
  private final List<Relationship> relationships;

  public VirtualPath(Node start) {
    this(start, new ArrayList<>());
  }

  private VirtualPath(Node start, List<Relationship> relationships) {
    Objects.requireNonNull(start);
    Objects.requireNonNull(relationships);
    this.start = start;
    this.relationships = relationships;
  }

  public void addRel(Relationship relationship) {
    Objects.requireNonNull(relationship);
    requireConnected(relationship);
    this.relationships.add(relationship);
  }

  @Override
  public Node startNode() {
    return start;
  }

  @Override
  public Node endNode() {
    return reverseNodes().iterator().next();
  }

  @Override
  public Relationship lastRelationship() {
    return relationships.isEmpty() ? null : relationships.get(relationships.size() - 1);
  }

  @Override
  public Iterable<Relationship> relationships() {
    return relationships;
  }

  @Override
  public Iterable<Relationship> reverseRelationships() {
    return relationships();
    //return reversedIterable(relationships());
  }

  @Override
  public Iterable<Node> nodes() {
    List<Node> nodes = new ArrayList<>();
    nodes.add(start);

    AtomicReference<Node> currNode = new AtomicReference<>(start);
    final List<Node> otherNodes = relationships.stream().map(rel -> {
      final Node otherNode = rel.getOtherNode(currNode.get());
      currNode.set(otherNode);
      return otherNode;
    }).collect(Collectors.toList());

    nodes.addAll(otherNodes);
    return nodes;
  }

  @Override
  public Iterable<Node> reverseNodes() {
    return nodes();
    //return reversedIterable(nodes());
  }

  @Override
  public int length() {
    return relationships.size();
  }

  @Override
  public Iterator<Entity> iterator() {
    return new Iterator<>() {
      Iterator<? extends Entity> current = nodes().iterator();
      Iterator<? extends Entity> next = relationships().iterator();

      @Override
      public boolean hasNext() {
        return current.hasNext();
      }

      @Override
      public Entity next() {
        try {
          return current.next();
        }
        finally {
          Iterator<? extends Entity> temp = current;
          current = next;
          next = temp;
        }
      }

      @Override
      public void remove() {
        next.remove();
      }
    };
  }

  @Override
  public String toString() {
    return Paths.defaultPathToString(this);
  }

  private void requireConnected(Relationship relationship) {
    final List<Node> previousNodes = getPreviousNodes();
    boolean isRelConnectedToPrevious = CollectionUtils.containsAny( previousNodes, Arrays.asList(relationship.getNodes()) );
    if (!isRelConnectedToPrevious) {
      throw new IllegalArgumentException("Relationship is not part of current path.");
    }
  }

  private List<Node> getPreviousNodes() {
    Relationship previousRelationship = lastRelationship();
    if (previousRelationship != null) {
      return Arrays.asList(previousRelationship.getNodes());
    }
    return List.of(endNode());
  }

  public static final class Builder {
    private final Node start;
    private final List<Relationship> relationships = new ArrayList<>();

    public Builder(Node start) {
      this.start = start;
    }

    public Builder push(Relationship relationship) {
      this.relationships.add(relationship);
      return this;
    }

    public VirtualPath build() {
      return new VirtualPath(start, relationships);
    }

  }
}