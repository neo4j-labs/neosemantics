package n10s.onto;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import n10s.RDFToLPGStatementProcessor;
import n10s.graphconfig.RDFParserConfig;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN_STRICT;
import static n10s.graphconfig.Params.DEFAULT_BASE_SCH_NS;

public class OntologyImporter extends RDFToLPGStatementProcessor {

  protected static final int SUB_CLASS_REST = 0;
  protected static final int EQUIV_CLASS_REST = 1;

  public static final Label RESOURCE = Label.label("Resource");
  protected static final List<IRI> ANNOTATION_PROPERTIES_TO_IMPORT = Arrays.asList(RDFS.LABEL, RDFS.COMMENT,
          SKOS.PREF_LABEL, SKOS.ALT_LABEL, SKOS.DEFINITION);
  protected static final List<IRI> PROPERTY_DECORATIONS_TO_IMPORT = Arrays.asList(OWL.TRANSITIVEPROPERTY,
          OWL.INVERSEFUNCTIONALPROPERTY, OWL.SYMMETRICPROPERTY, OWL.FUNCTIONALPROPERTY);

  protected Set<Statement> extraStatements = new HashSet<>();

  protected Map<BNode,OWLRestriction> allOpenRestrictions = new HashMap<>();
  protected Map<IRI,List<OWLRestriction>> openSubClassRestrictions = new HashMap<>();
  protected Map<IRI,List<OWLRestriction>> openEquivRestrictions = new HashMap<>();
  Cache<String, Node> nodeCache;

  public OntologyImporter(GraphDatabaseService db, Transaction tx,
      RDFParserConfig conf, Log l) {
    super(db, tx, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  @Override
  protected void periodicOperation() {

    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) {
      try (Transaction tempTransaction = graphdb.beginTx()) {
        namespaces.partialRefresh(tempTransaction);
        tempTransaction.commit();
        log.debug("namespace prefixes synced: " + namespaces.toString());
      } catch (Exception e) {
        log.error("Problems syncing up namespace prefixes in partial commit. ", e);
        if (getParserConfig().isAbortOnError()){
          throw new NamespacePrefixConflict("Problems syncing up namespace prefixes in partial commit. ", e);
        }
      }
    }

    try (Transaction tempTransaction = graphdb.beginTx()) {
      this.runPartialTx(tempTransaction);
      tempTransaction.commit();
      log.debug("partial commit: " + mappedTripleCounter + " triples ingested. Total so far: "
              + totalTriplesMapped);
      totalTriplesMapped += mappedTripleCounter;
    } catch (Exception e) {
      log.error("Problems when running partial commit. Partial transaction rolled back. "  + mappedTripleCounter + " triples lost.", e);
      if (getParserConfig().isAbortOnError()){
        throw new PartialCommitException("Problems when running partial commit. Partial transaction rolled back. " , e);
      }
    }

    mappedTripleCounter = 0;

  }

  @Override
  public void startRDF() throws RDFHandlerException{
    super.startRDF();
    //Loading an onto, using the default namespaces/prefixes when shortening
    if(parserConfig.getGraphConf().getHandleVocabUris()== GRAPHCONF_VOC_URI_SHORTEN ||
            parserConfig.getGraphConf().getHandleVocabUris()== GRAPHCONF_VOC_URI_SHORTEN_STRICT) {
      namespaces.add(parserConfig.getGraphConf().getBaseSchemaNamespacePrefix(),
              parserConfig.getGraphConf().getBaseSchemaNamespace());
      log.debug(
              "Added schema ns and prefix " + parserConfig.getGraphConf().getBaseSchemaNamespacePrefix() +
              ": " + parserConfig.getGraphConf().getBaseSchemaNamespace());
    }
  }

  @Override
  public void endRDF() throws RDFHandlerException {

    periodicOperation();

    // take away the unused extra triples (maybe too much for just returning a counter?)
    int unusedExtra = 0;
    for (Entry<String, Map<String, Object>> entry : resourceProps.entrySet()) {
      for (Entry<String, Object> values : entry.getValue().entrySet()) {
        if (values.getValue() instanceof List) {
          unusedExtra += ((List) values.getValue()).size();
        } else {
          unusedExtra++;
        }
      }
    }
    totalTriplesMapped -= unusedExtra;

    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();

  }

  @Override
  public void handleStatement(Statement st) {

    if (parserConfig.getPredicateExclusionList() == null || !parserConfig
        .getPredicateExclusionList()
        .contains(st.getPredicate().stringValue())) {
      if (st.getPredicate().equals(RDF.TYPE) && (st.getObject().equals(RDFS.CLASS) || st.getObject()
          .equals(OWL.CLASS)) && st.getSubject() instanceof IRI) {
        instantiate(vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getClassLabelName()),
            (IRI) st.getSubject());
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(RDF.TYPE) && (st.getObject().equals(RDF.PROPERTY) || st
          .getObject().equals(OWL.OBJECTPROPERTY)) && st.getSubject() instanceof IRI) {
        instantiate(vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getObjectPropertyLabelName()),
            (IRI) st.getSubject());
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(RDF.TYPE) && st.getObject().equals(OWL.DATATYPEPROPERTY)
          && st.getSubject() instanceof IRI) {
        instantiate(vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getDataTypePropertyLabelName()),
            (IRI) st.getSubject());
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(RDFS.SUBCLASSOF) && st
          .getSubject() instanceof IRI) {
        if (st.getObject() instanceof IRI) {
          instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
          addStatement(st);
        } else if (st.getObject() instanceof BNode){
          //object is a blank node, probably a restriction
          addLinkToClass((IRI)st.getSubject(), (BNode) st.getObject(), openSubClassRestrictions);
        }
        mappedTripleCounter++;
      }else if (st.getPredicate().equals(OWL.EQUIVALENTCLASS) && st
              .getSubject() instanceof IRI) {
        if (st.getObject() instanceof IRI) {
          instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
          addStatement(st);
        } else if (st.getObject() instanceof BNode){
          //object is a blank node, probably a restriction
          addLinkToClass((IRI)st.getSubject(), (BNode) st.getObject(),openEquivRestrictions);
        }
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(OWL.ONPROPERTY) && st
              .getSubject() instanceof BNode && st.getObject() instanceof IRI) {
        instantiate(vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getObjectPropertyLabelName()),
                (IRI) st.getObject());
        addRelToRestriction((BNode)st.getSubject(), (IRI)st.getObject());
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(OWL.SOMEVALUESFROM) && st
              .getSubject() instanceof BNode && st.getObject() instanceof IRI) {
        instantiate(vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getClassLabelName()),
                (IRI) st.getObject());
        addTargetToRestriction((BNode)st.getSubject(), (IRI)st.getObject(), OWL.SOMEVALUESFROM);
        mappedTripleCounter++;
      }else if (st.getPredicate().equals(OWL.ALLVALUESFROM) && st
              .getSubject() instanceof BNode && st.getObject() instanceof IRI) {
        instantiate(vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getClassLabelName()),
                (IRI) st.getObject());
        addTargetToRestriction((BNode)st.getSubject(), (IRI)st.getObject(), OWL.ALLVALUESFROM);
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(RDFS.SUBPROPERTYOF) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(RDFS.DOMAIN) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(RDFS.RANGE) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
        mappedTripleCounter++;
      } else if (ANNOTATION_PROPERTIES_TO_IMPORT.contains(st.getPredicate())
          && st.getSubject() instanceof IRI) {
        setProp(st.getSubject().stringValue(), vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), st.getPredicate().getLocalName()),
                (Literal) st.getObject());
        mappedTripleCounter++;
      }else if (st.getPredicate().equals(RDF.TYPE) && PROPERTY_DECORATIONS_TO_IMPORT.contains(st.getObject())
              && st.getSubject() instanceof IRI) {
        setProp(st.getSubject().stringValue(), vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), "propCharacteristics"),
                vf.createLiteral(((IRI)st.getObject()).getLocalName().substring(0,((IRI)st.getObject()).getLocalName().indexOf("Property"))));
        mappedTripleCounter++;
      }
    }
    totalTriplesParsed++;

    if (parserConfig.getCommitSize() != Long.MAX_VALUE
        && mappedTripleCounter != 0
        && mappedTripleCounter % parserConfig.getCommitSize() == 0) {
      periodicOperation();
    }

  }

  private void addTargetToRestriction(BNode rId, IRI target, IRI type) {
    //we are adding the relationship to a restriction
    OWLRestriction restr = (allOpenRestrictions.containsKey(rId)? allOpenRestrictions.get(rId): new OWLRestriction(rId));
    restr.setTarget(target);
    restr.setType(type);
    if(!allOpenRestrictions.containsKey(rId)){
      allOpenRestrictions.put(rId,restr);
    }
  }

  private void addRelToRestriction(BNode rId, IRI relName) {
    //we are adding the relationship to a restriction
    OWLRestriction restr = (allOpenRestrictions.containsKey(rId)? allOpenRestrictions.get(rId): new OWLRestriction(rId));
    restr.setRelName(relName);
    if(!allOpenRestrictions.containsKey(rId)){
      allOpenRestrictions.put(rId,restr);
    }
  }

  private void addLinkToClass(IRI subject, BNode rId, Map<IRI, List<OWLRestriction>> mapofLists) {
    //we are adding the link to the subject via SCO or EQC
    OWLRestriction restr = (allOpenRestrictions.containsKey(rId)? allOpenRestrictions.get(rId): new OWLRestriction(rId));
    if(!allOpenRestrictions.containsKey(rId)){
      allOpenRestrictions.put(rId,restr);
    }

    if(mapofLists.containsKey(subject)){
      mapofLists.get(subject).add(restr);
    } else {
      ArrayList<OWLRestriction> owlRestrictionList = new ArrayList<>();
      owlRestrictionList.add(restr);
      mapofLists.put(subject,owlRestrictionList);
    }

  }



  private void instantiate(IRI label, IRI iri) {
    setLabel(iri.stringValue(), handleIRI(label, LABEL));
    resourceProps.get(iri.stringValue()).put(handleIRI(
            vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), "name"),PROPERTY), iri.getLocalName());
  }

  private void instantiatePair(String label1, IRI iri1, String label2, IRI iri2) {
    setLabel(iri1.stringValue(), label1);
    resourceProps.get(iri1.stringValue()).put(handleIRI(
            vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), "name"),PROPERTY), iri1.getLocalName());
    setLabel(iri2.stringValue(), label2);
    resourceProps.get(iri2.stringValue()).put(handleIRI(
            vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), "name"),PROPERTY), iri2.getLocalName());
  }


  public Integer runPartialTx(Transaction inThreadTransaction) {

    for (Map.Entry<String, Set<String>> entry : resourceLabels.entrySet()) {
      try {
        if (!entry.getValue().isEmpty()) {
          // if the uri is for an element for which we have not parsed the
          // onto element type (class, property, rel) then it's an extra-statement
          // and should be processed when the element in question is parsed

          final Node node;
          node = nodeCache.get(entry.getKey(), new Callable<Node>() {
            @Override
            public Node call() {
              Node node = inThreadTransaction.findNode(RESOURCE, "uri", entry.getKey());
              if (node == null) {
                node = inThreadTransaction.createNode(RESOURCE);
                node.setProperty("uri", entry.getKey());
              }
              return node;
            }
          });

          entry.getValue().forEach(l -> node.addLabel(Label.label(l)));

          resourceProps.get(entry.getKey()).forEach((k, v) -> {
            //node.setProperty(k, v);
            if (v instanceof List) {
              Object currentValue = node.getProperty(k, null);
              if (currentValue == null) {
                node.setProperty(k, toPropertyValue(v));
              } else {
                if (currentValue.getClass().isArray()) {
                  Object[] properties = (Object[]) currentValue;
                  for (Object property : properties) {
                    ((List) v).add(property);
                    //here an exception can be raised if types are conflicting
                  }
                } else {
                  ((List) v).add(node.getProperty(k));
                }
                //we make it a set to remove duplicates. Semantics of multivalued props in RDF.
                node.setProperty(k,
                    toPropertyValue(((List) v).stream().collect(Collectors.toSet())));
              }
            } else {
              node.setProperty(k, v);
            }
          });
          //and after processing the props for all uris, then we clear them from resourceProps
          resourceProps.remove(entry.getKey());

        }
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }


    Iterator<Entry<IRI, List<OWLRestriction>>> scRestIter = openSubClassRestrictions.entrySet().iterator();
    while(scRestIter.hasNext()){
      Entry<IRI, List<OWLRestriction>> classAndRestrictions = scRestIter.next();
      Iterator<OWLRestriction> restIter = classAndRestrictions.getValue().iterator();
      while(restIter.hasNext()) {
        OWLRestriction rest = restIter.next();
        if (rest.isComplete()) {
          processRestriction(inThreadTransaction, classAndRestrictions.getKey(), rest, SUB_CLASS_REST);
          allOpenRestrictions.remove(rest.getRestrictionId());
          restIter.remove();
        }
      }
    }


//    openSubClassRestrictions.keySet().stream().forEach(c -> {
//      openSubClassRestrictions.get(c).forEach( rest -> {
//        processRestriction(inThreadTransaction, c, rest);
//        //TODO: Complete this<<<<<<<
//        //delete restriction (TODO: delete via iterator or mark for deletion)
//        openSubClassRestrictions.get(c).remove(rest);
//        allOpenRestrictions.remove(rest.getRestrictionId());
//      });
//    });

    Iterator<Entry<IRI, List<OWLRestriction>>> eqcRestIter = openEquivRestrictions.entrySet().iterator();
    while(eqcRestIter.hasNext()){
      Entry<IRI, List<OWLRestriction>> classAndRestrictions = eqcRestIter.next();
      Iterator<OWLRestriction> restIter = classAndRestrictions.getValue().iterator();
      while(restIter.hasNext()) {
        OWLRestriction rest = restIter.next();
        if (rest.isComplete()) {
          processRestriction(inThreadTransaction, classAndRestrictions.getKey(), rest, EQUIV_CLASS_REST);
          allOpenRestrictions.remove(rest.getRestrictionId());
          eqcRestIter.remove();
        }
      }
    }

//    openEquivRestrictions.keySet().stream().forEach(c -> {
//      openEquivRestrictions.get(c).forEach( rest -> {
//        processRestriction(inThreadTransaction, c, rest);
//        //TODO: Complete this<<<<<<<
//        //delete restriction (TODO: delete via iterator or mark for deletion)
//        openEquivRestrictions.get(c).remove(rest);
//        openEquivRestrictions.remove(rest.getRestrictionId());
//      });
//    });


    for (Statement st : statements) {
      try {
        final Node fromNode = nodeCache.get(st.getSubject().stringValue(), new Callable<Node>() {
          @Override
          public Node call() {  //throws AnyException
            return inThreadTransaction.findNode(RESOURCE, "uri", st.getSubject().stringValue());
          }
        });

        final Node toNode = nodeCache.get(st.getObject().stringValue(), new Callable<Node>() {
          @Override
          public Node call() {  //throws AnyException
            return inThreadTransaction.findNode(RESOURCE, "uri", st.getObject().stringValue());
          }
        });

        // check if the rel is already present. If so, don't recreate.
        // explore the node with the lowest degree
        boolean found = false;
        if (fromNode.getDegree(RelationshipType.withName(handleIRI(translateRelName(st.getPredicate()), RELATIONSHIP)),
            Direction.OUTGOING) <
            toNode.getDegree(RelationshipType.withName(handleIRI(translateRelName(st.getPredicate()), RELATIONSHIP)),
                Direction.INCOMING)) {
          for (Relationship rel : fromNode
              .getRelationships(Direction.OUTGOING,
                  RelationshipType.withName(handleIRI(translateRelName(st.getPredicate()),RELATIONSHIP)))) {
            if (rel.getEndNode().equals(toNode)) {
              found = true;
              break;
            }
          }
        } else {
          for (Relationship rel : toNode
              .getRelationships(Direction.INCOMING,
                  RelationshipType.withName(handleIRI(translateRelName(st.getPredicate()),RELATIONSHIP)))) {
            if (rel.getStartNode().equals(fromNode)) {
              found = true;
              break;
            }
          }
        }

        if (!found) {
          fromNode.createRelationshipTo(
              toNode,
              RelationshipType.withName(handleIRI(translateRelName(st.getPredicate()),RELATIONSHIP)));
        }
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    statements.clear();
    resourceLabels.clear();
    nodeCache.invalidateAll();
    return 0;
  }

  private void processRestriction(Transaction tx, IRI c, OWLRestriction rest, int type) {
      //process restriction
      try {
        final Node fromNode = nodeCache.get(c.stringValue(), new Callable<Node>() {
          @Override
          public Node call() {  //throws AnyException
            return tx.findNode(RESOURCE, "uri", c.stringValue());
          }
        });

        final Node toNode = nodeCache.get(rest.getTargetClass().stringValue(), new Callable<Node>() {
          @Override
          public Node call() {  //throws AnyException
            return tx.findNode(RESOURCE, "uri", rest.getTargetClass().stringValue());
          }
        });

        //check if an identical restriction exists already, if not create
        boolean found = false;
        for (Relationship rel : fromNode
                .getRelationships(Direction.OUTGOING,
                        RelationshipType.withName(handleIRI(vf.createIRI(DEFAULT_BASE_SCH_NS +
                                getRestrictionRelName(type)), RELATIONSHIP)))) {
          if (rel.getEndNode().equals(toNode)) {
            found = rel.getProperty("onPropertyURI").equals(rest.getRelName().stringValue()) &&
                    rel.getProperty("onPropertyName").equals(rest.getRelName().getLocalName()) &&
                    rel.getProperty("restrictionType").equals(getTypeAsString(rest));
            break;
          }
        }

        if (!found) {

          Relationship restrictionRel = fromNode.createRelationshipTo(
                  toNode,
                  RelationshipType.withName(handleIRI(vf.createIRI(DEFAULT_BASE_SCH_NS + getRestrictionRelName(type)), RELATIONSHIP)));
          restrictionRel.setProperty("onPropertyURI", rest.getRelName().stringValue());
          restrictionRel.setProperty("onPropertyName", rest.getRelName().getLocalName());
          restrictionRel.setProperty("restrictionType", getTypeAsString(rest));

        }
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
  }

  protected String getRestrictionRelName(int type) {
    String relName;
    switch (type) {
      case SUB_CLASS_REST:  relName = "SCO_RESTRICTION";
        break;
      case EQUIV_CLASS_REST:  relName = "EQC_RESTRICTION";
        break;
      default: relName = "RESTRICTION";
        break;
    }
    return relName;
  }

  protected String getTypeAsString(OWLRestriction rest) {
    if(rest.getType().equals(OWL.SOMEVALUESFROM)){
      return "SOME";
    } else if (rest.getType().equals(OWL.ALLVALUESFROM)){
      return "ALL";
    } else {
      return "UNKNOWN";
    }
  }

  protected IRI translateRelName(IRI iri) {
    if (iri.equals(RDFS.SUBCLASSOF)) {
      return vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getSubClassOfRelName());
    } else if (iri.equals(RDFS.SUBPROPERTYOF)) {
      return vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getSubPropertyOfRelName());
    } else if (iri.equals(OWL.EQUIVALENTCLASS)) {
      return vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), "EQC");//parserConfig.getGraphConf().getEquivalentClassRelName()
    } else if (iri.equals(RDFS.DOMAIN)) {
      return vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getDomainRelName());
    } else if (iri.equals(RDFS.RANGE)) {
      return vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), parserConfig.getGraphConf().getRangeRelName());
    } else {
      //Not valid
      return vf.createIRI(parserConfig.getGraphConf().getBaseSchemaNamespace(), "REL");
    }
  }

}
