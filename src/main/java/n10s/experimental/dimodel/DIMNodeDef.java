package n10s.experimental.dimodel;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XSD;

import java.util.*;

public class DIMNodeDef {

    public IRI nodeId;
    public Map<IRI, IRI> props;
    public Map<IRI,Set<IRI>> rels;
    public long x;
    public long y;

    public DIMNodeDef(IRI nodeId) {
        this.nodeId = nodeId;
        this.props = new HashMap<>();
        this.props.put(RDFS.LABEL, XSD.STRING);
        this.props.put(RDFS.COMMENT, XSD.STRING);
        this.props.put(SimpleValueFactory.getInstance().createIRI("neo4j://graph.schema#uri"), XSD.STRING);
        this.rels = new HashMap<>();
    }

    public String toString(){
        return "\nID: " + nodeId.getLocalName() + " \n - PROPS:" + mapAsString(props) + "\n - RELS:" + mapOfSetsAsString(rels) ;
    }

    private String mapAsString(Map<IRI, IRI> m) {
        StringBuilder sb = new StringBuilder();
        m.entrySet().forEach( x -> sb.append("\t").append(((IRI)x.getKey()).getLocalName()).append(":")
                .append((IRI)x.getValue()!=null?((IRI)x.getValue()).getLocalName():" - "));
        return sb.toString();
    }

    private String mapOfSetsAsString(Map<IRI, Set<IRI>> m) {
        StringBuilder sb = new StringBuilder();
        m.entrySet().forEach( x -> { sb.append("\t").append(((IRI)x.getKey()).getLocalName()).append(":");
                x.getValue().forEach(r -> sb.append("\t").append(r.getLocalName())) ; } );
        return sb.toString();
    }

    public Map<String, Object> getNodeSchemasAsJsonObject(){
        Map<String, Object> map = new HashMap<>();
        map.put("label",nodeId.getLocalName());
        map.put("additionLabels", new ArrayList<>());
        map.put("labelProperties", new ArrayList<>());
        List<Object> properties = new ArrayList<>();
        map.put("properties", properties);
        props.forEach( (k, v) -> {
            Map<String, Object> prop = new HashMap<>();
            prop.put("property", k.getLocalName());
            prop.put("type", v!=null?v.getLocalName():"string");
            prop.put("identifier", k.stringValue());
            properties.add(prop);
        });
        Map<String, Object> key = new HashMap<>();
        map.put("key", key);
        key.put("properties", Collections.EMPTY_LIST);
        key.put("name","");

        return map;
    }

    public Map<String, Object> getRelSchemasAsJsonObject(){
        Map<String, Object> map = new HashMap<>();
        rels.forEach( (k,v) -> {
            v.forEach(r -> {
                Map<String, Object> prop = new HashMap<>();
                prop.put("type", k.getLocalName());
                prop.put("sourceNodeSchema", nodeId.stringValue());
                prop.put("targetNodeSchema", r.stringValue());
                prop.put("properties", Collections.EMPTY_LIST);
                map.put(nodeId.stringValue()+k.stringValue()+r.stringValue(),prop);
            });
        });
        return map;
    }

    public Map<String, Object> getNodeMappingsAsJsonObject(){
        Map<String, Object> map = new HashMap<>();
        map.put("nodeSchema",nodeId.stringValue());
        map.put("mappings", new ArrayList<>());

        return map;
    }

    public Map<String, Object> getRelsMappingsAsJsonObject(){
        Map<String, Object> map = new HashMap<>();
        rels.forEach( (k,v) -> {
            v.forEach( r -> {
                Map<String, Object> relMap = new HashMap<>();
                relMap.put("relationshipSchema", nodeId.stringValue()+k.stringValue()+r.stringValue());
                relMap.put("mappings", Collections.EMPTY_LIST);
                relMap.put("sourceMappings", Collections.EMPTY_LIST);
                relMap.put("targetMappings", Collections.EMPTY_LIST);
                map.put(nodeId.stringValue()+k.stringValue()+r.stringValue(),relMap);
            });

        });
        return map;
    }

    public Map<String,Object> getGraphNodeAsJsonObject() {
        Map<String, Object> map = new HashMap<>();
        map.put("id",nodeId.stringValue());
        Map<String, Object> position = new HashMap<>();
        position.put("x",this.x);
        position.put("y",this.y);
        map.put("position", position);
        map.put("caption", nodeId.getLocalName());

        return map;
    }

    public List<Object> getGraphRelsAsJsonObject() {
        List<Object> graphrels = new ArrayList<>();
        rels.forEach( (k,v) -> {
            v.forEach(r -> {
                Map<String, Object> rel = new HashMap<>();
                rel.put("id", nodeId.stringValue()+k.stringValue()+r.stringValue());
                rel.put("type", k.getLocalName());
                rel.put("fromId", nodeId.stringValue());
                rel.put("toId", r.stringValue());
                graphrels.add(rel);
            });
        });

        return graphrels;
    }

    public void addProp(IRI prop, IRI datatype) {
        this.props.put(prop, datatype);
    }

    public void addRel(IRI rel, IRI range) {
        if(this.rels.containsKey(rel)){
            Set<IRI> existingSet = this.rels.get(rel);
            existingSet.add(range);
        } else {
            Set<IRI> newSet = new HashSet<>();
            newSet.add(range);
            this.rels.put(rel, newSet);
        }
    }

    public void setPos(long x, long y){
        this.x = x;
        this.y = y;
    }

    public int getRelCount(){
        return this.rels.values().stream().map( s-> s.size()).reduce(0, Integer::sum);
    }

}
