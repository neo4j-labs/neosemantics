package n10s.rdf.load;

import n10s.graphconfig.GraphConfig;
import n10s.rdf.RDFProcedures;
import n10s.result.NodeResult;
import n10s.result.RelationshipResult;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.NsPrefixMap;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.PointValue;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static n10s.graphconfig.Params.*;

public class AddProcedures  extends RDFProcedures {

    @Procedure(name = "n10s.add.node", mode = Mode.WRITE)
    @Description("creates a node in the graph following the existing GraphConfig.")
    public Stream<NodeResult> nodeAdd(@Name("uri") String uri,
                                          @Name("types") List<String> types,
                                          @Name("properties") List<Map<String, Object>> props)
            throws GraphConfig.GraphConfigNotFound, RDFImportPreRequisitesNotMet, IOException,
            InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName, InvalidURI {

        StringBuilder rdfSerialization = new StringBuilder();
        IRI subject = SimpleValueFactory.getInstance().createIRI(uri);
        for (String type : types) {
            rdfSerialization.append('<').append(subject.stringValue()).append('>').append(' ')
                    .append('<').append(RDF.TYPE.stringValue()).append('>').append(' ')
                    .append('<').append(processTypeAsString(type)).append('>').append(' ').append('.').append('\n');
        }

        for (Map<String, Object> x : props) {
            if (x.containsKey("key") && x.containsKey("val")) {
                rdfSerialization.append('<').append(subject.stringValue()).append('>').append(' ')
                        .append('<').append(processPropKey(x.get("key"))).append('>').append(' ')
                        .append(processValue(x.get("val"), x.get("dt"), x.get("lan"))).append(' ').append('.').append('\n');
            }
        }

        DirectNodeAdder nodeAdder = (DirectNodeAdder) doAdd(rdfSerialization.toString(), new HashMap<String, Object>(), RDFFormat.NTRIPLES, false);
        return  Stream.of(nodeAdder.returnNode());

    }

    @Procedure(name = "n10s.add.relationship.nodes", mode = Mode.WRITE)
    @Description("creates a relationship in the graph following the existing GraphConfig.")
    public Stream<RelationshipResult> relAdd(@Name("from") Node fromNode,
                                             @Name("type") String relType,
                                             @Name("properties") List<Map<String, Object>> props,
                                             @Name("to") Node toNode)
            throws GraphConfig.GraphConfigNotFound, RDFImportPreRequisitesNotMet, IOException,
            InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName, InvalidURI, NodeIsNotResource {

        StringBuilder rdfSerialization = new StringBuilder();
        if (fromNode.hasProperty("uri") && fromNode.hasLabel(Label.label("Resource")) &&
                toNode.hasProperty("uri") && toNode.hasLabel(Label.label("Resource"))) {
            String processedRelType = processTypeAsString(relType);
            IRI fromUri = SimpleValueFactory.getInstance().createIRI((String)fromNode.getProperty("uri"));
            IRI toUri = SimpleValueFactory.getInstance().createIRI((String)toNode.getProperty("uri"));
            rdfSerialization.append('<').append(fromUri.stringValue()).append('>').append(' ')
                        .append('<').append(processedRelType).append('>').append(' ')
                    .append('<').append(toUri.stringValue()).append('>').append(' ').append('.').append('\n');

            for (Map<String, Object> x : props) {
                if (x.containsKey("key") && x.containsKey("val")) {
                    rdfSerialization.append('<').append('<').append(' ')
                            .append('<').append(fromUri.stringValue()).append('>').append(' ')
                            .append('<').append(processedRelType).append('>').append(' ')
                            .append('<').append(toUri.stringValue()).append('>')
                            .append(' ').append('>').append('>').append(' ')
                            .append('<').append(processPropKey(x.get("key"))).append('>').append(' ')
                            .append(processValue(x.get("val"), x.get("dt"), x.get("lan"))).append(' ').append('.').append('\n');
                }
            }

            DirectRelationshipAdder relAdder = (DirectRelationshipAdder) doAdd(rdfSerialization.toString(),
                    new HashMap<String, Object>(), RDFFormat.TURTLESTAR, true);
            return Stream.of(relAdder.returnRel());

        } else {
            throw new NodeIsNotResource("n10s.add.relationship cannot link nodes that are not resources " +
                    "(label 'Resource' and property 'uri'). Offending pair node <ids>: " + fromNode.getId() +
                    ", " + toNode.getId());
        }
    }


    @Procedure(name = "n10s.add.relationship.uris", mode = Mode.WRITE)
    @Description("creates a relationship in the graph following the existing GraphConfig.")
    public Stream<RelationshipResult> relAdd(@Name("from") String fromNode,
                                             @Name("type") String relType,
                                             @Name("properties") List<Map<String, Object>> props,
                                             @Name("to") String toNode)
            throws GraphConfig.GraphConfigNotFound, RDFImportPreRequisitesNotMet, IOException,
            InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName, InvalidURI {

        StringBuilder rdfSerialization = new StringBuilder();
            String processedRelType = processTypeAsString(relType);
            IRI fromUri = SimpleValueFactory.getInstance().createIRI(fromNode);
            IRI toUri = SimpleValueFactory.getInstance().createIRI(toNode);
            rdfSerialization.append('<').append(fromUri.stringValue()).append('>').append(' ')
                    .append('<').append(processedRelType).append('>').append(' ')
                    .append('<').append(toUri.stringValue()).append('>').append(' ').append('.').append('\n');

            for (Map<String, Object> x : props) {
                if (x.containsKey("key") && x.containsKey("val")) {
                    rdfSerialization.append('<').append('<').append(' ')
                            .append('<').append(fromUri.stringValue()).append('>').append(' ')
                            .append('<').append(processedRelType).append('>').append(' ')
                            .append('<').append(toUri.stringValue()).append('>')
                            .append(' ').append('>').append('>').append(' ')
                            .append('<').append(processPropKey(x.get("key"))).append('>').append(' ')
                            .append(processValue(x.get("val"), x.get("dt"), x.get("lan"))).append(' ').append('.').append('\n');
                }
            }

            DirectRelationshipAdder relAdder = (DirectRelationshipAdder) doAdd(rdfSerialization.toString(),
                    new HashMap<String, Object>(), RDFFormat.TURTLESTAR, true);
            return Stream.of(relAdder.returnRel());

    }

    private String processValue(Object val, Object dt, Object lan) {
        if ((dt !=null || lan!= null) && (dt instanceof String && (lan instanceof String || val instanceof String))){
            return "\"" + val.toString().replace("\"","\\\"") + "\"" + (lan!=null&&lan instanceof String?"@@" + lan.toString():"^^<" + dt.toString() + ">") ;
        } else {
            Literal typedLiteral = createTypedLiteral(val);
            return "\"" + typedLiteral.stringValue() + "\"^^<" + typedLiteral.getDatatype().stringValue() +">";
        }
    }

    //TODO: REFACTOR THIS WITH THE ONE IN ExportProcessor
    protected Literal createTypedLiteral(Object value) {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Literal result;
        if (value instanceof String) {
            result = vf.createLiteral(((String)value).replace("\"","\\\""));
        } else if (value instanceof Integer) {
            result = vf.createLiteral((Integer) value);
        } else if (value instanceof Long) {
            result = vf.createLiteral((Long) value);
        } else if (value instanceof Float) {
            result = vf.createLiteral((Float) value);
        } else if (value instanceof Double) {
            result = vf.createLiteral((Double) value);
        } else if (value instanceof Boolean) {
            result = vf.createLiteral((Boolean) value);
        } else if (value instanceof LocalDateTime) {
            result = vf
                    .createLiteral(((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                            XMLSchema.DATETIME);
        } else if (value instanceof LocalDate) {
            result = vf
                    .createLiteral(((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE),
                            XMLSchema.DATE);
        } else if (value instanceof PointValue) {
            result = vf
                    .createLiteral(pointValueToWTK((PointValue)value),
                            WKTLITERAL_URI);
        } else {
            // default to string
            result = vf.createLiteral(((String)value).replace("\"","\\\""));
        }

        return result;
    }

    //TODO: this method is duplicated from Export processor. Unify.
    private String pointValueToWTK(PointValue pv) {
        return "Point(" + pv.coordinate()[0] + " " + pv.coordinate()[1] + (pv.getCRS().getCode() == 9157?" "+pv.coordinate()[2]:"") + ")";
    }
    private String processPropKey(Object key) throws InvalidShortenedName, InvalidNamespacePrefixDefinitionInDB,
            InvalidURI, GraphConfig.GraphConfigNotFound {
        return shortenToURIOrLeaveAsIs(key.toString());
    }

    private String shortenToURIOrLeaveAsIs(String str) throws InvalidShortenedName, InvalidNamespacePrefixDefinitionInDB,
            InvalidURI, GraphConfig.GraphConfigNotFound {
        Matcher m = SHORTENED_URI_PATTERN.matcher(str);
        GraphConfig gc = new GraphConfig(tx);

        if (!m.matches()) {
            if ( URIUtil.isValidURIReference(str)){
                return str;
            }else if (gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_IGNORE){
                return DEFAULT_BASE_SCH_NS + str;
            } else {
                throw new InvalidURI(str + " is not a valid URI");
            }

        }else {
            if (gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_SHORTEN_STRICT || gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_SHORTEN) {
                NsPrefixMap prefixDefs = new NsPrefixMap(tx, false);
                if (!prefixDefs.hasPrefix(m.group(1))) {
                    throw new InvalidShortenedName("Prefix Undefined: " + str + " is using an undefined prefix.");
                } else {
                    return prefixDefs.getNsForPrefix(m.group(1)) + m.group(2);
                }
            } else{
                if (gc.getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_IGNORE){
                    return DEFAULT_BASE_SCH_NS + str;
                } else{
                    return str;
                }

            }
        }
    }

    private String processTypeAsString(String str) throws InvalidNamespacePrefixDefinitionInDB, InvalidShortenedName,
            InvalidURI, GraphConfig.GraphConfigNotFound {
        return shortenToURIOrLeaveAsIs(str);
    }

    private class NodeIsNotResource extends Throwable {
        public NodeIsNotResource(String s) {
            super(s);
        }
    }
}
