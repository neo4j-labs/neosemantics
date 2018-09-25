package semantics.mapping;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import semantics.result.NodeResult;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class MappingUtils {
    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    // public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD, RDFFormat.TURTLE,
    //        RDFFormat.NTRIPLES, RDFFormat.TRIG};


    @Procedure(mode = Mode.WRITE)
    public Stream<NodeResult> addSchema(@Name("schemaUri") String uri, @Name("prefix") String prefix) {
        Map<String,Object> params = new HashMap<>();
        params.put("schema",uri);
        params.put("prefix",prefix);
        String cypher = "MERGE (mns:_MapNs { _ns: mappingDef.namespace, _prefix : mappingDef.prefix }) return mns";
        return db.execute(cypher, params).stream().map(n -> (Node) n.get("mns")).map(NodeResult::new);
    }

    @Procedure(mode = Mode.WRITE)
    public Stream<NodeResult> addCommonSchemas() {
        String cypher = "UNWIND [ { namespace: 'http://schema.org/', prefix: 'sch' },\n" +
                "{ namespace: 'http://purl.org/dc/elements/1.1/', prefix: 'dc' },\n" +
                "{ namespace: 'http://purl.org/dc/terms/', prefix: 'dct' },\n" +
                "{ namespace: 'http://www.w3.org/2004/02/skos/core#', prefix: 'skos' },\n" +
                "{ namespace: 'http://www.w3.org/2000/01/rdf-schema#', prefix: 'rdfs' },\n" +
                "{ namespace: 'http://www.w3.org/2002/07/owl#', prefix: 'owl' },\n" +
                "{ namespace: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', prefix: 'rdf' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/', prefix: 'fibo-be-corp-corp' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/', prefix: 'fibo-be-le-cb' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/FormalBusinessOrganizations/', prefix: 'fibo-be-le-fbo' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/LegalPersons/', prefix: 'fibo-be-le-lp' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/AgentsAndPeople/Agents/', prefix: 'fibo-fnd-aap-agt' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Accounting/CurrencyAmount/', prefix: 'fibo-fnd-acc-cur' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/', prefix: 'fibo-fnd-dt-fd' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Law/Jurisdiction/', prefix: 'fibo-fnd-law-jur' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Organizations/FormalOrganizations/', prefix: 'fibo-fnd-org-fm' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Relations/Relations/', prefix: 'fibo-fnd-rel-rel' },\n" +
                "{ namespace: 'https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/', prefix: 'fibo-fnd-utl-av' }\n" +
                "] AS schemaDef \n" +
                "MERGE (mns:_MapNs { _ns: schemaDef.namespace, _prefix : schemaDef.prefix }) RETURN mns";
        return db.execute(cypher).stream().map(n -> (Node) n.get("mns")).map(NodeResult::new);
    }

    @Procedure(mode = Mode.READ)
    public Stream<NodeResult> listSchemas(@Name(value="schemaUri",defaultValue = "") String schemaUri) {

        Map<String,Object> params = new HashMap<>();
        params.put("schemaUri",schemaUri);

        String cypher = (schemaUri.trim().equals("")?"MATCH (mns:_MapNs) RETURN mns ":"MATCH (mns:_MapNs) WHERE mns._ns CONTAINS $schemaUri RETURN mns ");

        return db.execute(cypher, params).stream().map(n -> (Node) n.get("mns")).map(NodeResult::new);
    }

    @Procedure(mode = Mode.WRITE)
    public Stream<NodeResult> addMappingToSchema(@Name("schemaNode") Node schema, @Name("graphElementName") String gElem,
                                                 @Name("schemaElementlocalName") String schElem) {
        Node sch = schema;
        String cypher = "MERGE (mp:_MapDef { _key : $elem }) SET mp._local = $localName RETURN mp";
        Map<String,Object> params = new HashMap<>();
        params.put("elem",gElem);
        params.put("localName",schElem);

        Node newMapping = (Node)db.execute(cypher, params).next().get("mp");
        schema.createRelationshipTo(newMapping, RelationshipType.withName("_IN"));
        return Stream.of(new NodeResult(newMapping));
    }



}
