package n10s.onto;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;


public class OWLRestriction {
    protected static final IRI INIT = SimpleValueFactory.getInstance().createIRI("neo4j://initRestrictionType");
    protected static final IRI CARDINALITY = SimpleValueFactory.getInstance().createIRI("neo4j://cardinalityRestrictionType");

    private BNode restrictionId;
    private IRI type = INIT;
    private IRI relName = null;
    private IRI targetClass = null;
    private Integer cardinalityValue = null;
    private IRI cardinalityType = null;

    public OWLRestriction(BNode bNode) {
       restrictionId =  bNode;
    }

    boolean isComplete(){
        return !(type==INIT || relName==null || targetClass==null ||
                (type.equals(CARDINALITY)&&cardinalityValue==null));
    }

    public void setRelName(IRI rn) {
        relName = rn;
    }

    public void setTarget(IRI target) {
        targetClass = target;
    }

    public BNode getRestrictionId() {
        return restrictionId;
    }

    public IRI getType() {
        return type;
    }
    public IRI getCardinalityType() {
        return cardinalityType;
    }
    public boolean isCardinalityConstraint(){
        return type.equals(CARDINALITY);
    }

    public IRI getRelName() {
        return relName;
    }

    public IRI getTargetClass() {
        return targetClass;
    }

    public void setType(IRI tp) {
        type = (tp.equals(OWL.ONCLASS)?CARDINALITY:tp);
    }

    public void setCardinalityValue(Literal val) {
        if(( cardinalityValue = Integer.parseInt(val.stringValue())) < 0){
            throw new NumberFormatException("Cardinality needs to be defined as a non-negative integer");
        };
    }

    public void setCardinalitySpecificType(IRI type) {
        cardinalityType = type;
    }

    public int getCardinalityVal() {
        return cardinalityValue;
    }
}
