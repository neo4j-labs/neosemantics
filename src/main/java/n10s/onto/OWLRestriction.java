package n10s.onto;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;


public class OWLRestriction {
    protected static final IRI INIT = SimpleValueFactory.getInstance().createIRI("neo4j://initRestrictionType");

    private BNode restrictionId;
    private IRI type = INIT;
    private IRI relName = null;
    private IRI targetClass = null;

    public OWLRestriction(BNode bNode) {
       restrictionId =  bNode;
    }

    boolean isComplete(){
        return !(type==INIT || relName==null || targetClass==null);
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

    public IRI getRelName() {
        return relName;
    }

    public IRI getTargetClass() {
        return targetClass;
    }

    public void setType(IRI tp) {
        type = tp;
    }
}
