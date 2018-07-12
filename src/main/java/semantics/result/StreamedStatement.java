package semantics.result;

public class StreamedStatement {
    public String subject;
    public String predicate;
    public String object;
    public boolean isLiteral;
    public String literalType;
    public String literalLang;

    public StreamedStatement(String subj, String pred, String obj, boolean isLiteral, String literalType, String lang) {

        subject = subj;
        predicate = pred;
        object = obj;
        this.isLiteral = isLiteral;
        this.literalType = literalType;
        literalLang = lang;
    }

}
