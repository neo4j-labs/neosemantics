package n10s.result;

public class StreamedStatement {

  public String subject;
  public String predicate;
  public String object;
  public boolean isLiteral;
  public String literalType;
  public String literalLang;

  public StreamedStatement(String subj, String pred, String obj, boolean isLiteral,
      String literalType, String lang) {

    this.subject = subj;
    this.predicate = pred;
    this.object = obj;
    this.isLiteral = isLiteral;
    this.literalType = literalType;
    this.literalLang = lang;
  }

}
