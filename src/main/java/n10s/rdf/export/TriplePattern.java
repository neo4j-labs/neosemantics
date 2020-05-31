package n10s.rdf.export;

public class TriplePattern {


  private final String subject;
  private final String predicate;
  private final String object;
  private final Boolean isLiteral;
  private final String literalType;
  private final String literalLang;

  public TriplePattern(String subject, String predicate, String object, Boolean isLiteral,
      String literalType, String literalLang) {

    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
    this.isLiteral = isLiteral;
    this.literalType = literalType;
    this.literalLang = literalLang;
  }

  public String getSubject() {
    return subject;
  }

  public String getPredicate() {
    return predicate;
  }

  public String getObject() {
    return object;
  }

  public Boolean getLiteral() {
    return isLiteral;
  }

  public String getLiteralType() {
    return literalType;
  }

  public String getLiteralLang() {
    return literalLang;
  }
}
