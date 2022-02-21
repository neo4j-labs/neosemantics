package n10s.result;

import java.util.List;

public class StreamedStatement {

  public String subject;
  public String predicate;
  public String object;
  public boolean isLiteral;
  public String literalType;
  public String literalLang;
  public boolean subjectIsTriple;
  public List<String> subjectSPO;

  public StreamedStatement(String subj, String pred, String obj, boolean isLiteral,
      String literalType, String lang) {

    this.subject = subj;
    this.predicate = pred;
    this.object = obj;
    this.isLiteral = isLiteral;
    this.literalType = literalType;
    this.literalLang = lang;
    this.subjectIsTriple = false;
    this.subjectSPO = null;
  }

  public StreamedStatement(String subj, String pred, String obj, boolean isLiteral,
                           String literalType, String lang, List<String> sSPO) {

    this(subj, pred, obj, isLiteral, literalType, lang);
    this.subjectIsTriple = true;
    this.subjectSPO = sSPO;
  }

}
