package n10s.rdf.stream;

import java.util.Map;
import java.util.stream.Stream;
import n10s.RDFImportException;
import n10s.rdf.RDFProcedures;
import n10s.result.StreamedStatement;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class RDFStreamProcedures extends RDFProcedures {

  @Procedure(mode = Mode.READ)
  @Description(
      "Parses RDF and streams each triple as a record with <S,P,O> along with datatype and "
          + "language tag for Literal values. No writing to the DB.")
  public Stream<StreamedStatement> fetch(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {

    return doStream(url, null, format, props);
  }

  @Procedure(mode = Mode.READ)
  @Description(
      "Parses RDF passed as a string and streams each triple as a record with <S,P,O> along "
          + "with datatype and language tag for Literal values. No writing to the DB.")
  public Stream<StreamedStatement> inline(@Name("rdf") String rdf,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props)
      throws RDFImportException {

    return doStream(null, rdf, format, props);
  }

}
