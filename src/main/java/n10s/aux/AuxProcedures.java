package n10s.aux;

import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.graphdb.spatial.Point;
import static n10s.graphconfig.Params.WKTLITERAL_URI;

import java.time.*;

public class AuxProcedures  {

  @UserFunction(name = "n10s.aux.dt.check")
  @Description("Checks if a value has a given datatype (XSD)")
  public Boolean checkDatatype(@Name("expectedType") String expected, @Name("actual") Object value) {

    return getXSDFrom(value == null ? null : value.getClass()).equals(expected);
  }

  private String getXSDFrom(Class<?> type) {
    if (LocalDate.class.isAssignableFrom(type)) { return XMLSchema.DATE.stringValue(); }
    if (LocalDateTime.class.isAssignableFrom(type)) { return XMLSchema.DATETIME.stringValue(); }
    if (LocalTime.class.isAssignableFrom(type)) { return XMLSchema.TIME.stringValue(); }
    if (ZonedDateTime.class.isAssignableFrom(type)) { return XMLSchema.DATETIME.stringValue(); }
    if (Point.class.isAssignableFrom(type)){ return WKTLITERAL_URI.stringValue(); }
    if (DurationValue.class.isAssignableFrom(type)) { return XMLSchema.DURATION.stringValue(); }
    if (OffsetTime.class.isAssignableFrom(type)) { return XMLSchema.TIME.stringValue(); }
    return "NONE";  //this should work also for other types but it's ok for now
  }

}
