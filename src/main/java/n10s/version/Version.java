package n10s.version;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.UserFunction;

public class Version {
    @UserFunction("n10s.version")
    @Description("RETURN n10s.version() | return the version of n10s currently installed")
    public String version() {
        return Version.class.getPackage().getImplementationVersion();
    }
}
