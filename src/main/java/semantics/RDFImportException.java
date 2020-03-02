package semantics;

public class RDFImportException extends Throwable {

    public RDFImportException(Throwable e) {
    }

    public RDFImportException(String s) {
        super(s);
    }
}
