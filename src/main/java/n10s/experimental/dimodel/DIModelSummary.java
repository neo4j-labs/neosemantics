package n10s.experimental.dimodel;

public class DIModelSummary {
    public String modelFile;
    public String mappingsScript;
    public String summary;

    public DIModelSummary(String filename, String scr, String s) {
        this.modelFile = filename;
        this.mappingsScript = scr;
        this.summary = s;
    }
}
