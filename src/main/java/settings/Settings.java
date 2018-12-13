package settings;

import org.kohsuke.args4j.Option;

public class Settings {
    @Option(name = "-dataSet", required = true, usage = "set dataSet")
    private String dataSet;
    @Option(name = "-project", required = true, usage = "set project")
    private String project;
    @Option(name = "-template", required = true, usage = "set template")
    private String template;

    public String getDataSet() {
        return dataSet;
    }

    public String getProject() {
        return project;
    }

    public String getTemplate() {
        return template;
    }
}
