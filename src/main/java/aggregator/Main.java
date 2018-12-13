package aggregator;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import persistence.Repository;
import persistence.ipml.BigQueryRepository;
import service.Service;
import service.ipml.BigQueryService;
import settings.Settings;

public class Main {
    public static void main(String[] args) {
        Settings settings = new Settings();
        CmdLineParser cmdLineParser = new CmdLineParser(settings);
        try {
            cmdLineParser.parseArgument(args);
            System.out.println("settings " + settings);
            Repository bigQueryRepository = new BigQueryRepository(
                    settings.getProject(),
                    settings.getDataSet()
            );
            Service bigQueryService = new BigQueryService(bigQueryRepository);
            bigQueryService.updateByTemplate(settings.getTemplate());
        } catch (CmdLineException e) {
            cmdLineParser.printUsage(System.out);
        }
    }
}
