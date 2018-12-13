import persistence.ipml.BigQueryRepository;
import service.ipml.BigQueryService;

import java.io.IOException;

public class test {
    public static void main(String[] args) throws IOException {
        BigQueryRepository bigQueryRepository = new BigQueryRepository("mytestproject-225210", "my_new_dataset");
        BigQueryService bigQueryService = new BigQueryService(bigQueryRepository);
        bigQueryService.updateByTemplate("_co.*");
    }
}
