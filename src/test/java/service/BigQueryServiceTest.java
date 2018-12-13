package service;

import com.google.api.services.bigquery.model.TableList;
import helper.BigQueryHelperForTest;
import org.junit.*;
import persistence.ipml.BigQueryRepository;
import service.ipml.BigQueryService;

import java.io.IOException;
import java.util.List;

public class BigQueryServiceTest {
    private static BigQueryHelperForTest bigQueryHelperForTest = new BigQueryHelperForTest("mytestproject-225210");
    private final String DATASET = "dataSet_for_test";

    @BeforeClass
    public static void before() throws IOException {
        bigQueryHelperForTest.createDataSetForTest();
    }

    @AfterClass
    public static void after() {
        bigQueryHelperForTest.removeDataSet();
    }

    @Before
    public void addTables() throws IOException {
        bigQueryHelperForTest.addTables();
    }

    @After
    public void removeTables() {
        bigQueryHelperForTest.removeAllTables();
    }

    @Test
    public void UpdateByRegExpTestShouldUpdateAndReturnAllTables() {
        //Given
        int expected = 4;
        BigQueryRepository bigQueryRepository = new BigQueryRepository("mytestproject-225210", DATASET);
        BigQueryService bigQueryService = new BigQueryService(bigQueryRepository);
        //When
        List<TableList.Tables> tables = bigQueryService.updateByTemplate(".*");
        //Then
        Assert.assertEquals(expected, tables.size());
    }

    @Test
    public void updateByRegExpTestShouldUpdateAndReturnTableWithExpectedTableId() {
        //Given
        String expectedTableId = "table_for_test";
        BigQueryRepository bigQueryRepository = new BigQueryRepository("mytestproject-225210", DATASET);
        BigQueryService bigQueryService = new BigQueryService(bigQueryRepository);
        //When
        List<TableList.Tables> tables = bigQueryService.updateByTemplate("table_for_test");
        String actualTableId = tables.get(0).getTableReference().getTableId();
        //Then
        Assert.assertEquals(expectedTableId, actualTableId);
    }

    @Test
    public void updateByRegExpTestUpdatedTableShouldShouldChangeExpirationTime() throws IOException {
        //Given
        long oldExpirationTime = bigQueryHelperForTest.getTable("table_for_test").getExpirationTime();
        BigQueryRepository bigQueryRepository = new BigQueryRepository("mytestproject-225210", DATASET);
        BigQueryService bigQueryService = new BigQueryService(bigQueryRepository);
        //When
        List<TableList.Tables> tables = bigQueryService.updateByTemplate("table_for_test");
        long actualExpirationTime = tables.get(0).getExpirationTime();
        //Then
        Assert.assertNotEquals(oldExpirationTime, actualExpirationTime);
    }
}
