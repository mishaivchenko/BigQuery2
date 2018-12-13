package persistence;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import helper.BigQueryHelperForTest;
import org.joda.time.DateTime;
import org.junit.*;
import persistence.ipml.BigQueryRepository;

import java.io.IOException;
import java.util.List;

public class BigQueryRepositoryTest {
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
    public void getAllTablesTest() {
        //Given
        int expected = 4;
        BigQueryRepository bigQueryRepository = new BigQueryRepository("mytestproject-225210", DATASET);
        //When
        int actual = bigQueryRepository.getTables().size();
        //Then
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void updateTableTestAfterUpdateExpirationTimeShouldBeEqualsToExpected() throws IOException {
        //Given
        Long expecetExpirationTime = DateTime.now().plusWeeks(2).getMillis();
        BigQueryRepository bigQueryRepository = new BigQueryRepository("mytestproject-225210", DATASET);
        List<TableList.Tables> tableList = bigQueryRepository.getTables();
        tableList.get(0).setExpirationTime(expecetExpirationTime);
        tableList.get(1).setExpirationTime(expecetExpirationTime);
        tableList.get(2).setExpirationTime(expecetExpirationTime);
        tableList.get(3).setExpirationTime(expecetExpirationTime);
        //When
        bigQueryRepository.updateTables(tableList);
        Table table = bigQueryHelperForTest.getTable("table_for_test");
        //Then
        Assert.assertEquals(expecetExpirationTime, table.getExpirationTime());
    }
}
