package helper;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BigQueryHelperForTest {
    private final String PROJECT_ID;
    private final String DATASET_ID = "dataSet_for_test";
    private Bigquery bigquery = createBigQuery();

    public BigQueryHelperForTest(String projectId) {
        PROJECT_ID = projectId;
    }

    public void createDataSetForTest() {
        Dataset dataset = new Dataset();
        DatasetReference datasetReference = new DatasetReference();
        datasetReference
                .setProjectId(PROJECT_ID)
                .setDatasetId(DATASET_ID);
        dataset.setDatasetReference(datasetReference);
        try {
            bigquery.datasets().insert(PROJECT_ID, dataset).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Table getTable(String tableId) throws IOException {
        return bigquery.tables().get(PROJECT_ID, DATASET_ID, tableId).execute();
    }

    public void removeAllTables() {
        try {
            for (TableList.Tables t : bigquery.tables().list(PROJECT_ID, DATASET_ID).execute().getTables()) {
                bigquery.tables().delete(PROJECT_ID, DATASET_ID, t.getTableReference().getTableId()).execute();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void removeDataSet() {
        try {
            bigquery.datasets().delete(PROJECT_ID, DATASET_ID).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addTables() throws IOException {
        createTable("table_for_test");
        createTable("tableTest");
        createTable("someTable");
        createTable("tabletable");
    }

    private void createTable(String name) throws IOException {
        List<TableFieldSchema> fieldSchema = new ArrayList<>();
        fieldSchema.add(new TableFieldSchema().setName("testId").setType("STRING").setMode("NULLABLE"));
        fieldSchema.add(new TableFieldSchema().setName("testId2").setType("STRING").setMode("NULLABLE"));
        TableSchema schema = new TableSchema();
        schema.setFields(fieldSchema);
        TableReference ref = new TableReference();
        ref.setProjectId(PROJECT_ID);
        ref.setDatasetId(DATASET_ID);
        ref.setTableId(name);
        Table content = new Table();
        content.setTableReference(ref);
        content.setSchema(schema);
        bigquery.tables().insert(ref.getProjectId(), ref.getDatasetId(), content).execute();
    }

    private Bigquery createBigQuery() {
        // Create the credential
        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = null;
        try {
            credential = GoogleCredential.fromStream(
                    new FileInputStream("src/test/APPLICATION_CREDENTIALS_TEST.json"),
                    transport,
                    jsonFactory);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        if (Objects.requireNonNull(credential).createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("FOR_TEST_ONLY")
                .build();
    }
}
