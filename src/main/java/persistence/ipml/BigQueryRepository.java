package persistence.ipml;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import persistence.Repository;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BigQueryRepository implements Repository {

    private final String PROJECT_ID;
    private final String DATASET_ID;
    private Bigquery bigquery = createAuthorizedClient();

    public BigQueryRepository(String projectId, String dataSetId) {
        PROJECT_ID = projectId;
        DATASET_ID = dataSetId;
    }

    public List<TableList.Tables> getTables() {
        try {
            return bigquery.tables().list(PROJECT_ID, DATASET_ID).execute().getTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();

    }

    public void updateTables(List<TableList.Tables> tables) {
        try {
            for (TableList.Tables t : tables) {
                Table table = bigquery.tables().get(PROJECT_ID, DATASET_ID, t.getTableReference().getTableId()).execute();
                table.setExpirationTime(t.getExpirationTime());
                bigquery.tables().patch(PROJECT_ID, DATASET_ID, table.getTableReference().getTableId(), table).execute();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Bigquery createAuthorizedClient() {
        // Create the credential
        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();


        GoogleCredential credential = null;
        try {
            credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
        } catch (IOException e) {
            try {
                credential = GoogleCredential.fromStream(
                        new FileInputStream("src/APPLICATION_CREDENTIALS_TEST.json"),
                        transport,
                        jsonFactory);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        if (Objects.requireNonNull(credential).createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Bigquery Samples")
                .build();
    }
}
