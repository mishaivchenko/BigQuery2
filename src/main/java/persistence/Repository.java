package persistence;

import com.google.api.services.bigquery.model.TableList;

import java.util.List;

public interface Repository {
    List<TableList.Tables> getTables();

    void updateTables(List<TableList.Tables> tables);
}
