package service.ipml;

import com.google.api.services.bigquery.model.TableList;
import org.joda.time.DateTime;
import persistence.Repository;
import service.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BigQueryService implements Service {

    private Repository repository;

    public BigQueryService(Repository repository) {
        this.repository = repository;
    }

    public List<TableList.Tables> updateByTemplate(String template) {
        List<TableList.Tables> tablesList = repository.getTables();
        List<TableList.Tables> listToUpdate = new ArrayList<TableList.Tables>();
        if (tablesList != null) {
            Pattern pattern = Pattern.compile(template);
            for (TableList.Tables t : tablesList) {
                String tableId = t.getTableReference().getTableId();
                Matcher m = pattern.matcher(tableId);
                if (m.matches()) {
                    setExpirationTime(t);
                    listToUpdate.add(t);
                }
            }
            repository.updateTables(listToUpdate);
        }
        return listToUpdate;
    }

    private void setExpirationTime(TableList.Tables t) {
        t.setExpirationTime(DateTime.now().plusWeeks(2).getMillis());
    }
}
