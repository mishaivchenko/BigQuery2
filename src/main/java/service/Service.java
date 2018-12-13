package service;

import com.google.api.services.bigquery.model.TableList;

import java.util.List;

public interface Service {
    List<TableList.Tables> updateByTemplate(String template);
}
