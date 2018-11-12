package db.daos;

import db.beans.DBBean;
import com.datastax.driver.core.Row;
import utils.Log;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A generic DAO for Cassandra tables
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-10 10:59 PM
 */
public class GenericDAO <T extends DBBean>{

    protected final String TABLE;
    protected final Class<T> BEAN_CLASS;

    protected GenericDAO(String table, Class<T> beanClass) {
        this.TABLE = table;
        this.BEAN_CLASS = beanClass;
    }

    /**
     * Create bean based on row result
     * @param row
     * @return
     */
    private T createBean(Row row) {
        T bean = null;
        try {
            // use reflection to solve generic type problem
            bean = BEAN_CLASS.newInstance();
            bean.buildFrom(row);
        } catch (Exception e) {
            e.printStackTrace();
            Log.d("Null beans found! Be careful!");
        }
        return bean;
    }

    /**
     * Select all fields from the table
     * @return
     */
    public List<T> select() {
        return select(null);
    }

    /**
     * Select given fields from the table
     * @param fieldsToGet
     * @return
     */
    public List<T> select(String[] fieldsToGet) {
        CqlRequest request = CqlRequestFactory.getSelectSafeRequest(TABLE, fieldsToGet);
        // without "?" placeholder, we don't need to pass parameter to addStatements method
        request.addStatements();
        List<Row> dbRows = request.treat();
        List<T> beans = dbRows.stream().map(row -> createBean(row)).collect(Collectors.toList());
        return beans;
    }

    /**
     * Upsert list of beans with list of given fields into table
     * @param beans
     * @param fieldsToUpsert
     * @return
     */
    public int upsert(List<T> beans, String... fieldsToUpsert) {
        CqlRequest request = CqlRequestFactory.getUpsertSafeRequest(TABLE, fieldsToUpsert);
        for (T bean: beans) {
            request.addStatementsAsBeans(bean, fieldsToUpsert);
        }
        request.treat();
        // return upserted records count
        return request.isFailed()? 0: beans.size();
    }

    /**
     * Overload method for single bean upsert, not very efficient, try to upsert a list first instead
     * @param bean
     * @param fieldsToUpsert
     * @return
     */
    public int upsert(T bean, String... fieldsToUpsert) {
        List<T> beans = new ArrayList<>();
        beans.add(bean);
        return upsert(beans, fieldsToUpsert);
    }

    /**
     * Deletes list of beans where given fieldsToDeleteBy is equal to the bean values
     * @param beans
     * @param fieldsToDeleteBy
     * @return
     */
    public int delete(List<T> beans, String... fieldsToDeleteBy) {
        CqlRequest request = CqlRequestFactory.getDeleteSafeRequest(TABLE, fieldsToDeleteBy);
        for (T bean: beans) {
            request.addStatementsAsBeans(bean, fieldsToDeleteBy);
        }
        request.treat();
        // return count of deleted records
        return request.isFailed()? 0: beans.size();
    }

    /**
     * Overload method for single bean delete, not very efficient, try to delete a list first instead
     * @param bean
     * @param fieldsToDeleteBy
     * @return
     */
    public int delete(T bean, String... fieldsToDeleteBy) {
        List<T> beans = new ArrayList<>();
        beans.add(bean);
        return delete(beans, fieldsToDeleteBy);
    }

}
