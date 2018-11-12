package db.daos;

import java.util.Collections;

/**
 * Use factory design pattern for CqlRequest, get basic CRUD requests
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-10 11:10 PM
 */
public class CqlRequestFactory {

    /**
     * Get the insert request string
     * @param table
     * @param fieldsToUpsert
     * @return
     */
    public static String getInsertString(String table, String... fieldsToUpsert) {
        String columns = "("+String.join(",", fieldsToUpsert)+")";
        String[] valuesPlaceHoldersArray =  Collections.nCopies(fieldsToUpsert.length, "?").toArray(new String[fieldsToUpsert.length]);
        String valuesPlaceHolders = "("+String.join(",", valuesPlaceHoldersArray)+")";
        String query = "INSERT INTO " + table + " " + columns + " VALUES " + valuesPlaceHolders;
        return query;
    }

    /**
     * Get the upsert request based on table and fields (for cassandra and for now, it's same with insert request)
     * @param table
     * @param fieldsToUpsert
     * @return
     */
    public static CqlRequest getUpsertSafeRequest(String table, String... fieldsToUpsert) {
        String query = getInsertString(table, fieldsToUpsert);
        CqlRequest request = new CqlRequest(query);
        return request;
    }

    /**
     * Get the select request based on table and fields
     * @param table
     * @param fieldsToGet
     * @return
     */
    public static CqlRequest getSelectSafeRequest(String table, String[] fieldsToGet) {
        String query =  "SELECT " + ((fieldsToGet==null || fieldsToGet.length==0)?"*": String.join(", ", fieldsToGet))+" " +
                "FROM " + table;
        CqlRequest request = new CqlRequest(query);
        return request;
    }

    /**
     * Get the delete request based on table and fields to delete by
     * @param table
     * @param fieldsToDeleteBy
     * @return
     */
    public static CqlRequest getDeleteSafeRequest(String table, String[] fieldsToDeleteBy) {
        // create condition strings
        String[] conditions = new String[fieldsToDeleteBy.length];
        for (int i=0; i<fieldsToDeleteBy.length; i+=1) {
            conditions[i] = fieldsToDeleteBy[i] +"=?";
        }
        String conditionsPlaceHolders = String.join(" AND ", conditions);
        String query = "DELETE FROM " + table + " WHERE " + conditionsPlaceHolders;
        CqlRequest request = new CqlRequest(query);
        return request;
    }
}
