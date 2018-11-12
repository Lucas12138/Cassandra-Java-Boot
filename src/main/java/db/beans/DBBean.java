package db.beans;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import org.joda.time.DateTime;
import utils.Log;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.*;

/**
 * Root bean related to database operations
 * Cassandra vs java type
 * timestamp <==> date
 * date <==> LocalDate
 * http://www.lucas-liu.com
 *
 * @author lucas
 * @create 2018-11-09 12:44 AM
 */
public class DBBean {

    /**
     * Let child class override this method if they have auto increment fields
     * @return
     */
    public String getAutoIncrementField() {
        return null;
    }

    /**
     * Get selected fields using reflection
     * @param selectedFieldNames if null, get all fields
     * @return
     */
    public Field[] getFields(Set<String> selectedFieldNames) {
        if (selectedFieldNames == null) {
            return this.getClass().getFields();
        }

        // return only selected ones
        Field[] fields = this.getClass().getFields();
        List<Field> selectedFields = new ArrayList<>();
        for (Field field : fields) {
            String fieldName = field.getName();
            if (selectedFieldNames.contains(fieldName)) {
                selectedFields.add(field);
            }
        }
        return selectedFields.toArray(new Field[selectedFields.size()]);
    }

    /**
     * Get a specific field given field name
     * @param name
     * @return
     */
    public Field getField(String name) {
        Field[] fields = this.getClass().getFields();
        for (Field f : fields) {
            if (f.getName().equals(name)) return f;
        }
        return null;
    }

    /**
     * Using reflection to build this(bean itself) from row
     * @param row
     */
    public void buildFrom(Row row) {
        Field[] fields = this.getClass().getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            String fieldName = field.getName();
            // Check if column definitions have the field name, notice the db field name has to match bean field name
            ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
            if (!columnDefinitions.contains(fieldName)) {
                continue;
            }
            Class<?> fieldType = field.getType();
            // special case: cassandra's date's using LocalDate
            boolean isDateType = false;
            if (fieldType.equals(Date.class)) {
                fieldType = LocalDate.class;
                isDateType = true;
            }
            try {
                Object cqlValue = row.get(fieldName, fieldType);
                if (cqlValue == null) {
                    continue;
                }
                if (isDateType) {
                    LocalDate localDate = (LocalDate) cqlValue;
                    cqlValue = new DateTime(localDate.getMillisSinceEpoch()).withTimeAtStartOfDay().toDate();
                }
                field.set(this, cqlValue);
            } catch (IllegalAccessException | CodecNotFoundException e) {
                Log.i(fieldName + " with error: " + e);
                e.printStackTrace();
            }
        }
    }

    /**
     * Map field name to cassandra compatible object
     * @return
     */
    public Map<String, Object> toCassandraObject() {
        Map<String, Object> beanAsMap = new LinkedHashMap<>();
        Field[] fields = this.getClass().getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            final String fieldNameLowerCased = field.getName().toLowerCase();
            Class<?> fieldType = field.getType();
            try {
                Object value = field.get(this);
                if (fieldType == Date.class) {
                    if (value != null) {
                        // timezone issue
                        java.time.LocalDate localDate = ((Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                        value = LocalDate.fromYearMonthDay(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
                    }
                }
                beanAsMap.put(fieldNameLowerCased, value);
            } catch (IllegalAccessException e) {
                Log.e("Could not read class member " + fieldNameLowerCased, e);
            }
        }
        return beanAsMap;
    }
}
