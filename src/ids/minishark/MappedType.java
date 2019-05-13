package ids.minishark;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

final class MappedType {

    static final int UNDEFINED = Integer.MIN_VALUE;
    private static final Set<Integer> JDBC_TYPE = new HashSet<>();

    static {
        //得到所有jdbc类型
        Field[] fields = Types.class.getFields();
        for (Field field : fields) {
            try {
                JDBC_TYPE.add(field.getInt(Types.class));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    private MappedType() {
    }

    /***
     * @param object is the value for PreparedStatement
     * @return sql type
     * 根据object判断jdbc的类型
     * */
    static int tryFrom(Object object) {
        int type;
        if (object == null) {
            type = Types.NULL;
        } else if (object instanceof BigDecimal) {
            type = Types.DECIMAL;
        } else if (object instanceof Number) {
            type = Types.NUMERIC;
        } else if (object instanceof Boolean) {
            type = Types.BOOLEAN;
        } else if (object instanceof Character) {
            type = Types.CHAR;
        } else if (object instanceof String) {
            type = Types.VARCHAR;
        } else if (object instanceof java.sql.Timestamp) {
            type = Types.TIMESTAMP;
        } else if (object instanceof java.sql.Time) {
            type = Types.TIME;
        } else if (object instanceof java.sql.Date) {
            type = Types.DATE;
        } else if (object instanceof java.util.Date) {
            type = Types.TIMESTAMP;
        }else if (object instanceof byte[]) {
            type = Types.VARBINARY;
        } else if (object instanceof Byte[]) {
            type = Types.VARBINARY;
        } else if (object instanceof java.sql.Blob) {
            type = Types.BLOB;
        } else if (object instanceof java.sql.NClob) {
            type = Types.NCLOB;
        } else if (object instanceof java.sql.Clob) {
            type = Types.CLOB;
        } else if (object instanceof java.sql.Array) {
            type = Types.ARRAY;
        } else if (object instanceof java.sql.SQLXML) {
            type = Types.SQLXML;
        } else if (object instanceof java.sql.Ref) {
            type = Types.REF;
        } else if (object instanceof java.sql.Struct) {
            type = Types.STRUCT;
        } else {
            type = UNDEFINED;
        }
        return type;
    }

    static int jdbcTypeOf(int type) {
        if (!JDBC_TYPE.contains(type))
            type = UNDEFINED;
        return type;
    }

}
