package ids.minishark;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

final class MapType {

    private MapType(){
    }

    private static final Set<Integer> JDBC_TYPE=new HashSet<>();

    static final int UNDEFINED = Integer.MIN_VALUE;

    static {
        Field[] fields=Types.class.getFields();
        for(Field field:fields){
            try {
                JDBC_TYPE.add(field.getInt(Types.class));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    /***
     * @param object is the value for PreparedStatement
     * @return sql type
     * */
     static int tryFrom(Object object){
        int code;
        if(object==null){
            code= Types.NULL;
        }else if(object instanceof BigDecimal){
            code=Types.DECIMAL;
        }else if(object instanceof Number){
            code=Types.NUMERIC;
        }else if(object instanceof Boolean){
            code=Types.BOOLEAN;
        }else if(object instanceof Character){
            code=Types.CHAR;
        }else if(object instanceof String){
            code=Types.VARCHAR;
        }else if(object instanceof java.sql.Timestamp){
            code=Types.TIMESTAMP;
        }else if(object instanceof java.sql.Time){
            code=Types.TIME;
        }else if(object instanceof java.sql.Date){
            code=Types.DATE;
        }else if(object instanceof byte[]){
            code=Types.VARBINARY;
        }else if(object instanceof Byte[]){
            code=Types.VARBINARY;
        }else if(object instanceof java.sql.Blob){
            code = Types.BLOB;
        }else if(object instanceof java.sql.NClob){
            code = Types.NCLOB;
        }else if(object instanceof java.sql.Clob){
            code=Types.CLOB;
        }else if(object instanceof java.sql.Array){
            code=Types.ARRAY;
        }else if(object instanceof java.sql.SQLXML){
            code=Types.SQLXML;
        }else if(object instanceof java.sql.Ref){
            code=Types.REF;
        }else if(object instanceof java.sql.Struct){
            code=Types.STRUCT;
        }else {
            code=UNDEFINED;
        }
        return code;
    }

    static int jdbcTypeOf(int code){
        if(!JDBC_TYPE.contains(code))
            code= UNDEFINED;
        return code;
    }

}
