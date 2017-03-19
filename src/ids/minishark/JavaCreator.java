package ids.minishark;


import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public final class JavaCreator {

    private static Map<Integer,String> sqlTypes=new HashMap<>();
    private static Map<String,String> columnClass=new HashMap<>();
    private static Map<String,Integer> jdbcType=new HashMap<>();
    private static Set<String> readOnly=new HashSet<>();

    private JavaCreator(){
    }

    public static String create(String packageName, String tableName,Connection conn){
        return create(packageName,tableName,tableName,conn);
    }

    public static String create(String packageName, String className, String tableName,Connection connection) {
        Field[] fields= Types.class.getFields();
        for(Field f:fields){
            try {
                sqlTypes.put(f.getInt(f.getName()),f.getName());
            } catch (ReflectiveOperationException e) {
                e.printStackTrace();
            }
        }
        StringBuilder javaBuilder=new StringBuilder();
        javaBuilder.append("package  ").append(packageName).append(";\n\n");
        parseDBTable(tableName,connection);
        javaBuilder.append(packageImport());
        className= _String_.toUpperAtFirst(tableName);
        javaBuilder.append("@Table(\"").append(tableName).append("\")\n");
        javaBuilder.append("public class ").append(className).append("{\n\n");
        javaBuilder.append("    public ").append(className).append("(){\n  }\n\n");
        javaBuilder.append(fields());
        javaBuilder.append(settersAndGetters());
        javaBuilder.append("}\n");
        return javaBuilder.toString();
    }

    private static void parseDBTable(String table, Connection connection){
        ColumnInformation column=new ColumnInformation(table,connection);
        jdbcType=column.columnType;
        columnClass=column.columnClass;
        readOnly=column.readOnlyColumns;
    }

    private static StringBuilder packageImport(){
        StringBuilder builder=new StringBuilder();
        Set<String> set=new HashSet<>();
        set.addAll(columnClass.values());
        for(String s:set){
            if(!s.contains("java.lang") && !s.contains("[") && !s.contains("java.sql."))
                builder.append("import ").append(s).append(";\n");
        }
        builder.append("import ").append("ids.minishark.annotation.Column;\n");
        builder.append("import ").append("ids.minishark.annotation.JdbcType;\n");
        builder.append("import ").append("ids.minishark.annotation.ReadOnly;\n");
        builder.append("import ").append("ids.minishark.annotation.Table;\n");
        builder.append("import ").append("java.sql.*;\n");
        builder.append("\n");
        return builder;
    }

    private static StringBuilder fields(){
        StringBuilder stringBuilder=new StringBuilder();
        for(String col:columnClass.keySet()){
            int type=jdbcType.get(col);
            if(readOnly.contains(col))
                stringBuilder.append("    @ReadOnly\n");
            stringBuilder.append("    @Column(\"").append(col).append("\")").append("\n");
            stringBuilder.append("    @JdbcType(Types.").append(sqlTypes.get(type)).append(")").append("\n");
            String fieldName=_String_.toLowerAtFirst(col);
            stringBuilder.append("    private ").append(toBaseType(javaType(columnClass.get(col)))).append(" ").append(fieldName).append(";\n\n");
        }
        stringBuilder.append("\n");
        return stringBuilder;
    }

    private static StringBuilder settersAndGetters(){
        StringBuilder stringBuilder=new StringBuilder();
        Map<String,String> map=columnClass;
        for(String col:map.keySet()){
            String fieldName=_String_.toLowerAtFirst(col);
            //setter
            stringBuilder.append("    public void ").append(_String_.SET).append(_String_.toUpperAtFirst(col));
            stringBuilder.append("(").append(toBaseType(javaType(map.get(col)))).append(" ").append(fieldName).append(")");
            stringBuilder.append("{\n").append("        this.").append(fieldName).append("=").append(fieldName).append(";\n");
            stringBuilder.append("\t}\n");
            //getter
            stringBuilder.append("    public ").append(toBaseType(javaType(map.get(col)))).append(" ").append(_String_.GET).append(_String_.toUpperAtFirst(col));
            stringBuilder.append("()").append("{\n");
            stringBuilder.append("        return this.").append(fieldName).append(";\n");
            stringBuilder.append("\t}\n");

        }
        stringBuilder.append("\n");
        return stringBuilder;
    }

    private static String javaType(String className){
        String str[]=className.split("\\.");
        className=str[str.length-1];
        if(className.contains("["))
            className="byte[]";
        return className;
    }

    private static String toBaseType(String s){
        if(s.equals("Integer"))
            return "int";
        if(s.equals("Short"))
            return "short";
        if(s.equals("Byte"))
            return "byte";
        if(s.equals("Long"))
            return "long";
        if(s.equals("Character"))
            return "char";
        if(s.equals("Float"))
            return "float";
        if(s.equals("Double"))
            return "double";
        if(s.equals("Boolean"))
            return "boolean";
        return s;
    }
}
