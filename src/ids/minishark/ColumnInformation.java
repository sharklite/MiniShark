package ids.minishark;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class ColumnInformation {

    Set<String> readOnlyColumns;
    Map<String,Integer> columnType;
    Map<String,String> columnClass;

    String autoIncrement;
    Set<String> primaryKeys;

    private Connection connection;

    /**
     * @param tableName  the table name in database
     * */
    ColumnInformation(String tableName,Connection connection){
        this.connection=connection;
        init(tableName,null);
    }
    /**
     * @param tableName  the table name in database
     * @param eClass  the table mapped class
     * */
    ColumnInformation(String tableName,Class eClass,Connection connection){
        this.connection=connection;
        init(tableName,eClass);
    }

    private void init(String tableName,Class eClass){
        this.columnType=new HashMap<>();
        this.columnClass=new HashMap<>();
        this.primaryKeys=new HashSet<>();
        this.readOnlyColumns=new HashSet<>();

        ResultSet rs=null;
        Statement statement=null;
        Connection conn=this.connection;
        ResultSet primaryKeyResultSet=null;
        String sql="SELECT * FROM  " + tableName+ " WHERE 1=0";
        try{
            if(existTable(tableName,conn)){
                statement=conn.createStatement();
                rs=statement.executeQuery(sql);
                ResultSetMetaData meta=rs.getMetaData();
                for(int i=1;i<=meta.getColumnCount();i++){
                    String col=meta.getColumnName(i);
                    if(meta.isReadOnly(i))
                        this.readOnlyColumns.add(col);
                    int jdbcType=meta.getColumnType(i);
                    String className=meta.getColumnClassName(i);
                    columnType.put(col,jdbcType);
                    columnClass.put(col,className);
                    if(autoIncrement==null && meta.isAutoIncrement(i))
                        autoIncrement=col;
                }
                primaryKeyResultSet=conn.getMetaData().getPrimaryKeys(null,null,tableName);
                while(primaryKeyResultSet.next()){
                    String pk=primaryKeyResultSet.getString(4);
                    this.primaryKeys.add(pk);
                }
            }else{
                if(eClass!=null)
                    System.out.println(eClass+" does not refer to database table.");
                throw new Exception("this table - "+tableName+" does not exist in database");
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(sql);
        }finally{
            DataBase.close(primaryKeyResultSet);
            DataBase.close(rs);
            DataBase.close(statement);
            DataBase.close(conn);
        }
    }

    private static boolean existTable(String tableName, Connection conn){
        boolean flag=false;
        ResultSet rsTables=null;
        try {
            rsTables = conn.getMetaData().getTables(null, null, tableName, new String[] { "TABLE" });
            flag = rsTables.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DataBase.close(rsTables);
        }
        return flag;
    }

}
