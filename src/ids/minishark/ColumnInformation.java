package ids.minishark;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class ColumnInformation {

    Set<String> readOnlyColumns;//只读列的列名
    Map<String,Integer> columnType;//列名及对应的jdbc类型
    Map<String,String> columnClass;//列名及对应的java类型

    String autoIncrement;//自增列的列名
    Set<String> primaryKeys;//主键列名

    private Connection conn;

    /**
     * @param tableName  the table name in database
     * @param eClass  the table mapped class
     * */
    ColumnInformation(String tableName,Class eClass,Connection connection){
        this.conn =connection;
        init(tableName,eClass);
    }

    //得到表及相关列的信息
    private void init(String tableName,Class eClass){
        this.columnType=new HashMap<>();
        this.columnClass=new HashMap<>();
        this.primaryKeys=new HashSet<>();
        this.readOnlyColumns=new HashSet<>();

        ResultSet rs=null;
        Statement statement=null;
        ResultSet primaryKeyResultSet=null;
        String sql="SELECT * FROM  " + tableName+ " WHERE 1=0";
        try{
            if(existTable(tableName)){
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


    //检查是否存在此表
    private boolean existTable(String tableName){
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
