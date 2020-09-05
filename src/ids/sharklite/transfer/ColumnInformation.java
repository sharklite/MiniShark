package ids.sharklite.transfer;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class ColumnInformation {

    Set<String> readOnlyColumns;//只读列的列名
    Map<String, JDBCType> columnType;//列名及对应的jdbc类型

    String autoIncrement;//自增列的列名
    Set<String> conditions;//主键(条件)列名

    private final Transfer<?> transfer;
    private final Connection conn;

    ColumnInformation(Transfer<?> transfer) {
        this.transfer = transfer;
        this.conn = transfer.getConnection();
        init(transfer.getTableName());
    }

    //得到表及相关列的信息
    private void init(String tableName) {
        this.columnType = new HashMap<>();
        this.conditions = new HashSet<>();
        this.readOnlyColumns = new HashSet<>();

        ResultSet rs = null;
        Statement statement = null;
        ResultSet primaryKeyResultSet = null;
        String sql = "SELECT * FROM " + this.transfer.getLeftWrap() + tableName + this.transfer.getRightWrap() + " WHERE 1=0";
        try {
            if (existTable(tableName)) {
                statement = conn.createStatement();
                rs = statement.executeQuery(sql);
                ResultSetMetaData meta = rs.getMetaData();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String col = meta.getColumnName(i);
                    if (meta.isReadOnly(i))
                        this.readOnlyColumns.add(col);
                    int jdbcType = meta.getColumnType(i);
                    columnType.put(col, JDBCType.valueOf(jdbcType));
                    if (autoIncrement == null && meta.isAutoIncrement(i))
                        autoIncrement = col;
                }
                primaryKeyResultSet = conn.getMetaData().getPrimaryKeys(null, null, tableName);
                while (primaryKeyResultSet.next()) {
                    String pk = primaryKeyResultSet.getString(4);
                    this.conditions.add(pk);
                }
            } else {
                throw new Exception("this table - " + tableName + " does not exist in database");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(sql);
        } finally {
            Util.close(primaryKeyResultSet);
            Util.close(rs);
            Util.close(statement);
            Util.close(conn);
        }
    }


    //检查是否存在此表
    private boolean existTable(String tableName) {
        boolean flag = false;
        ResultSet rsTables = null;
        try {
            rsTables = conn.getMetaData().getTables(null, null, tableName, new String[]{"TABLE"});
            flag = rsTables.next();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Util.close(rsTables);
        }
        return flag;
    }

}
