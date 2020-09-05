package ids.sharklite.transfer;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;


final class TransferExecutor {

    private static final int BATCH = 128;

    private TransferExecutor() {

    }

    private static boolean unusable(Collection<?> collection, Transfer<?> transfer) {
        return collection == null || collection.size() == 0 || transfer.conditions.size() == 0;
    }

    static <T> List<T> selectBatch(Collection<T> collection, Transfer<T> transfer) throws SQLException {
        List<T> list = new ArrayList<>();
        if (unusable(collection, transfer))
            return list;
        Connection conn = transfer.getConnection();
        try {
            for (T entity : collection) {
                if (entity == null)
                    continue;
                PreparedStatement pst = conn.prepareStatement(transfer.select_one);
                TransferExecutor.addPreparedStatementParameters(pst, transfer.selectValues(entity));
                ResultSet rs = pst.executeQuery();
                while (rs.next()) {
                    list.add(entity);
                    for (Map.Entry<String, String> entry : transfer.colFieldMapper.entrySet()) {
                        String col = entry.getKey();
                        if (!transfer.notReads.contains(col)) {
                            String label = transfer.colFieldMapper.get(col);
                            transfer.setFieldValue(label, rs.getObject(label));
                        }
                    }
                }
                Util.close(rs);
                Util.close(pst);
            }
        } finally {
            Util.close(conn);
        }
        return list;
    }

    static <T> void updateBatch(Collection<T> collection, Transfer<T> transfer) throws SQLException {
        if (unusable(collection, transfer))
            return;
        Connection conn = transfer.getConnection();
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(transfer.update_one);
            int i = 1;
            for (T entity : collection) {
                if (entity == null)
                    continue;
                TransferExecutor.addPreparedStatementParameters(pst, transfer.updateValues(entity));
                pst.addBatch();
                if (i % BATCH == 0) {
                    pst.executeBatch();
                    pst.clearBatch();
                    i = 1;
                }
                i++;
            }
            pst.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            Util.close(pst);
            Util.close(conn);
        }
    }

    static <T> void deleteBatch(Collection<T> collection, Transfer<T> transfer) throws SQLException {
        if (unusable(collection, transfer))
            return;
        Connection conn = transfer.getConnection();
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(transfer.delete_one);
            int i = 1;
            for (T entity : collection) {
                if (entity == null)
                    continue;
                TransferExecutor.addPreparedStatementParameters(pst, transfer.deleteValues(entity));
                pst.addBatch();
                if (i % BATCH == 0) {
                    pst.executeBatch();
                    pst.clearBatch();
                    i = 1;
                }
                i++;
            }
            pst.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            Util.close(pst);
            Util.close(conn);
        }
    }

    static <T> void insertBatch(Collection<T> collection, Transfer<T> transfer) throws SQLException {
        if (collection == null || collection.size() == 0)
            return;
        Connection conn = transfer.getConnection();
        PreparedStatement pst = null;
        ResultSet rsAuto = null;
        try {
            pst = conn.prepareStatement(transfer.insert_one, PreparedStatement.RETURN_GENERATED_KEYS);
            for (T entity : collection) {
                if (entity == null)
                    continue;
                SqlParameter[] parameters = transfer.insertValues(entity);
                TransferExecutor.addPreparedStatementParameters(pst, parameters);
                pst.executeUpdate();
                conn.commit();
                if (transfer.autoIncrementCol != null) {//有自增列
                    rsAuto = pst.getGeneratedKeys();
                    while (rsAuto.next()) {
                        Object o = rsAuto.getObject(1);//得到插入返回的值
                        Field field = transfer.fields.get(transfer.colFieldMapper.get(transfer.autoIncrementCol));//得到Field
                        if (field != null) {
                            o = autoIncrement(o, field.getType());
                            //将值赋给E的实例
                            transfer.setFieldValue(transfer.colFieldMapper.get(transfer.autoIncrementCol), o);
                        }
                    }
                    Util.close(rsAuto);
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            Util.close(rsAuto);
            Util.close(pst);
            Util.close(conn);
        }
    }

    private static Object autoIncrement(Object o, Class<?> fieldType) {
        if (o instanceof Number) {//自增列为BigDecimal，此处要对其进行转化
            Number number = (Number) o;
            if (fieldType.equals(int.class) || fieldType.equals(Integer.class))
                o = number.intValue();
            else if (fieldType.equals(long.class) || fieldType.equals(Long.class))
                o = number.longValue();
            else if (fieldType.equals(short.class) || fieldType.equals(Short.class))
                o = number.shortValue();
            else if (fieldType.equals(byte.class) || fieldType.equals(Byte.class))
                o = number.byteValue();
            else if (fieldType.equals(BigDecimal.class))
                o = new BigDecimal(o.toString());
        }
        return o;
    }

    //得到第一列、第一行的值
    static Object queryScalar(Connection conn, String preparedSql, SqlParameter... parameters) throws SQLException {
        Object v = null;
        ResultSet rs = null;
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(preparedSql);
            TransferExecutor.addPreparedStatementParameters(statement, parameters);
            rs = statement.executeQuery();
            if (rs.next()) {
                v = rs.getObject(1);
            }
        } finally {
            Util.close(rs);
            Util.close(statement);
            Util.close(conn);
        }
        return v;
    }


    static void addPreparedStatementParameters(PreparedStatement pst, SqlParameter... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object value = params[i].getValue();
            int code = params[i].getType();
            int index = i + 1;
            if (value == null)
                pst.setNull(index, code);
            else if (code == MappedType.UNDEFINED)
                pst.setObject(index, value);
            else
                pst.setObject(index, value, code);
        }
    }


    static int executeUpdate(Connection connection, String preparedSql, SqlParameter... args) throws SQLException {
        int r;
        try (PreparedStatement pst = connection.prepareStatement(preparedSql)) {
            TransferExecutor.addPreparedStatementParameters(pst, args);
            r = pst.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            Util.close(connection);
        }
        return r;
    }

}
