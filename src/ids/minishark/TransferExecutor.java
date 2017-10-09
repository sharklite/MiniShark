package ids.minishark;

import com.sun.istack.internal.NotNull;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

final class TransferExecutor {

    private static int batch = DataBase.batch;

    private TransferExecutor() {

    }
    private static final Set<Class<?>> notNulls =new HashSet<>();

    static {
        notNulls.add(byte.class);
        notNulls.add(short.class);
        notNulls.add(int.class);
        notNulls.add(long.class);
        notNulls.add(float.class);
        notNulls.add(double.class);
        notNulls.add(char.class);
    }

    @NotNull
    static <T> List<T> queryBatch(Collection<T> collection, Transfer<T> transfer) {
        List<T> list = new ArrayList<>();
        if (collection.size() == 0 || transfer.primaryKeys.size() == 0)
            return list;
        Connection conn = transfer.getConnection();
        try {
            for (T entity : collection) {
                List<Object> values = transfer.queryOneBuilder(entity);
                PreparedStatement pst = conn.prepareStatement(transfer.select_one);
                TransferExecutor.invokePreparedStatement(pst, values, transfer.pkJdbcType);
                ResultSet rs = pst.executeQuery();
                while (rs.next()) {
                    list.add(entity);
                    for (String label : transfer.colFieldMapper.values()) {
                        Object v = rs.getObject(label);
                        transfer.setFieldValue(label, v);
                    }
                }
                DataBase.close(rs);
                DataBase.close(pst);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(TransferExecutor.class.getName() + transfer.eClass + "\n" + transfer.select_one);
        } finally {
            DataBase.close(conn);
        }
        return list;
    }


    static <T> void modifyBatch(Collection<T> collection, Transfer<T> transfer) {
        if (collection.size() == 0 || transfer.primaryKeys.size() == 0)
            return;
        Connection conn = transfer.getConnection();
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(transfer.modify_one);
            int i = batch + 1;
            for (T entity : collection) {
                List<Object> values = transfer.modifyOneBuilder(entity);
                TransferExecutor.invokePreparedStatement(pst, values, transfer.jdbcTypeForModify);
                pst.addBatch();
                if (i % batch == 0) {
                    pst.executeBatch();
                    pst.clearBatch();
                }
                i++;
            }
            pst.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            System.out.println(TransferExecutor.class.getName() + "\n" + transfer.modify_one);
        } finally {
            DataBase.close(pst);
            DataBase.close(conn);
        }
    }


    static <T> void deleteBatch(Collection<T> collection, Transfer<T> transfer) {
        if (collection.size() == 0 || transfer.primaryKeys.size() == 0)
            return;
        Connection conn = transfer.getConnection();
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement(transfer.delete_one);
            int i = batch + 1;
            for (T entity : collection) {
                List<Object> values = transfer.deleteOneBuilder(entity);
                TransferExecutor.invokePreparedStatement(pst, values, transfer.pkJdbcType);
                pst.addBatch();
                if (i % batch == 0) {
                    pst.executeBatch();
                    pst.clearBatch();
                }
                i++;
            }
            pst.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            System.out.println(TransferExecutor.class.getName() + transfer.eClass + "\n" + transfer.delete_one);
        } finally {
            DataBase.close(pst);
            DataBase.close(conn);
        }
    }


    static <T> void insertBatch(Collection<T> collection, Transfer<T> transfer) {
        if (collection.size() == 0 || transfer.primaryKeys.size() == 0)
            return;
        Connection conn = transfer.getConnection();
        PreparedStatement pst = null;
        ResultSet rsAuto = null;
        try {
            pst = conn.prepareStatement(transfer.insert_one, PreparedStatement.RETURN_GENERATED_KEYS);
            for (T entity : collection) {
                List<Object> values = transfer.insertOneBuilder(entity);
                TransferExecutor.invokePreparedStatement(pst, values, transfer.jdbcTypeForInsert);
                pst.executeUpdate();
                conn.commit();
                if (transfer.autoIncrementCol != null) {//有自增列
                    rsAuto = pst.getGeneratedKeys();
                    if (rsAuto.next()) {
                        Object o = rsAuto.getObject(1);//得到插入返回的值
                        Field field = transfer.fields.get(transfer.colFieldMapper.get(transfer.autoIncrementCol));//得到Field
                        if (field != null) {
                            Class fieldType = field.getType();//Field的类型
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
                            }
                            //将值赋给E的实例
                            transfer.setFieldValue(transfer.colFieldMapper.get(transfer.autoIncrementCol), o);
                        }
                    }
                    DataBase.close(rsAuto);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            System.out.println(TransferExecutor.class.getName() + "\n" + transfer.eClass + "\n" + transfer.insert_one);
        } finally {
            DataBase.close(rsAuto);
            DataBase.close(pst);
            DataBase.close(conn);
        }
    }


    static <T> List<T> executeQuery(boolean byPage, int startIndex, int rows, String preparedSql, Transfer<T> transfer, Object... supportedSQLArg) throws IllegalAccessException {
        List<T> list = new ArrayList<>();
        Set<String> set = new HashSet<>();
        PreparedStatement pst = null;
        ResultSet rs = null;
        Map<String, Field> stringFieldMap = transfer.fields;
        Class<T> beanClass = transfer.eClass;
        Connection conn = transfer.getConnection();
        int count = 0;
        try {
            pst = conn.prepareStatement(preparedSql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            for (int i = 0; i < supportedSQLArg.length; i++)
                invokePreparedStatement(pst, (i + 1), supportedSQLArg[i]);
            rs = pst.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String label = meta.getColumnLabel(i);
                set.add(label);
            }
            if (byPage)
                rs.absolute(startIndex - 1);
            while (rs.next()) {
                count++;
                T entity = beanClass.newInstance();
                for (String label : set) {
                    Object value = rs.getObject(label);
                    entityFieldValueSet(stringFieldMap.get(label), entity, value);
                }
                list.add(entity);
                if (byPage) {
                    if (count >= rows)
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print(TransferExecutor.class.getName() + "\n" + preparedSql);
        } finally {
            DataBase.close(rs);
            DataBase.close(pst);
            DataBase.close(conn);
        }
        return list;
    }

    private static void entityFieldValueSet(Field field, Object entity, Object value) throws IllegalAccessException {
        if (value == null)
            value = parseNullToValue(field);
        field.set(entity, value);
    }

    static int executeUpdate(Connection conn, String preparedSql, Object... supportedSQLArg) {
        PreparedStatement pst = null;
        int rows = 0;
        try {
            pst = conn.prepareStatement(preparedSql);
            for (int i = 0; i < supportedSQLArg.length; i++) {
                invokePreparedStatement(pst, (i + 1), supportedSQLArg[i]);
            }
            rows = pst.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(TransferExecutor.class.getName() + "\n" + preparedSql);
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            DataBase.close(pst);
            DataBase.close(conn);
        }
        return rows;
    }

    //得到第一列的值
    @NotNull
    private static List<Object> firstColumnValues(Connection conn, boolean firstRow, String preparedSql, Object... supportedSQLArg) {
        List<Object> list = new ArrayList<>();
        Object v;
        ResultSet rs = null;
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement(preparedSql);
            for (int i = 0; i < supportedSQLArg.length; i++)
                invokePreparedStatement(statement, (i + 1), supportedSQLArg[i]);
            rs = statement.executeQuery();
            if (firstRow)
                rs.setFetchSize(1);
            while (rs.next()) {
                v = rs.getObject(1);
                list.add(v);
                if (firstRow)
                    break;
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.print(TransferExecutor.class.getName() + "\n" + preparedSql);
        } finally {
            DataBase.close(rs);
            DataBase.close(statement);
            DataBase.close(conn);
        }
        return list;
    }

    //得到第一列的值
    @NotNull
    static List<Object> firstColumnValues(Connection conn, String preparedSql, Object... supportedSQLArg) {
        return firstColumnValues(conn, Boolean.FALSE, preparedSql, supportedSQLArg);
    }

    //得到第一列、第一行的值
    static Object getObject(Connection conn, String preparedSql, Object... supportedSQLArg) {
        List<Object> l = firstColumnValues(conn, Boolean.TRUE, preparedSql, supportedSQLArg);
        return l.size() == 0 ? null : l.get(0);
    }


    //处理PreparedStatement
    private static void invokePreparedStatement(PreparedStatement pst, List<Object> params, List<Integer> type) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            int code = MappedType.jdbcTypeOf(type.get(i));
            int parameterIndex = i + 1;
            Object param = params.get(i);
            if (code == MappedType.UNDEFINED) {
                invokePreparedStatement(pst, parameterIndex, param);
            } else {
                if (param == null)
                    pst.setNull(parameterIndex, code);
                else
                    pst.setObject(parameterIndex, param, code);
            }
        }
    }

    private static void invokePreparedStatement(PreparedStatement pst, int index, Object object) throws SQLException {
        int code = MappedType.tryFrom(object);
        if (code == MappedType.UNDEFINED)
            pst.setObject(index, object);
        else
            pst.setObject(index, object, code);
    }

    //根据类型得到基本类型的默认值
    static Object parseNullToValue(Field field) {
        Object o = null;
        Class c = field.getType();
        if (notNulls.contains(c)) {
            o = 0;
        } else if (c.equals(boolean.class)) {
            o = false;
        }
        return o;
    }
}
