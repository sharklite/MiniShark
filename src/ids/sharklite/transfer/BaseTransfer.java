package ids.sharklite.transfer;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.*;

abstract class BaseTransfer<E> {

    DataSource dataSource;
    TransferConfig config;
    //CRUD语句，带 '?'
    String select_one;
    String insert_one;
    String update_one;
    String delete_one;

    /**
     * 表中的主键,及class中带Condition注解的字段
     */
    Set<String> conditions;
    /**
     * E 的类本身
     */
    Class<E> eClass;
    /**
     * 自增列
     */
    String autoIncrementCol;
    /**
     * 属性名与属性的对应关系
     */
    Map<String, Field> fields;
    /**
     * 列名与属性名的对应关系
     */
    Map<String, String> colFieldMapper;
    /**
     * 不从属性对应的数据库列读取
     */
    Set<String> notReads;
    /**
     * 所有列的查询语句
     */
    String selectAll;
    /**
     * 数据库表名
     */
    String tableName;
    /**
     * 查询所有，所有的列有注解则以注解值作为别名
     */
    String allColumnLabels;
    /**
     * 只读列
     */
    Set<String> readOnlyColumns;
    /**
     * E 的实体类
     * 始终为 private
     */
    E entity;
    /**
     * 属性对应的数据库jdbc类型
     */
    Map<String, JDBCType> jdbcTypes;

    final String getLeftWrap() {
        return config == null || config.getLeftWrap() == null ? "" : config.getLeftWrap();
    }

    final String getRightWrap() {
        return config == null || config.getRightWrap() == null ? "" : config.getRightWrap();
    }

    final Connection getConnection() {
        Connection c = null;
        try {
            c = this.dataSource.getConnection();
            c.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return c;
    }

    public abstract void insert(E e) throws SQLException;

    public abstract void insert(Collection<E> collection) throws SQLException;

    public abstract void update(E e) throws SQLException;

    public abstract void update(Collection<E> collection) throws SQLException;

    public abstract void delete(E e) throws SQLException;

    public abstract void delete(Collection<E> collection) throws SQLException;

    public abstract E select(E e) throws SQLException;

    public abstract List<E> select(Collection<E> Collection) throws SQLException;

}
