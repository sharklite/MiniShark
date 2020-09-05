package ids.sharklite.transfer;


import ids.sharklite.transfer.annotation.*;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.*;


public class Transfer<E> extends BaseTransfer<E> {

    /*
     * 通过无参构造器构造时，要调用initializeConfig才能使用Transfer的功能
     * 或者调用级构造器super()
     * initializeConfig方法可以切换数据源
     * */
    public Transfer() {

    }

    public Transfer(TransferConfig config) {
        this.initializeConfig(config);
    }

    public Transfer(TransferConfig config, String tableName) {
        this.initializeConfig(config, tableName);
    }

    public void initializeConfig(TransferConfig config) {
        this.dataSource = config.getDataSource();
        this.config = config;
        this.createSelf(eClassGet(), null);
    }

    public void initializeConfig(TransferConfig config, String tableName) {
        this.dataSource = config.getDataSource();
        this.config = config;
        this.createSelf(eClassGet(), tableName);
    }

    /*
     *不传入tableName的，要在 E.class 上注解配置表名
     */
    public Transfer(DataSource dataSource) {
        this.dataSource = dataSource;
        this.createSelf(eClassGet(), null);
    }

    public Transfer(DataSource dataSource, String tableName) {
        this.dataSource = dataSource;
        this.createSelf(eClassGet(), tableName);
    }

    private static String removeFirstAnd(String where) {
        return where.trim().substring(4);
    }

    protected Class<E> getEntityClass() {
        return this.eClass;
    }

    protected E getEntity() {
        return this.entity;
    }

    /***
     * 查询所有的SQL
     * */
    public String getSelectAll() {
        return this.selectAll;
    }

    /***
     * 表名
     * */
    public String getTableName() {
        return this.tableName;
    }

    //通过继承Transfer时，得到E.class
    private Class<E> eClassGet() {
        Type genType = this.getClass().getGenericSuperclass();
        Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
        @SuppressWarnings("unchecked")
        Class<E> eClass = (Class<E>) params[0];
        return eClass;
    }

    //通过构造器或注解传入对应的数据库表，以注解传入的表为准,如果都没有则用类名做表名
    private void makeTableName(String inputTable) {
        this.tableName = inputTable;
        Table tableAnnotation = eClass.getAnnotation(Table.class);
        if (tableAnnotation != null)
            this.tableName = tableAnnotation.value();
        if (this.tableName == null)
            this.tableName = eClass.getSimpleName();
    }

    /**
     * java对象与数据库表之间的对应关系
     * 包括表的主键、自增列、只读列
     */
    void createSelf(Class<E> eClass, String table) {
        if (dataSource == null && table == null)
            return;
        this.eClass = eClass;
        makeTableName(table);
        notReads = new HashSet<>();
        //E 对应表的信息
        ColumnInformation column = new ColumnInformation(this);
        this.jdbcTypes = new HashMap<>();
        this.fields = new HashMap<>();
        this.colFieldMapper = new HashMap<>();
        this.autoIncrementCol = column.autoIncrement;
        this.conditions = column.conditions;
        this.readOnlyColumns = column.readOnlyColumns;
        Set<String> columnNames = column.columnType.keySet();
        Map<String, JDBCType> columnType = column.columnType;
        //有ColumnAnnotation的Field，用于简化SQL
        //生成属性名与属性，表字段，jdbc类型的映射
        Field[] fs = this.eClass.getDeclaredFields();
        for (Field f : fs) {
            f.setAccessible(true);
            String fieldName = f.getName();
            fields.put(fieldName, f);
            //属性与列的对应关系，以及自定义的只读字段
            //需要区别columnName和columnLabel，因为
            //当fieldName与columnName不同时，用注解Column设置正确的columnName以保证正确映射
            ReadOnly readOnly = f.getAnnotation(ReadOnly.class);
            Column columnAnnotation = f.getAnnotation(Column.class);
            IgnoreRead notRead = f.getAnnotation(IgnoreRead.class);
            String columnNm;
            if (columnAnnotation != null) {
                columnNm = columnAnnotation.value();
            } else {
                columnNm = fieldName;
            }
            //Field-Column 中列名和字段名都不能重复，键值一一对应,并且columnNm确定存在数据库表中
            boolean exist = columnNames.contains(columnNm);
            if (exist) {
                this.colFieldMapper.put(columnNm, fieldName);
                if (readOnly != null)
                    this.readOnlyColumns.add(columnNm);
                if (notRead != null)
                    notReads.add(columnNm);
            }
            DataType typeAnnotation = f.getAnnotation(DataType.class);
            JDBCType type = JDBCType.OTHER;
            if (typeAnnotation == null) {
                if (exist)
                    type = columnType.get(columnNm);
            } else {
                type = typeAnnotation.value();
            }
            jdbcTypes.put(fieldName, type);
            //带有Condition的Field会被当成有主键对应的列(查询条件列)处理
            Condition myPK = f.getAnnotation(Condition.class);
            if (myPK != null && exist)
                conditions.add(columnNm);
        }
        //拼装SQL
        StringBuilder columnAs = new StringBuilder();
        for (String columnName : this.colFieldMapper.keySet()) {
            String fieldLabel = this.colFieldMapper.get(columnName);
            if (fieldLabel != null && !notReads.contains(columnName)) {
                if (fieldLabel.equals(columnName)) {
                    columnAs.append(",").append(wrap(columnName));
                } else {
                    columnAs.append(",").append(wrap(columnName)).append(" AS ").append(wrap(fieldLabel));
                }

            }
        }
        this.allColumnLabels = columnAs.substring(1);
        try {
            this.makeUpdateOneSql();
            this.makeDeleteOneSql();
            this.makeSelectOneSql();
            this.makeInsertOneSql();
            this.entity = this.eClass.newInstance();
        } catch (ReflectiveOperationException ex) {
            ex.printStackTrace();
        }
        this.selectAll = "SELECT " + this.allColumnLabels + " FROM " + this.tableName;
    }

    /**
     * 通过属性名给entity的属性赋值
     */
    void setFieldValue(String fieldName, Object value) {
        try {
            Field field = fields.get(fieldName);
            value = Util.parseValueOrDefault(field, value);
            field.set(this.entity, value);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过属性名从entity取值
     */
    private Object getFieldValue(String field) {
        Object v = null;
        try {
            v = fields.get(field).get(this.entity);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return v;
    }

    /**
     * 插入数据
     */
    @Override
    public void insert(E e) throws SQLException {
        List<E> l = new ArrayList<>();
        l.add(e);
        this.insert(l);
    }

    @Override
    public void insert(Collection<E> collection) throws SQLException {
        TransferExecutor.insertBatch(collection, this);
    }

    /**
     * 根据实体类及对应主键修改数据
     */
    @Override
    public void update(E e) throws SQLException {
        List<E> l = new ArrayList<>();
        l.add(e);
        this.update(l);
    }

    @Override
    public void update(Collection<E> collection) throws SQLException {
        TransferExecutor.updateBatch(collection, this);
    }

    /**
     * 根据实体类及对应主键删除数据
     */
    @Override
    public void delete(E e) throws SQLException {
        List<E> l = new ArrayList<>();
        l.add(e);
        this.delete(l);
    }

    @Override
    public void delete(Collection<E> collection) throws SQLException {
        TransferExecutor.deleteBatch(collection, this);
    }

    /**
     * 根据实体类及对应主键查找数据
     */
    @Override
    public E select(E e) throws SQLException {
        if (e == null)
            return null;
        List<E> l = new ArrayList<>();
        l.add(e);
        l = this.select(l);
        return l.isEmpty() ? null : l.get(0);
    }

    @Override
    public List<E> select(Collection<E> Collection) throws SQLException {
        return TransferExecutor.selectBatch(Collection, this);
    }


    //单条数据sql生成
    private void makeSelectOneSql() {
        StringBuilder where = new StringBuilder();
        for (String pk : this.conditions) {
            where.append(" AND ").append(wrap(pk)).append("=?");
        }
        //select * 的具体字段
        String condition = removeFirstAnd(where.toString());
        this.select_one = "SELECT " + this.allColumnLabels + " FROM " + this.wrap(this.tableName) + " WHERE " + condition;
    }

    private void makeDeleteOneSql() {
        StringBuilder where = new StringBuilder();
        for (String pk : this.conditions) {
            where.append(" AND ").append(wrap(pk)).append("=?");
        }
        String condition = removeFirstAnd(where.toString());
        this.delete_one = "DELETE FROM " + this.wrap(this.tableName) + " WHERE " + condition;
    }

    private void makeInsertOneSql() {
        StringBuilder cols = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (String col : this.colFieldMapper.keySet()) {
            if (!col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)) {
                cols.append(',').append(wrap(col));
                values.append(",?");
            }
        }
        this.insert_one = "INSERT INTO " + this.tableName + " (" + cols.substring(1) + ") VALUES (" + values.substring(1) + ")";
    }

    private void makeUpdateOneSql() {
        StringBuilder sets = new StringBuilder();
        for (String col : this.colFieldMapper.keySet()) {
            if (!col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)) {
                sets.append(',').append(wrap(col)).append("=?");
            }
        }
        StringBuilder where = new StringBuilder();
        for (String col : this.conditions) {
            where.append(" AND ").append(wrap(col)).append("=?");
        }
        String condition = removeFirstAnd(where.toString());
        this.update_one = "UPDATE " + wrap(this.tableName) + " SET " + sets.substring(1) + " WHERE " + condition;
    }

    //获取对象个属性的值
    SqlParameter[] selectValues(E e) {
        SqlParameter[] parameters = new SqlParameter[this.conditions.size()];
        this.entity = e;
        int i = 0;
        for (String pk : conditions) {
            String field = this.colFieldMapper.get(pk);
            parameters[i++] = new SqlParameter(getFieldValue(field), jdbcTypes.get(field));
        }
        return parameters;
    }

    //这两个相同
    SqlParameter[] deleteValues(E e) {
        return selectValues(e);
    }

    SqlParameter[] updateValues(E e) {
        List<SqlParameter> list = new ArrayList<>();
        this.entity = e;
        for (String col : this.colFieldMapper.keySet()) {
            if (!this.conditions.contains(col) && !col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)) {
                String field = this.colFieldMapper.get(col);
                list.add(new SqlParameter(getFieldValue(field), jdbcTypes.get(field)));
            }
        }
        for (String col : this.conditions) {
            String field = this.colFieldMapper.get(col);
            list.add(new SqlParameter(getFieldValue(field), jdbcTypes.get(field)));
        }
        return list.toArray(new SqlParameter[0]);
    }

    SqlParameter[] insertValues(E e) {
        ArrayList<SqlParameter> list = new ArrayList<>();
        this.entity = e;
        for (String col : this.colFieldMapper.keySet()) {
            if (!col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)) {
                String field = this.colFieldMapper.get(col);
                list.add(new SqlParameter(getFieldValue(field), jdbcTypes.get(field)));
            }
        }
        return list.toArray(new SqlParameter[]{});
    }


    protected int executeUpdate(String preparedSql, SqlParameter... parameters) throws SQLException {
        return TransferExecutor.executeUpdate(getConnection(), preparedSql, parameters);
    }

    protected Object queryScalar(String preparedSql, SqlParameter... parameters) throws SQLException {
        return TransferExecutor.queryScalar(getConnection(), preparedSql, parameters);
    }

    private String wrap(String name) {
        return this.getLeftWrap() + name + this.getRightWrap();
    }

}
