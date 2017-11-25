package ids.minishark;

import com.sun.istack.internal.NotNull;
import ids.minishark.annotation.*;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;


public abstract class Transfer<E> extends TransferBase {

    //CRUD语句，带 '?'
    String select_one;
    String insert_one;
    String modify_one;
    String delete_one;
    /**
     * 用于插入行时，jdbc type按对应顺序保存
     */
    List<Integer> jdbcTypeForInsert;
    /**
     * 用于修改行时，jdbc type按对应顺序保存
     */
    List<Integer> jdbcTypeForModify;
    /**
     * 用于删除、查询时时，主键jdbc type按对应顺序保存
     */
    List<Integer> pkJdbcType;
    /**
     * 表中的主键
     */
    Set<String> primaryKeys;
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
    private String select_all;
//    private boolean datasourceBySetter = false;//是否是通过setDataSource方法初始化dataSource
    /**
     * 数据库表名
     */
    private String tableName;
    /**
     * 查询所有，所有的列有注解则以注解值作为别名
     */
    private String allColumnLabels;
    /**
     * 只读列
     */
    private Set<String> readOnlyColumns;
    /**
     * E 的实体类
     */
    private E entity;
    /**
     * 属性对应的数据库jdbc类型
     */
    private Map<String, Integer> jdbcTypes;

    /**
     * 要在 E 上注解配置表名
     */
    public Transfer() {
        this.init(eClassGet(), null);
    }

    public Transfer(String tableName) {
        this.init(eClassGet(), tableName);
    }

    //供getDefault使用
    Transfer(Class<E> eClass, String tableName) {
        this.init(eClass, tableName);
    }

    public static <T> Transfer<T> getDefault(Class<T> eClass) {
        return new DefaultTransfer<>(eClass);
    }

    private static String firstAnd(String condition) {
        String s = condition.trim().toUpperCase();
        if (s.startsWith("AND"))
            return " 1=1 " + condition;
        return condition;
    }

    protected E getEntity() {
        return this.entity;
    }

    @Override
    protected void setDataSource(DataSource dataSource) {
        DataBase.CONFIG_DATA_SOURCE.put(this.eClass,dataSource);
        this.init(this.eClass, this.tableName);
    }

    /***
     * 查询所有的SQL
     * */
    public String getSelectAll() {
        return this.select_all;
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
        Class<E> eClass = (Class) params[0];
        return eClass;
    }

    //CRUD by Primary Keys

    /**
     * java对象与数据库表之间的对应关系
     * 包括表的主键、自增列、只读列
     */
    private void init(Class<E> eClass, String table) {
        this.eClass = eClass;
        //配置对应的数据源
        Class<?> key = this.getClass();
        if (DataBase.CONFIG_DATA_SOURCE.containsKey(key)) {//是否通过包名配置了数据源
            this.dataSource = DataBase.CONFIG_DATA_SOURCE.get(key);
        } else {
            this.dataSource = DataBase.defaultDataSource;
        }
        if (this.dataSource == null) {
            if (DataBase.CONFIG_DATA_SOURCE.containsKey(key)) {
                System.err.println("error:there's no dataSource in Transfer<" + this.eClass.getName() + ">");
            } else {
                System.err.println("error:there's no default dataSource in Transfer");
            }
            return;
        }
        //通过构造器或注解传入对应的数据库表，以注解传入的表为准
        this.tableName = table;
        Table tableAnnotation = eClass.getAnnotation(Table.class);
        if (tableAnnotation != null)
            this.tableName = tableAnnotation.value();
        //E 对应表的信息
        ColumnInformation column = new ColumnInformation(this.tableName, this.eClass, this.getConnection());
        this.jdbcTypes = new HashMap<>();
        this.fields = new HashMap<>();
        this.colFieldMapper = new HashMap<>();

        this.autoIncrementCol = column.autoIncrement;
        this.primaryKeys = column.primaryKeys;
        this.readOnlyColumns = column.readOnlyColumns;
        Set<String> columnNames = column.columnClass.keySet();
        Map<String, Integer> columnType = column.columnType;
        //有ColumnAnnotation的Field，用于简化SQL
        Set<String> hasColumnAnnotation = new HashSet<>();
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
            String columnNm;
            if (columnAnnotation != null) {
                columnNm = columnAnnotation.value();
                hasColumnAnnotation.add(columnNm);
            } else {
                columnNm = fieldName;
            }
            //Field-Column 中列名和字段名都不能重复，键值一一对应,并且columnNm确定存在数据库表中
            boolean exist = columnNames.contains(columnNm);
            if (exist) {
                this.colFieldMapper.put(columnNm, fieldName);
                if (readOnly != null)
                    this.readOnlyColumns.add(columnNm);
            }
            JdbcType typeAnnotation = f.getAnnotation(JdbcType.class);
            int type = MappedType.UNDEFINED;
            if (typeAnnotation == null) {
                if (exist)
                    type = columnType.get(columnNm);
            } else {
                type = typeAnnotation.value();
            }
            jdbcTypes.put(fieldName, type);
            //带有ConditionKey注解的Field会被当成有主键对应的列处理
            ConditionKey myPK = f.getAnnotation(ConditionKey.class);
            if (myPK != null && exist)
                primaryKeys.add(columnNm);
        }
        //拼装SQL
        StringBuilder columnAs = new StringBuilder();
        for (String columnName : this.colFieldMapper.keySet()) {
            String fieldLabel = this.colFieldMapper.get(columnName);
            if (fieldLabel != null) {
                columnAs.append(",").append(columnName);
                if (hasColumnAnnotation.contains(fieldLabel))
                    columnAs.append(" AS ").append(fieldLabel);
            }
        }
        this.allColumnLabels = columnAs.substring(1);
        try {
            E e = this.eClass.newInstance();
            this.modifyOneBuilder(e);
            this.deleteOneBuilder(e);
            this.queryOneBuilder(e);
            this.insertOneBuilder(e);
            this.entity = e;
        } catch (ReflectiveOperationException ex) {
            ex.printStackTrace();
        }
        this.select_all = "SELECT " + this.allColumnLabels + " FROM " + this.tableName;
    }

    /**
     * 通过属性名给entity的属性赋值
     */
    void setFieldValue(String field, Object value) {
        try {
            if (value == null)
                value = TransferExecutor.parseNullToValue(fields.get(field));
            fields.get(field).set(this.entity, value);
        } catch (IllegalAccessException e) {
            System.out.println(this.eClass.getName() + " set value of " + field + " error.");
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
            System.out.println(this.eClass.getName() + " get value of " + field + " error.");
            e.printStackTrace();
        }
        return v;
    }

    /**
     * 插入数据
     */
    public void insert(E e) {
        this.entity = e;
        List<E> l = new ArrayList<>();
        l.add(e);
        this.insert(l);
    }

    public void insert(Collection<E> collection) {
        for (E e : collection) {
            beforeInsert(e);
        }
        TransferExecutor.insertBatch(collection, this);
    }

    List<Object> insertOneBuilder(E e) {
        this.entity = e;
        StringBuilder cols = new StringBuilder();
        StringBuilder values = new StringBuilder();
        List<Object> valueList = new ArrayList<>();
        List<Integer> jdbcTypeList = new ArrayList<>();
        for (String col : this.colFieldMapper.keySet()) {
            if (!col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)) {
                String field = this.colFieldMapper.get(col);
                valueList.add(getFieldValue(field));
                if (this.insert_one == null || this.jdbcTypeForInsert == null) {
                    cols.append(",").append(col);
                    values.append(",?");
                    jdbcTypeList.add(jdbcTypes.get(field));
                }
            }
        }
        if (this.insert_one == null || this.jdbcTypeForInsert == null) {
            this.insert_one = "INSERT INTO " + this.tableName + " (" + cols.substring(1) + ") VALUES (" + values.substring(1) + ")";
            this.jdbcTypeForInsert = jdbcTypeList;
        }
        return valueList;
    }

    /**
     * 根据实体类及对应主键修改数据
     */
    public void modify(E e) {
        List<E> l = new ArrayList<>();
        l.add(e);
        this.modify(l);
    }

    public void modify(Collection<E> collection) {
        for (E e : collection) {
            beforeModify(e);
        }
        TransferExecutor.modifyBatch(collection, this);
    }

    List<Object> modifyOneBuilder(E e) {
        List<Object> valueList = new ArrayList<>();
        if (this.primaryKeys.size() != 0) {
            this.entity = e;
            StringBuilder sets = new StringBuilder();
            List<Integer> jdbcTypeList = new ArrayList<>();
            for (String col : this.colFieldMapper.keySet()) {
                if (!this.primaryKeys.contains(col) && !col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)) {
                    String field = this.colFieldMapper.get(col);
                    valueList.add(getFieldValue(field));
                    if (this.modify_one == null || this.jdbcTypeForModify == null) {
                        sets.append(",").append(col).append("=?");
                        jdbcTypeList.add(jdbcTypes.get(field));
                    }
                }
            }
            StringBuilder whereByPK = new StringBuilder();
            for (String col : this.primaryKeys) {
                String field = this.colFieldMapper.get(col);
                valueList.add(getFieldValue(field));
                if (this.modify_one == null || this.jdbcTypeForModify == null) {
                    whereByPK.append(",And ").append(col).append("=?");
                    jdbcTypeList.add(jdbcTypes.get(field));
                }
            }
            if (whereByPK.length() != 0) {
                if (this.modify_one == null || this.jdbcTypeForInsert == null) {
                    this.modify_one = "UPDATE " + this.tableName + " SET " + sets.toString().substring(1) + "  WHERE 1=1 " + whereByPK.substring(1);
                    this.jdbcTypeForModify = jdbcTypeList;
                }
            }
        }
        return valueList;
    }

    /**
     * 根据实体类及对应主键删除数据
     */
    public void delete(E e) {
        List<E> l = new ArrayList<>();
        l.add(e);
        this.delete(l);
    }

    public void delete(Collection<E> collection) {
        for (E e : collection) {
            beforeDelete(e);
        }
        TransferExecutor.deleteBatch(collection, this);
    }

    List<Object> deleteOneBuilder(E e) {
        List<Object> valuesList = new ArrayList<>();
        if (this.primaryKeys.size() != 0) {
            this.entity = e;
            List<Integer> jdbcTypeList = new ArrayList<>();
            StringBuilder where = new StringBuilder();
            for (String pk : this.primaryKeys) {
                String field = this.colFieldMapper.get(pk);
                valuesList.add(getFieldValue(field));
                if (this.delete_one == null || this.pkJdbcType == null) {
                    where.append(",And ").append(pk).append("=?");
                    jdbcTypeList.add(jdbcTypes.get(field));
                }
            }
            if (where.length() != 0) {
                if (this.delete_one == null || this.primaryKeys == null) {
                    this.delete_one = "DELETE FROM " + this.tableName + " WHERE 1=1 " + where.substring(1);
                    this.pkJdbcType = jdbcTypeList;
                }
            }
        }
        return valuesList;
    }

    /**
     * 根据实体类及对应主键查找数据
     */
    public E query(E e) {
        List<E> l = new ArrayList<>();
        l.add(e);
        l = this.query(l);
        return l.isEmpty() ? null : l.get(0);
    }

    @NotNull
    public List<E> query(Collection<E> Collection) {
        List<E> list = TransferExecutor.queryBatch(Collection, this);
        for (E e : list) {
            afterQuery(e);
        }
        return list;
    }

    List<Object> queryOneBuilder(E e) {
        List<Object> values = new ArrayList<>();
        if (this.primaryKeys.size() != 0) {
            this.entity = e;
            List<Integer> types = new ArrayList<>();
            StringBuilder where = new StringBuilder();
            for (String pk : this.primaryKeys) {
                String field = this.colFieldMapper.get(pk);
                values.add(getFieldValue(field));
                if (this.select_one == null || this.pkJdbcType == null) {
                    where.append(",And ").append(pk).append("=?");
                    types.add(jdbcTypes.get(field));
                }
            }
            //select * 的具体字段
            if (this.select_one == null || this.pkJdbcType == null) {
                this.select_one = "SELECT " + this.allColumnLabels + " FROM " + this.tableName + " WHERE 1=1 " + where.substring(1);
                this.pkJdbcType = types;
            }
        }
        return values;
    }

    //在delete, modify, insert前，对entity进行操作
    protected void beforeDelete(E entity) {

    }

    protected void beforeInsert(E entity) {

    }

    protected void beforeModify(E entity) {

    }

    //在查询出结果后，对entity进行操作
    protected void afterQuery(E entity) {

    }

    /**
     * @param condition       按条件查询所有列,不要包含where关键字
     * @param supportedSQLArg 可变长参数，与condition中的 ? 对应
     */
    @NotNull
    protected List<E> query(String condition, Object... supportedSQLArg) {
        List<E> list = new ArrayList<>();
        try {
            condition = firstAnd(condition);
            list = TransferExecutor.executeQuery(Boolean.FALSE, 0, 0, this.select_all + " WHERE " + condition, this, supportedSQLArg);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        for (E e : list) {
            afterQuery(e);
        }
        return list;
    }

    /**
     * @param startIndex      起始列的行数
     * @param rows            从startIndex开始，查询出多少行
     * @param condition       按条件查询所有列,不要包含where关键字
     * @param supportedSQLArg 可变长参数，与condition中的 ? 对应
     */
    @NotNull
    protected List<E> query(int startIndex, int rows, String condition, Object... supportedSQLArg) {
        List<E> list = new ArrayList<>();
        try {
            condition = firstAnd(condition);
            list = TransferExecutor.executeQuery(Boolean.TRUE, startIndex, rows, this.select_all + " WHERE " + condition, this, supportedSQLArg);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        for (E e : list) {
            afterQuery(e);
        }
        return list;
    }

    public <T> Transfer<T> getDefault(Class<T> eClass, String table, DataSource dataSource) {
        DataBase.CONFIG_DATA_SOURCE.put(eClass, dataSource);
        return new DefaultTransfer<>(eClass, table);
    }

    public <T> Transfer<T> getDefault(Class<T> eClass, String table) {
        return new DefaultTransfer<>(eClass, table);
    }

    public <T> Transfer<T> getDefault(Class<T> eClass, DataSource dataSource) {
        DataBase.CONFIG_DATA_SOURCE.put(eClass, dataSource);
        return new DefaultTransfer<>(eClass);
    }

    /**
     * 修改一个entity
     * 只修改Enumeration中存在的属性名
     */
    public void saveWithNamed(E entity, Enumeration<String> enumFieldNames) {
        Set<String> set = new HashSet<>();
        while (enumFieldNames.hasMoreElements()) {
            set.add(enumFieldNames.nextElement());
        }
        this.saveWithNamed(entity, set);
    }

    /**
     * 修改一个entity
     * 只修改Set<String>中存在的属性名
     */
    public void saveWithNamed(E entity, Set<String> fieldNames) {
        if (entity != null) {
            this.entity = entity;
            Map<String, Object> map = new HashMap<>();
            for (String name : fieldNames) {
                if (fields.keySet().contains(name))
                    map.put(name, getFieldValue(name));
            }
            this.entity = this.query(entity);
            if (this.entity != null) {
                for (String name : map.keySet()) {
                    setFieldValue(name, map.get(name));
                }
                this.modify(this.entity);
            }
        }
    }

    public E setValues(E e, Map<String, Object> map) {
        if (e == null)
            return null;
        this.entity = e;
        for (String s : map.keySet()) {
            if (fields.keySet().contains(s))
                this.setFieldValue(s, map.get(s));
        }
        return this.entity;
    }

}
