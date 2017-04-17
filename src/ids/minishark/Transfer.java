package ids.minishark;

import ids.minishark.annotation.*;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.*;


public abstract class Transfer<E> implements ITransfer{


    String select_one;
    String insert_one;
    String modify_one;
    String delete_one;
    private String select_all;

    private boolean dsBySetter=false;
    private DataSource dataSource;

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        this.dsBySetter=true;
        this.init(this.eClass,this.tableName);
    }

    Connection getConnection(){
        Connection c=null;
        try {
            c=this.dataSource.getConnection();
            c.setAutoCommit(false);
        }catch (Exception e){
            e.printStackTrace();
        }
        return c;
    }

    /**
     * 用于插入行时，jdbc type按对应顺序保存
     * */
    List<Integer> jdbcTypeForInsert;

    /**
     * 用于修改行时，jdbc type按对应顺序保存
     * */
    List<Integer> jdbcTypeForModify;

    /**
     * 用于删除、查询时时，主键jdbc type按对应顺序保存
     * */
    List<Integer> pkJdbcType;

    /**
     * 数据库表名
     * */
    private String tableName;

    /**
     * 查询所有，所有的列以属性作为别名
     * */
    private String allColumnLabels;

    /**
     * 表中的主键
     * */
    Set<String> primaryKeys;

    /**
     * 只读列
     * */
    private Set<String> readOnlyColumns;

    /**
     * E 的实体类
     * */
    private E entity;

    /**
     * E 的类本身
     * */
    Class<E> eClass;

    /**
     * 自增列
     * */
    String autoIncrementCol;

    /**
     * 属性对应的数据库jdbc类型
     * */
    private Map<String,Integer> jdbcTypes;

    /**
     * 属性名与属性的对应关系
     * */
    Map<String,Field> fields;

    /**
     * 列名与属性名的对应关系
     * */
    Map<String ,String> colFieldMapper;

    /***
     * 查询所有的SQL
     * */
    public String getSelectAll(){
        return this.select_all;
    }
    /***
     * 表名
     * */
    public String getTableName(){
        return this.tableName;
    }

    //档通过继承Transfer时，得到E.class
    private Class<E> eClassGet(){
        Type genType = this.getClass().getGenericSuperclass();
        Type[] params = ((ParameterizedType)genType).getActualTypeArguments();
        @SuppressWarnings("unchecked")
        Class<E> eClass=(Class)params[0];
        return eClass;
    }
    /**
     * 要在 E 上注解配置表名
     * */
    public Transfer(){
        this.init(eClassGet(),null);
    }

    public Transfer(String tableName){
        this.init(eClassGet(),tableName);
    }
    Transfer(Class<E> eClass,String tableName){
        this.init(eClass,tableName);
    }



    /**
     * java对象与数据库表之间的对应关系
     * 包括表的主键、自增列、只读列
     * */
    void init(Class<E> eClass,String table) {
        this.eClass=eClass;
        Class<?> key=this.getClass();
        if(this.dsBySetter){
            DataBase.CONFIG_DS.put(key,this.dataSource);
        }else {
            if(DataBase.CONFIG_DS.containsKey(key)){
                this.dataSource=DataBase.CONFIG_DS.get(key);
            }else {
                this.dataSource=DataBase.defaultDS;
            }
        }
        if(this.dataSource==null){
            if (this.dsBySetter||DataBase.CONFIG_DS.containsKey(key))
                System.out.println("no dataSource in Transfer<"+ this.eClass.getName()+">");
            return;
        }

        //通过构造器或注解传入对应的数据库表，以注解传入的表为准
        this.tableName=table;
        Table tableAnnotation= eClass.getAnnotation(Table.class);
        if(tableAnnotation!=null)
            this.tableName=tableAnnotation.value();
        ColumnInformation column=new ColumnInformation(this.tableName,this.eClass,this.getConnection());
        this.jdbcTypes=new HashMap<>();
        this.fields=new HashMap<>();
        this.colFieldMapper=new HashMap<>();

        this.autoIncrementCol=column.autoIncrement;
        this.primaryKeys=column.primaryKeys;
        this.readOnlyColumns=column.readOnlyColumns;
        Set<String> columnNames=column.columnClass.keySet();
        Map<String,Integer> columnType=column.columnType;

        //生成属性名与属性，表字段，jdbc类型的映射
        Field[] fs=this.eClass.getDeclaredFields();
        for(Field f:fs){
            f.setAccessible(true);
            String fieldName=f.getName();
            fields.put(fieldName,f);
            //属性与列的对应关系，以及自定义的只读字段
            ReadOnly readOnly=f.getAnnotation(ReadOnly.class);
            Column columnAnnotation=f.getAnnotation(Column.class);
            String columnNm;
            if(columnAnnotation!=null){
                columnNm=columnAnnotation.value();
            }else{
                columnNm=fieldName;
            }
            //Field-Column 中列名和字段名都不能重复，键值一一对应,并且数据库中存在此列
            boolean exist=columnNames.contains(columnNm);
            if(exist){
                this.colFieldMapper.put(columnNm,fieldName);
                if(readOnly!=null)
                    this.readOnlyColumns.add(columnNm);
            }
            JdbcType typeAnnotation=f.getAnnotation(JdbcType.class);
            int t=MapType.UNDEFINED;
            if(typeAnnotation==null){
                if(exist)
                    t=columnType.get(columnNm);
            }else{
                t=typeAnnotation.value();
            }
            jdbcTypes.put(fieldName,t);
            ConditionKey myPK=f.getAnnotation(ConditionKey.class);
            if(myPK!=null && exist)
                primaryKeys.add(columnNm);
        }
        StringBuilder columnAs=new StringBuilder();
        for(String col:this.colFieldMapper.keySet()){
            String label=this.colFieldMapper.get(col);
            if(label!=null)
                columnAs.append(",").append(col).append(" AS ").append(label);
        }
        this.allColumnLabels=columnAs.substring(1);
        E e= null;
        try {
            e = this.eClass.newInstance();
        } catch (ReflectiveOperationException roe) {
            roe.printStackTrace();
        }
        this.modifyOneBuilder(e);
        this.deleteOneBuilder(e);
        this.queryOneBuilder(e);
        this.insertOneBuilder(e);
        this.select_all="Select "+ this.allColumnLabels+" From "+this.tableName;

    }

    /**
     * 通过属性名赋值
     * */
    void setFieldValue(String field, Object value){
        try {
            if(value==null)
                value=_Transfer_.parseNullToValue(fields.get(field));
             fields.get(field).set(this.entity,value);
        } catch (IllegalAccessException e) {
            System.out.println(this.eClass.getName()+" set value of "+field+" error.");
            e.printStackTrace();
        }
    }

    /**
     * 通过属性名取值
     * */
    private Object getFieldValue(String field){
        Object v=null;
        try {
            v=fields.get(field).get(this.entity);
        } catch (IllegalAccessException e) {
            System.out.println(this.eClass.getName()+" get value of "+field+" error.");
            e.printStackTrace();
        }
        return  v;
    }

    //CRUD by Primary Keys
    /**
     * 插入数据
     * */
    public void insert(E e) {
        this.entity=e;
        List<E> l=new ArrayList<>();
        l.add(e);
        this.insert(l);
    }
    public void insert(Collection<E> connection){
        BatchExecutor.insertBatch(connection,this);
    }
    List<Object> insertOneBuilder(E e){
        this.entity=e;
        StringBuilder cols=new StringBuilder();
        StringBuilder values=new StringBuilder();
        List<Object> valueList=new ArrayList<>();
        List<Integer> jdbcTypeList=new ArrayList<>();
        for(String col:this.colFieldMapper.keySet()){
            if(!col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)){
                String field=this.colFieldMapper.get(col);
                valueList.add(getFieldValue(field));
                if(this.insert_one==null || this.jdbcTypeForInsert==null){
                    cols.append(",").append(col);
                    values.append(",?");
                    jdbcTypeList.add(jdbcTypes.get(field));
                }
            }
        }
        if(this.insert_one==null || this.jdbcTypeForInsert==null){
            this.insert_one="INSERT INTO "+this.tableName+" ("+cols.substring(1)+") VALUES ("+values.substring(1)+")";
            this.jdbcTypeForInsert=jdbcTypeList;
        }
        return valueList;
    }

    /**
     * 根据实体类及对应主键修改数据
     */
    public void modify(E e) {
        List<E> l=new ArrayList<>();
        l.add(e);
        this.modify(l);
    }
    public void modify(Collection<E> Collection){
        BatchExecutor.modifyBatch(Collection,this);
    }
    List<Object> modifyOneBuilder(E e){
        List<Object> valueList=new ArrayList<>();
        if(this.primaryKeys.size()!=0){
            this.entity=e;
            StringBuilder sets=new StringBuilder();
            List<Integer> jdbcTypeList=new ArrayList<>();
            for(String col:this.colFieldMapper.keySet()){
                if(!this.primaryKeys.contains(col) && !col.equals(autoIncrementCol) && !readOnlyColumns.contains(col)){
                    String field=this.colFieldMapper.get(col);
                    valueList.add(getFieldValue(field));
                    if(this.modify_one==null || this.jdbcTypeForModify==null) {
                        sets.append(",").append(col).append("=?");
                        jdbcTypeList.add(jdbcTypes.get(field));
                    }
                }
            }
            StringBuilder whereByPK=new StringBuilder();
            for(String col:this.primaryKeys){
                String field=this.colFieldMapper.get(col);
                valueList.add(getFieldValue(field));
                if(this.modify_one==null || this.jdbcTypeForModify==null) {
                    whereByPK.append(",And ").append(col).append("=?");
                    jdbcTypeList.add(jdbcTypes.get(field));
                }
            }
            if(whereByPK.length()!=0){
                if(this.modify_one==null || this.jdbcTypeForInsert==null){
                    this.modify_one="Update "+this.tableName +" Set "+sets.toString().substring(1)+"  Where 1=1 " +whereByPK.substring(1);
                    this.jdbcTypeForModify=jdbcTypeList;
                }
            }
        }
        return valueList;
    }

    /**
     * 根据实体类及对应主键删除数据
     */
    public void delete(E e) {
        List<E> l=new ArrayList<>();
        l.add(e);
        this.delete(l);
    }
    public void delete(Collection<E> Collection){
        BatchExecutor.deleteBatch(Collection,this);
    }
    List<Object> deleteOneBuilder(E e){
        List<Object> valuesList=new ArrayList<>();
        if(this.primaryKeys.size()!=0){
            this.entity=e;
            List<Integer> jdbcTypeList=new ArrayList<>();
            StringBuilder where=new StringBuilder();
            for(String pk: this.primaryKeys){
                String field=this.colFieldMapper.get(pk);
                valuesList.add(getFieldValue(field));
                if(this.delete_one==null || this.pkJdbcType==null){
                    where.append(",And ").append(pk).append("=?");
                    jdbcTypeList.add(jdbcTypes.get(field));
                }
            }
            if(where.length()!=0){
                if(this.delete_one==null || this.primaryKeys==null){
                    this.delete_one="Delete From "+this.tableName +" Where 1=1 "+ where.substring(1);
                    this.pkJdbcType=jdbcTypeList;
                }
            }
        }
        return valuesList;
    }

    /**
     * 根据实体类及对应主键查找数据
     * */
    public E query(E e){
        List<E> l=new ArrayList<>();
        l.add(e);
        l=this.query(l);
        return l.isEmpty()?null:l.get(0);
    }
    public List<E> query(Collection<E> Collection){
        return BatchExecutor.queryBatch(Collection,this);
    }
    List<Object> queryOneBuilder(E e){
        List<Object> values=new ArrayList<>();
        if(this.primaryKeys.size()!=0){
            this.entity=e;
            List<Integer> types=new ArrayList<>();
            StringBuilder where=new StringBuilder();
            for(String pk:this.primaryKeys){
                String field=this.colFieldMapper.get(pk);
                values.add(getFieldValue(field));
                if(this.select_one==null||this.pkJdbcType==null){
                    where.append(",And ").append(pk).append("=?");
                    types.add(jdbcTypes.get(field));
                }
            }
            //select * 的具体字段
            if(this.select_one==null || this.pkJdbcType==null){
                this.select_one="Select "+ this.allColumnLabels+" From "+this.tableName+ " WHERE 1=1 "+ where.substring(1);
                this.pkJdbcType=types;
            }
        }
        return values;
    }

    /**
     * @param condition 按条件查询所有列,不要包含where关键字
     * */
    protected List<E> query(String condition,Object ...supportedSQLArg) {
        List<E> list=new ArrayList<>();
        try {
            list= _Transfer_.executeQuery(getConnection(),this.select_all+" WHERE "+ condition, this.eClass, supportedSQLArg);
        }  catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return list;
    }

    protected int executeUpdate(String preparedSql,Object...supportedSQLArg){
        return _Transfer_.executeUpdate(getConnection(),preparedSql,supportedSQLArg);
    }
    protected List<Object> firstColumnValues(String preparedSql,Object ...supportedSQLArg){
        return _Transfer_.firstColumnValues(getConnection(),preparedSql,supportedSQLArg);
    }
    protected Object getObject(String preparedSql,Object ...supportedSQLArg)  {
        return _Transfer_.getObject(getConnection(),preparedSql,supportedSQLArg);
    }
    protected String getString(String preparedSql,Object ...supportedSQLArg){
        Object o=this.getObject(preparedSql,supportedSQLArg);
        return String.valueOf(o);
    }
    protected BigDecimal tryBigDecimal(String preparedSql, Object ...supportedSQLArg){
        Object object=this.getObject(preparedSql,supportedSQLArg);
        if(object instanceof BigDecimal){
            return (BigDecimal)object;
        }
        String s=String.valueOf(object).trim();
        return _String_.isNumeric(s)?new BigDecimal(s):null;
    }
    protected boolean getBoolean(String preparedSql,Object ...supportedSQLArg){
        Object object=this.getObject(preparedSql,supportedSQLArg);
        if(object instanceof Boolean)
            return (boolean)object;
        String s=String.valueOf(object).trim();
        return  s.equals("1")||Boolean.parseBoolean(s);
    }
    protected byte tryByte(String preparedSql,Object ...supportedSQLArg){
        Number number=this.tryNumber(preparedSql,supportedSQLArg);
        return number==null?0:number.byteValue();
    }
    protected short tryShort(String preparedSql,Object ...supportedSQLArg){
        Number number=this.tryNumber(preparedSql,supportedSQLArg);
        return number==null?0:number.shortValue();
    }
    protected int tryInt(String preparedSql,Object ...supportedSQLArg){
        Number number=this.tryNumber(preparedSql,supportedSQLArg);
        return number==null?0:number.intValue();
    }
    protected long tryLong(String preparedSql,Object ...supportedSQLArg){
        Number number=this.tryNumber(preparedSql,supportedSQLArg);
        return number==null?0:number.longValue();
    }
    protected double tryDouble(String preparedSql,Object ...supportedSQLArg){
        Number number=this.tryNumber(preparedSql,supportedSQLArg);
        return number==null?0:number.doubleValue();
    }
    private Number tryNumber(String preparedSql,Object ...supportedSQLArg){
        Number number=null;
        Object object=this.getObject(preparedSql,supportedSQLArg);
        if(object instanceof Number){
            number=(Number)object;
        }else{
            String s=String.valueOf(object).trim();
            if(_String_.isNumeric(s))
                number=new BigDecimal(s);
        }
        return number;
    }
    protected Date tryDate(String preparedSql, Object ...supportedSQLArg){
        Object object=this.getObject(preparedSql,supportedSQLArg);
        if(object instanceof Date){
            return (Date)object;
        }
        Number number=null;
        String s=String.valueOf(object).trim();
        if(_String_.isNumeric(s))
            number=new BigDecimal(s);
        return number==null?null:new Date(number.longValue());
    }


    public <T> Transfer<T> getDefualt(Class<T> eClass,String table,DataSource dataSource){
        Transfer<T> transfer=new DefaultTransfer<>(eClass,table);
        transfer.setDataSource(dataSource);
        return transfer;
    }
    public <T> Transfer<T> getDefualt(Class<T> eClass,String table){
        return new DefaultTransfer<>(eClass,table);
    }
    public <T> Transfer<T> getDefualt(Class<T> eClass,DataSource dataSource){
        Transfer<T> transfer=new DefaultTransfer<>(eClass);
        transfer.setDataSource(dataSource);
        return transfer;
    }
    public static <T> Transfer<T> getDefault(Class<T> eClass){
        return new DefaultTransfer<>(eClass);
    }

}
