package ids.minishark;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

 final class _Transfer_ {

    private _Transfer_(){

    }

    static <T> List<T> executeQuery(Connection conn,String preparedSql, Class<T> beanClass,Object ...supportedSQLArg) throws IllegalAccessException {
        List<T> list=new ArrayList<>();
        Set<String> set=new HashSet<>();
        PreparedStatement pst=null;
        ResultSet rs=null;
        Map<String,Field> stringFieldMap=new HashMap<>();
        Field[] fields=beanClass.getDeclaredFields();
        for(Field field:fields){
            field.setAccessible(true);
            stringFieldMap.put(field.getName(),field);
        }
        try {
            pst=conn.prepareStatement(preparedSql);
            for(int i=0;i<supportedSQLArg.length;i++)
                invokePreparedStatement(pst,(i+1),supportedSQLArg[i]);
            rs=pst.executeQuery();
            ResultSetMetaData meta=rs.getMetaData();
            for(int i=1;i<=meta.getColumnCount();i++){
                String label=meta.getColumnLabel(i);
                set.add(label);
            }
            while (rs.next()){
                T entity= beanClass.newInstance();
                for(String label:set){
                    Object value=rs.getObject(label);
                    if(value!=null)
                        stringFieldMap.get(label).set(entity,value);
                }
                list.add(entity);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.print(_Transfer_.class.getName()+"\n"+ preparedSql);
        }finally {
            DataBase.close(rs);
            DataBase.close(pst);
            DataBase.close(conn);
        }
        return list;
    }

    static int executeUpdate(Connection conn,String preparedSql,Object...supportedSQLArg) {
        PreparedStatement pst=null;
        int rows=0;
        try {
            pst=conn.prepareStatement(preparedSql);
            for(int i=0;i<supportedSQLArg.length;i++){
                invokePreparedStatement(pst,(i+1),supportedSQLArg[i]);
            }
            rows=pst.executeUpdate();
            conn.commit();
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println(_Transfer_.class.getName()+"\n"+ preparedSql);
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }finally {
            DataBase.close(pst);
            DataBase.close(conn);
        }
        return rows;
    }

    private static List<Object> firstColumnValues(Connection conn,boolean firstRow, String preparedSql,Object ...supportedSQLArg){
        List<Object> list=new ArrayList<>();
        Object v;
        ResultSet rs=null;
        PreparedStatement statement=null;
        try{
            statement=conn.prepareStatement(preparedSql);
            for(int i=0;i<supportedSQLArg.length;i++)
                invokePreparedStatement(statement,(i+1),supportedSQLArg[i]);
            rs=statement.executeQuery();
            if(firstRow)
                rs.setFetchSize(1);
            while(rs.next()){
                v=rs.getObject(1);
                list.add(v);
                if(firstRow)
                    break;
            }
        }catch (Exception e){
            System.out.println(e.toString());
            System.out.print(_Transfer_.class.getName()+"\n"+ preparedSql);
        }finally {
            DataBase.close(rs);
            DataBase.close(statement);
            DataBase.close(conn);
        }
        return list;
    }

    static List<Object> firstColumnValues(Connection conn,String preparedSql,Object ...supportedSQLArg)  {
        return firstColumnValues(conn,false,preparedSql,supportedSQLArg);
    }

    static Object getObject(Connection conn,String preparedSql,Object ...supportedSQLArg)  {
        List<Object> l=firstColumnValues(conn,true,preparedSql,supportedSQLArg);
        return l.size()==0?null:l.get(0);
    }


    static void  invokePreparedStatement(PreparedStatement pst, List<Object> params , List<Integer> type) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            int code=type.get(i);
            code=MapType.jdbcTypeOf(code);
            int parameterIndex=i+1;
            Object param=params.get(i);
            if(code==MapType.UNDEFINED){
                invokePreparedStatement(pst,parameterIndex,param);
            }else{
                if(param==null)
                    pst.setNull(parameterIndex,code);
                else
                    pst.setObject(parameterIndex,param,code);
            }
        }
    }

    private static void  invokePreparedStatement(PreparedStatement pst, int index, Object object) throws SQLException {
        int code=MapType.tryFrom(object);
        if(code==MapType.UNDEFINED)
            pst.setObject(index,object);
        else
            pst.setObject(index,object,code);
    }

}

