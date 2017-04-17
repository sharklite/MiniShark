package ids.minishark;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

final class BatchExecutor {

    private static final int BATCH=DataBase.batch;

    private BatchExecutor(){

    }

    static <T> List<T> queryBatch(Collection<T> collection,Transfer<T> transfer){
        List<T> list=new ArrayList<>();
        if(collection.size()==0||transfer.primaryKeys.size()==0)
            return list;
        Connection conn=transfer.getConnection();
        try {
            for(T entity:collection){
                List<Object> values=transfer.queryOneBuilder(entity);
                PreparedStatement pst=conn.prepareStatement(transfer.select_one);
                _Transfer_.invokePreparedStatement(pst,values,transfer.pkJdbcType);
                ResultSet rs=pst.executeQuery();
                while(rs.next()){
                    list.add(entity);
                    for(String label:transfer.colFieldMapper.values()){
                        Object v=rs.getObject(label);
                        transfer.setFieldValue(label,v);
                    }
                }
                DataBase.close(rs);
                DataBase.close(pst);
            }
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println(BatchExecutor.class.getName()+ transfer.eClass+"\n"+ transfer.select_one);
        }finally {
            DataBase.close(conn);
        }
        return list;
     }

    static <T> void modifyBatch(Collection<T> collection,Transfer<T> transfer){
         if(collection.size()==0||transfer.primaryKeys.size()==0)
             return;
         Connection conn=transfer.getConnection();
         PreparedStatement pst =null;
         try {
             pst = conn.prepareStatement(transfer.modify_one);
             int i=BATCH+1;
             for (T entity:collection){
                 List<Object> values=transfer.modifyOneBuilder(entity);
                 _Transfer_.invokePreparedStatement(pst,values,transfer.jdbcTypeForModify);
                 pst.addBatch();
                 if(i%BATCH==0){
                     pst.executeBatch();
                     pst.clearBatch();
                 }
                 i++;
             }
             pst.executeBatch();
             conn.commit();
         }catch (SQLException e){
             e.printStackTrace();
             try {
                 conn.rollback();
             } catch (SQLException ex) {
                 ex.printStackTrace();
             }
             System.out.println(BatchExecutor.class.getName()+"\n"+  transfer.modify_one);
         }finally{
             DataBase.close(pst);
             DataBase.close(conn);
         }
     }

    static <T> void deleteBatch(Collection<T> collection,Transfer<T> transfer){
         if(collection.size()==0||transfer.primaryKeys.size()==0)
             return;
        Connection conn=transfer.getConnection();
         PreparedStatement pst =null;
         try {
             pst = conn.prepareStatement(transfer.delete_one);
             int i=BATCH+1;
             for (T entity:collection){
                 List<Object> values=transfer.deleteOneBuilder(entity);
                 _Transfer_.invokePreparedStatement(pst,values,transfer.pkJdbcType);
                 pst.addBatch();
                 if(i%BATCH==0){
                     pst.executeBatch();
                     pst.clearBatch();
                 }
                 i++;
             }
             pst.executeBatch();
             conn.commit();
         }catch (SQLException e){
             e.printStackTrace();
             try {
                 conn.rollback();
             } catch (SQLException ex) {
                 ex.printStackTrace();
             }
             System.out.println(BatchExecutor.class.getName()+  transfer.eClass+"\n"+   transfer.delete_one);
         }finally{
             DataBase.close(pst);
             DataBase.close(conn);
         }
     }

    static <T> void insertBatch(Collection<T> collection,Transfer<T> transfer){
        if(collection.size()==0||transfer.primaryKeys.size()==0)
            return;
        Connection conn=transfer.getConnection();
        PreparedStatement pst =null;
        ResultSet rsAuto=null;
        try {
            pst = conn.prepareStatement(transfer.insert_one,PreparedStatement.RETURN_GENERATED_KEYS);
            for (T entity:collection){
                List<Object> values=transfer.insertOneBuilder(entity);
                _Transfer_.invokePreparedStatement(pst,values,transfer.jdbcTypeForInsert);
                pst.executeUpdate();
                conn.commit();
                if(transfer.autoIncrementCol!=null){
                    rsAuto = pst.getGeneratedKeys();
                    if(rsAuto.next()) {
                        Object o = rsAuto.getObject(1);
                        Field field = transfer.fields.get(transfer.colFieldMapper.get(transfer.autoIncrementCol));
                        if (field != null) {
                            Class fieldType = field.getType();
                            if (o instanceof Number) {
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
                            transfer.setFieldValue(transfer.colFieldMapper.get(transfer.autoIncrementCol), o);
                        }
                    }
                    DataBase.close(rsAuto);
                }
            }
        }catch (SQLException e){
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            System.out.println(BatchExecutor.class.getName()+"\n" +  transfer.eClass+"\n"+  transfer.insert_one);
        }finally{
            DataBase.close(rsAuto);
            DataBase.close(pst);
            DataBase.close(conn);
        }
    }


}
