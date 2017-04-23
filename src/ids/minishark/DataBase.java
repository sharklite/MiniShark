package ids.minishark;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class DataBase {

    //Transfer及对应数据源
    static final Hashtable<Class<?>,DataSource> CONFIG_DS=new Hashtable<>();

    static DataSource defaultDS;//Transfer默认的数据源，
    static int batch = 1024;//批处理时一次调教的数量
    private DataSource ds;//Transfer指定的数据源，适用于多数据源的情况，单数据源时与defaultDS相同


    public DataBase(DataSource dataSource){
        this.ds=DataBase.defaultDS=dataSource;
    }

    //设置包名，哪些Transfer调用此对应的数据源，适用于多数据源的情况
    //使用单一数据源时，可不用此项，默认使用defaultDS
    public void classLocationsConfig(String[] packageName){
        Set<Class<?>> set= new HashSet<>();
        for (String s:packageName){
            set.addAll(ClassesScanner.getClasses(s));
        }
        for(Class key:set){
            if(ITransfer.class.isAssignableFrom(key))
                CONFIG_DS.put(key,this.ds);
        }
        set.clear();
    }

    public static void defaultBatch(int batch){
        DataBase.batch=batch;
    }

    //适用于单数据源
    public static void defaultDataSource(DataSource dataSource){
        defaultDS=dataSource;
    }

    static void close(Connection c){
        try{
            if(c!=null){
                c.setAutoCommit(true);
                c.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void close(ResultSet rs){
        try{
            if(rs!=null)
                rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void close(Statement statement){
        try{
            if(statement!=null)
                statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
