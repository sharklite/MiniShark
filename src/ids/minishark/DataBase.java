package ids.minishark;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class DataBase {

    private String[] classLocations;

    static final Hashtable<Class<?>,DataSource> CONFIG_DS=new Hashtable<>();

    static DataSource defaultDS;
    static int batch = 1024;
    private DataSource ds;

    public DataBase(DataSource dataSource,int batch){
        DataBase.batch=batch;
        this.ds=DataBase.defaultDS=dataSource;
    }

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
