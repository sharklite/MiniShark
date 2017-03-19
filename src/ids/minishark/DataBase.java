package ids.minishark;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataBase {

    static DataSource ds;
    static int batch=512;

    public DataBase(DataSource dataSource,int batch){
        DataBase.batch=batch;
        DataBase.ds=dataSource;
    }

    public static void defaultBatch(int batch){
        DataBase.batch=batch;
    }

    public static void defaultDataSource(DataSource dataSource){
        ds=dataSource;
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
