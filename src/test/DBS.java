package test;

import org.apache.commons.dbcp2.BasicDataSource;

import java.util.Properties;

public class DBS {

    private static String DRIVER=null;
    private static String URL=null;
    private static String USER=null;
    private static String PWD=null;
    private static int initialSize=0;
    private static int minIdle=1;
    private static int maxIdle=2;
    private static int maxActive=3;
    private static int maxWait=10000;


    private static BasicDataSource bds =null;

    static{
        try {
            Properties prop=new Properties();
            prop.load(DBS.class.getClassLoader().getResourceAsStream("db.properties"));
            DRIVER=prop.getProperty("driver");
            URL=prop.getProperty("url");
            USER=prop.getProperty("user");
            PWD=prop.getProperty("pwd");
            initialSize= Integer.parseInt(prop.getProperty("initialSize"));
            minIdle= Integer.valueOf(prop.getProperty("minIdle"));
            maxIdle= Integer.valueOf(prop.getProperty("maxIdle"));
            maxActive= Integer.valueOf(prop.getProperty("maxActive"));
            maxWait= Integer.valueOf(prop.getProperty("maxWait"));

            bds=new BasicDataSource();
            bds.setDriverClassName(DRIVER);
            bds.setUrl(URL);
            bds.setUsername(USER);
            bds.setPassword(PWD);
            bds.setInitialSize(initialSize);
            bds.setMinIdle(minIdle);
            bds.setMaxTotal(maxActive);
            bds.setMaxIdle(maxIdle);
            bds.setMaxWaitMillis(maxWait);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static BasicDataSource getBasicDataSource(){
        return bds;
    }


}
