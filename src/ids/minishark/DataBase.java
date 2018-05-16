package ids.minishark;


import javax.sql.DataSource;
import java.util.*;

public class DataBase {

    //Transfer及对应数据源
    static final Hashtable<Class<?>, DataSource> CONFIG_DATA_SOURCE = new Hashtable<>();

    static DataSource defaultDataSource;//Transfer默认的数据源，
    static int batch = 1024;//批处理时一次commit的数量
    private DataSource ds;//Transfer指定的数据源，适用于多数据源的情况，单数据源时与defaultDS相同


    public DataBase(DataSource dataSource) {
        this.setDataSource(dataSource);
    }

    public DataBase() {
    }

    public static void defaultBatch(int batch) {
        DataBase.batch = batch;
    }

    //适用于单数据源
    public static void defaultDataSource(DataSource dataSource) {
        defaultDataSource = dataSource;
    }


    static void close(AutoCloseable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void setDataSource(DataSource dataSource) {
        this.ds = dataSource;
        if (DataBase.defaultDataSource == null)
            DataBase.defaultDataSource = this.ds;
    }

    //设置包名，哪些Transfer调用此对应的数据源，适用于多数据源的情况
    //使用单一数据源时，可不用此项，默认使用defaultDS
    public void setPackageConfig(String[] packageName) {
        this.setPackageConfig(Arrays.asList(packageName));
    }

    public void setPackageConfig(String packageName) {
        Set<String> set = new HashSet<>();
        set.add(packageName);
        this.setPackageConfig(set);
    }

    public void setPackageConfig(Collection<String> packageName) {
        Set<Class<?>> set = new HashSet<>();
        for (String s : packageName) {
            set.addAll(ClassesScanner.getClasses(s));
        }
        for (Class key : set) {
            if (TransferBase.class.isAssignableFrom(key))
                CONFIG_DATA_SOURCE.put(key, this.ds);
        }
        set.clear();
    }

    public <T extends TransferBase> void setClassConfig(Class<T> tClass) {
        if (TransferBase.class.isAssignableFrom(tClass))
            CONFIG_DATA_SOURCE.put(tClass, this.ds);
    }

    public <T extends TransferBase> void setClassConfig(Collection<Class<T>> collection) {
        for (Class key : collection) {
            if (TransferBase.class.isAssignableFrom(key))
                CONFIG_DATA_SOURCE.put(key, this.ds);
        }
    }

}
