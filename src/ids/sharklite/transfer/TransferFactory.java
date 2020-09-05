package ids.sharklite.transfer;

import javax.sql.DataSource;

public final class TransferFactory {

    private final TransferConfig config = new TransferConfig();

    public TransferConfig getConfig() {
        return config;
    }

    public void wrapLeftAndRight(String left, String right) {
        config.setLeftWrap(left);
        config.setLeftWrap(right);
    }

    TransferFactory(DataSource dataSource) {
        config.setDataSource(dataSource);
    }

    /**
     * 通过此方法生成的Transfer必须实现无参构造方法
     */
    public <T extends Transfer<?>> T generate(Class<T> transferClass, String tableName) throws IllegalAccessException, InstantiationException {
        T t = transferClass.newInstance();
        t.initializeConfig(this.config, tableName);
        return t;
    }

    public <T extends Transfer<?>> T generate(Class<T> transferClass) throws IllegalAccessException, InstantiationException {
        T t = transferClass.newInstance();
        t.initializeConfig(this.config);
        return t;
    }

    /*
        工厂用于自动生成已实现的Transfer
    */
    public static TransferFactory newInstance(DataSource dataSource) {
        return new TransferFactory(dataSource);
    }


}
