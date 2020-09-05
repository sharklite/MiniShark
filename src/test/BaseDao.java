package test;

import ids.sharklite.transfer.Transfer;

import javax.sql.DataSource;

public abstract class BaseDao<T> extends Transfer<T> {


    public BaseDao(DataSource dataSource) {
        super(dataSource);
    }

    abstract void deleteById(int id);
    abstract T findById(int id);
}
