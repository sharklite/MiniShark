package test;

import ids.minishark.Transfer;

public abstract class BaseDao<T> extends Transfer<T> {

    abstract void deleteById(int id);
    abstract T findById(int id);
}
