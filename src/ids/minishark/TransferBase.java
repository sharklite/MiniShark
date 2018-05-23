package ids.minishark;

import com.sun.istack.internal.NotNull;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Date;
import java.util.List;

abstract class TransferBase {

    DataSource dataSource;

    public DataSource getDataSource() {
        return dataSource;
    }

    abstract void setDataSource(DataSource dataSource);

    protected Connection getConnection() {
        Connection c = null;
        try {
            c = this.dataSource.getConnection();
            c.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return c;
    }

    protected int executeUpdate(String preparedSql, Object... supportedSQLArg) {
        return TransferExecutor.executeUpdate(getConnection(), preparedSql, supportedSQLArg);
    }

    @NotNull
    @SuppressWarnings("unchecked")
    protected <T> List<T> firstColumnValues(String preparedSql, Object... supportedSQLArg) {
        return (List<T>) TransferExecutor.firstColumnValues(getConnection(), preparedSql, supportedSQLArg);
    }

    @SuppressWarnings("unchecked")
    protected <T> T getValue(String preparedSql, Object... supportedSQLArg) {
        return (T) TransferExecutor.getObject(getConnection(), preparedSql, supportedSQLArg);
    }

    protected String getString(String preparedSql, Object... supportedSQLArg) {
        Object o = this.getValue(preparedSql, supportedSQLArg);
        return o == null ? null : o.toString();
    }

    protected BigDecimal getBigDecimal(String preparedSql, Object... supportedSQLArg) {
        Object object = this.getValue(preparedSql, supportedSQLArg);
        if (object instanceof BigDecimal) {
            return (BigDecimal) object;
        } else if (object == null) {
            return null;
        }
        String s = String.valueOf(object);
        if (!Util.isNumeric(object))
            throw new NumberFormatException(s + " is NaN.");
        return new BigDecimal(s);
    }

    /**
     * if ResultSet can be cast to number,
     * 0 is false, others is true
     */
    protected boolean getBoolean(String preparedSql, Object... supportedSQLArg) {
        Object object = this.getValue(preparedSql, supportedSQLArg);
        return Util.toBoolean(object);
    }

    protected byte getByte(String preparedSql, Object... supportedSQLArg) {
        return (byte) this.getLong(preparedSql, supportedSQLArg);
    }

    protected short getShort(String preparedSql, Object... supportedSQLArg) {
        return (short) this.getLong(preparedSql, supportedSQLArg);
    }

    protected int getInt(String preparedSql, Object... supportedSQLArg) {
        return (int) this.getLong(preparedSql, supportedSQLArg);
    }

    protected long getLong(String preparedSql, Object... supportedSQLArg) {
        Number number = this.getBigDecimal(preparedSql, supportedSQLArg);
        return number == null ? 0 : number.longValue();
    }

    protected double getDouble(String preparedSql, Object... supportedSQLArg) {
        Number number = this.getBigDecimal(preparedSql, supportedSQLArg);
        return number == null ? 0 : number.doubleValue();
    }

    protected Date getDate(String preparedSql, Object... supportedSQLArg) {
        Object object = this.getValue(preparedSql, supportedSQLArg);
        if (object instanceof Date) {
            return (Date) object;
        } else if (object == null) {
            return null;
        }
        String s = String.valueOf(object);
        if (!Util.isNumeric(object))
            throw new NumberFormatException(s + " is NaN,and " + object + " can't cast to Date.");
        return new Date(new BigDecimal(s).longValue());
    }

}
