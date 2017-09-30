package ids.minishark;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Date;
import java.util.List;

abstract class TransferBase {

    DataSource dataSource;

    abstract void setDataSource(DataSource dataSource);

    public DataSource getDataSource() {
        return dataSource;
    }

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

    protected List<Object> firstColumnValues(String preparedSql, Object... supportedSQLArg) {
        return TransferExecutor.firstColumnValues(getConnection(), preparedSql, supportedSQLArg);
    }

    protected Object getObject(String preparedSql, Object... supportedSQLArg) {
        return TransferExecutor.getObject(getConnection(), preparedSql, supportedSQLArg);
    }

    protected String getString(String preparedSql, Object... supportedSQLArg) {
        Object o = this.getObject(preparedSql, supportedSQLArg);
        return o == null ? null : o.toString();
    }

    protected BigDecimal getBigDecimal(String preparedSql, Object... supportedSQLArg) {
        Object object = this.getObject(preparedSql, supportedSQLArg);
        if (object instanceof BigDecimal) {
            return (BigDecimal) object;
        } else if (object == null) {
            return null;
        }
        String s = String.valueOf(object);
        if (!_Util_.isNumeric(object))
            throw new NumberFormatException(s + " is NaN.");
        return new BigDecimal(s);
    }

    protected boolean getBoolean(String preparedSql, Object... supportedSQLArg) {
        Object object = this.getObject(preparedSql, supportedSQLArg);
        if (object instanceof Boolean)
            return (boolean) object;
        String s = String.valueOf(object).trim();
        if (!Boolean.parseBoolean(s)) {
            return _Util_.isNumeric(object) && !(new BigDecimal(s).compareTo(BigDecimal.ZERO) == 0);
        }
        return Boolean.TRUE;
    }

    protected byte getByte(String preparedSql, Object... supportedSQLArg) {
        Number number = this.getBigDecimal(preparedSql, supportedSQLArg);
        return number == null ? 0 : number.byteValue();
    }

    protected short getShort(String preparedSql, Object... supportedSQLArg) {
        Number number = this.getBigDecimal(preparedSql, supportedSQLArg);
        return number == null ? 0 : number.shortValue();
    }

    protected int getInt(String preparedSql, Object... supportedSQLArg) {
        Number number = this.getBigDecimal(preparedSql, supportedSQLArg);
        return number == null ? 0 : number.intValue();
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
        Object object = this.getObject(preparedSql, supportedSQLArg);
        if (object instanceof Date) {
            return (Date) object;
        } else if (object == null) {
            return null;
        }
        String s = String.valueOf(object);
        if (!_Util_.isNumeric(object))
            throw new NumberFormatException(s + " is NaN.");
        return new Date(new BigDecimal(s).longValue());
    }

}
