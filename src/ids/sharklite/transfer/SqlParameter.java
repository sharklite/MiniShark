package ids.sharklite.transfer;

import java.sql.JDBCType;

public class SqlParameter {
    private final Object value;
    private final int type;

    public SqlParameter(Object v) {
        this.value = v;
        this.type = MappedType.tryFrom(v);
    }

    public SqlParameter(Object v, JDBCType jdbcType) {
        this.value = v;
        type = jdbcType.getVendorTypeNumber();
    }

    public Object getValue() {
        return value;
    }

    public int getType() {
        return type;
    }

}
