package ids.sharklite.transfer.annotation;

import java.lang.annotation.*;
import java.sql.JDBCType;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DataType {
    JDBCType value();

}
