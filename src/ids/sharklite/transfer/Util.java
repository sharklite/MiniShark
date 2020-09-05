package ids.sharklite.transfer;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

final class Util {

    private static final Pattern NUMERIC_1 = Pattern.compile("^[-+]?(([0-9]+)([.]([0-9]+))?|([0-9]+)[.]|([.]([0-9]+))?)$");
    private static final Pattern NUMERIC_2 = Pattern.compile("^([-+]?\\d+.?\\d*)[Ee]([-+]?\\d+)$");
    private static final Set<Class<?>> NOT_NULLS = new HashSet<>();

    static {
        NOT_NULLS.add(byte.class);
        NOT_NULLS.add(short.class);
        NOT_NULLS.add(int.class);
        NOT_NULLS.add(long.class);
        NOT_NULLS.add(float.class);
        NOT_NULLS.add(double.class);
        NOT_NULLS.add(char.class);
    }

    private Util() {
    }

    //判断一个对象是否为数字
    static boolean isNumeric(Object o) {
        boolean f = o instanceof Number;
        if (!f && o != null) {
            String s = o.toString().trim();
            f = !s.isEmpty() &&
                    (NUMERIC_1.matcher(s).matches() || NUMERIC_2.matcher(s).matches());
        }
        return f;
    }

    //将一个对象转换为boolean
    static boolean toBoolean(Object o) {
        if (o instanceof Boolean)
            return (boolean) o;
        else if (o == null)
            return false;
        String v = o.toString().trim();
        return Boolean.parseBoolean(v) || (!"0".equals(v) && isNumeric(v));
    }


    static void close(AutoCloseable closeable) {
        try {
            if (closeable instanceof Connection)
                ((Connection) closeable).setAutoCommit(true);
            if (closeable != null)
                closeable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //根据类型得到值或者默认值
    static Object parseValueOrDefault(Field field, Object value) {
        Class<?> c = field.getType();
        if (value == null && NOT_NULLS.contains(c)) {
            value = 0;
        }
        if (boolean.class.equals(c) || Boolean.class.equals(c)) {
            value = Util.toBoolean(value);
        }
        return value;
    }

}
