package ids.minishark;


import java.util.regex.Pattern;

final class Util {

    private static final Pattern NUMERIC_1 = Pattern.compile("^[-+]?(([0-9]+)([.]([0-9]+))?|([0-9]+)[.]|([.]([0-9]+))?)$");
    private static final Pattern NUMERIC_2 = Pattern.compile("^([-+]?\\d+.?\\d*)[Ee]([-+]?\\d+)$");

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

}
