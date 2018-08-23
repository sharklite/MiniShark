package ids.minishark;


final class Util {

    private Util() {
    }

    //判断一个对象是否为数字
    static boolean isNumeric(Object o) {
        boolean f = o instanceof Number;
        if (!f && o != null) {
            String s = o.toString().trim();
            f = !s.isEmpty() &&
                    (s.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([0-9]+)[.]|([.]([0-9]+))?)$") || s.matches("^([-+]?\\d+.?\\d*)[Ee]([-+]?\\d+)$"));
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
