package ids.minishark;


final class Util {

    private Util() {
    }

    //判断一个字符串是否为数字
    static boolean isNumeric(Object o) {
        boolean f = o instanceof Number;
        if (!f && o != null) {
            String s = o.toString().trim();
            f = s.length() != 0 &&
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
        String v = String.valueOf(o).trim();
        return Boolean.parseBoolean(v) || (Util.isNumeric(v) && !"0".equals(v));
    }

}
