package ids.minishark;


final class _Util_ {

    private _Util_(){
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


}
