package ids.minishark;


final class _Util_ {

    private _Util_(){
    }

    //判断一个字符串是否为数字
    static boolean isNumeric(Object o) {
        if (o == null)
            return false;
        if (o instanceof Number)
            return true;
        String s = o.toString().trim();
        return s.length() != 0 &&
                (s.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([0-9]+)[.]|([.]([0-9]+))?)$") || s.matches("^([-+]?\\d+.?\\d*)[Ee]([-+]?\\d+)$"));
    }


}
