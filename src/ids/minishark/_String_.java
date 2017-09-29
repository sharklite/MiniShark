package ids.minishark;


final class _String_ {
    //判断一个字符串是否为数字
    static boolean isNumeric(String s) {
        return s != null && s.trim().length() != 0 &&
                (s.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([0-9]+)[.]|([.]([0-9]+))?)$") || s.matches("^([-+]?\\d+.?\\d*)[Ee]([-+]?\\d+)$"));
    }

}
