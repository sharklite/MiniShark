package ids.minishark;


final class _String_ {
    static String toUpperAtFirst(String s){
        if(s!=null&&s.length()>0){
            s=s.substring(0,1).toUpperCase() + s.substring(1);
        }
        return s;
    }
    static String toLowerAtFirst(String s){
        if(s!=null&&s.length()>0){
            s=s.substring(0,1).toLowerCase() + s.substring(1);
        }
        return s;
    }
    static boolean isNumeric(String s){
        return s!=null && s.trim().length()!=0 &&
                (s.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([0-9]+)[.]|([.]([0-9]+))?)$") || s.matches("^([-+]?\\d+.?\\d*)[Ee]([-+]?\\d+)$"));
    }
    static final String SET="set";
    static final String GET="get";
}
