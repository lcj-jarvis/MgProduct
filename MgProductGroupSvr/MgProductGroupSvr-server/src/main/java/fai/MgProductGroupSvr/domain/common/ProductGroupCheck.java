package fai.MgProductGroupSvr.domain.common;

public class ProductGroupCheck {

    public static class Limit {
        public static final int NAME_MAXLEN = 100;
    }
    public static boolean isNameValid(String name) {
        if (name == null || name.isEmpty() || name.length() > Limit.NAME_MAXLEN) {
            return false;
        }
        return true;
    }
}
