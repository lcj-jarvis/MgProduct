package fai.MgProductLibSvr.domain.common;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:25
 */
public class ProductLibCheck {

    public static class Limit {
        public static final int NAME_MAXLEN = 100;
    }

    /**
     * 检查库的名称是否有效
     * @param name
     * @return
     */
    public static boolean isNameValid(String name) {
        return !(name == null || name.isEmpty() || name.length() > Limit.NAME_MAXLEN);
    }
}
