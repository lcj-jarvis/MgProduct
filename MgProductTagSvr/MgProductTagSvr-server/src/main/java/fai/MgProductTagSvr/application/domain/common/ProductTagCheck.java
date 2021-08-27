package fai.MgProductTagSvr.application.domain.common;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:25
 */
public class ProductTagCheck {

    public static class Limit {
        public static final int NAME_MAXLEN = 100;
    }

    /**
     * 检查标签的名称是否有效
     */
    public static boolean isNameValid(String name) {
        return !(name == null || name.isEmpty() || name.length() > Limit.NAME_MAXLEN);
    }
}
