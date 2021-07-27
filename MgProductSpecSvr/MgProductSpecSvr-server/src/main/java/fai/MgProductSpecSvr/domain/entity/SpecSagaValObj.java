package fai.MgProductSpecSvr.domain.entity;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/27 9:51
 */
public class SpecSagaValObj {

    public static final class Status {
        /** 初始化 */
        public static final int INIT = 0;
        /** 已补偿 */
        public static final int ROLLBACK_OK = 1;
    }
}
