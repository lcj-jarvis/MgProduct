package fai.MgProductBasicSvr.domain.entity;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/6/28 19:30
 */
public class ProductSagaValObj {

    public static final class Status {
        /** 初始化 */
        public static final int INIT = 0;
        /** 已补偿 */
        public static final int ROLLBACK_OK = 1;
    }
}
