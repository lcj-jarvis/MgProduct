package fai.MgProductStoreSvr.domain.entity;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/12 13:41
 */
public class StoreSagaValObj {

    public static final class Status {
        /** 初始化 */
        public static final int INIT = 0;
        /** 已补偿 */
        public static final int ROLLBACK_OK = 1;
    }
}
