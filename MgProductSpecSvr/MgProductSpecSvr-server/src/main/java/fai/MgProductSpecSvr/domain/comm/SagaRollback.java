package fai.MgProductSpecSvr.domain.comm;

import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/9/6 9:33
 */
public interface SagaRollback {
    void rollback(TransactionCtrl tc);
}
