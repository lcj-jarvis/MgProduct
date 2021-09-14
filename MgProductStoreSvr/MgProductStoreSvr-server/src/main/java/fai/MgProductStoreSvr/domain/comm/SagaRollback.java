package fai.MgProductStoreSvr.domain.comm;

import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/9/10 14:36
 */
public interface SagaRollback {
    void rollback(TransactionCtrl tc);
}
