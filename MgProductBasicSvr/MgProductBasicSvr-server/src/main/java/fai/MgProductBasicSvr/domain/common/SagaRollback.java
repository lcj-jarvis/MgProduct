package fai.MgProductBasicSvr.domain.common;

import fai.middleground.svrutil.repository.TransactionCtrl;

public interface SagaRollback {
    void rollback(TransactionCtrl tc);
}
