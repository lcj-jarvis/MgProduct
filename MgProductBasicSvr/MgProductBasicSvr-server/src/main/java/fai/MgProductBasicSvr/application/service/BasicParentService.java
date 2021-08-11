package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.ESUtil;
import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.common.SagaRollback;
import fai.MgProductBasicSvr.domain.entity.BasicSagaEntity;
import fai.MgProductBasicSvr.domain.entity.BasicSagaValObj;
import fai.MgProductBasicSvr.domain.repository.cache.CacheCtrl;
import fai.MgProductBasicSvr.domain.serviceproc.SagaProc;
import fai.app.DocOplogDef;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

public class BasicParentService extends ServicePub {
    /**
     * 新增商品业务关联-补偿
     */
    public int doRollback(int flow, int aid, String xid, long branchId, SagaRollback sagaRollback) {
        int branchStatus;

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                SagaProc sagaProc = new SagaProc(flow, aid, tc);
                Param sagaInfo = sagaProc.getInfoWithAdd(xid, branchId);
                if(sagaInfo == null) {
                    commit = true;
                    Log.logStd( "empty rollback! xid=%s;branchId=%s;", xid, branchId);
                    return BranchStatus.PhaseTwo_Rollbacked.getCode();
                }

                int status = sagaInfo.getInt(BasicSagaEntity.Info.STATUS);
                if(status == BasicSagaValObj.Status.ROLLBACK_OK) {
                    commit = true;
                    Log.logStd( "rollback already ok! saga=%s;", sagaInfo);
                    return BranchStatus.PhaseTwo_Rollbacked.getCode();
                }

                sagaRollback.rollback(tc);

                // 更新saga记录status
                sagaProc.setStatus(xid, branchId, BasicSagaValObj.Status.ROLLBACK_OK);

                commit = true;
            } finally {
                if(!commit) {
                    tc.rollback();
                    // 失败，需要重试
                    branchStatus = BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode();
                }else {
                    tc.commit();
                    // 成功，不需要重试
                    branchStatus = BranchStatus.PhaseTwo_Rollbacked.getCode();
                }
                tc.closeDao();
            }
            // 更新缓存
            CacheCtrl.clearCacheVersion(aid);
            Log.logStd("do rollback ok;aid=%d;xid=%s;branchId=%s;", aid, xid, branchId);
            // 同步数据到es
            ESUtil.commitPre(flow, aid);
        } finally {
            LockUtil.unlock(aid);
        }
        return branchStatus;
    }
}
