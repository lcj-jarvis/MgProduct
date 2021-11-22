package fai.MgProductStoreSvr.application.service;

import fai.MgProductStoreSvr.domain.comm.LockUtil;
import fai.MgProductStoreSvr.domain.comm.SagaRollback;
import fai.MgProductStoreSvr.domain.repository.cache.CacheCtrl;
import fai.MgProductStoreSvr.domain.serviceProc.StoreSagaProc;
import fai.comm.fseata.client.core.model.BranchStatus;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Ref;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/9/10 14:31
 */
public class StoreParentService extends ServicePub {

    public int doRollback(int flow, int aid, String xid, Long branchId, SagaRollback sagaRollback) {
        int branchStatus;
        LockUtil.lock(aid);
        try {
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                StoreSagaProc sagaProc = new StoreSagaProc(flow, aid, tc);
                Ref<Param> sagaInfoRef = new Ref<>();
                int rt = sagaProc.getInfoWithAdd(xid, branchId, sagaInfoRef);
                if (rt != Errno.OK) {
                    // 如果 rt = NOT_FOUND，说明出现空补偿或悬挂现象，插入 saga 记录占位后 return OK 告知分布式事务组件回滚成功
                    if (rt == Errno.NOT_FOUND) {
                        commit = true;
                        Log.logStd( "empty rollback! xid=%s;branchId=%s;", xid, branchId);
                        return BranchStatus.PhaseTwo_Rollbacked.getCode();
                    }
                    return BranchStatus.PhaseTwo_RollbackFailed_Retryable.getCode();
                }

                Param sagaInfo = sagaInfoRef.value;
                Integer status = sagaInfo.getInt(SagaEntity.Info.STATUS);
                // 幂等性保证
                if (status == SagaValObj.Status.ROLLBACK_OK) {
                    commit = true;
                    Log.logStd(rt, "rollback already ok! saga=%s;", sagaInfo);
                    return BranchStatus.PhaseTwo_Rollbacked.getCode();
                }

                // 回滚操作
                sagaRollback.rollback(tc);

                // 修改状态
                rt = sagaProc.setStatus(xid, branchId, SagaValObj.Status.ROLLBACK_OK);
                if (rt != Errno.OK) {
                    throw new MgException(rt, "setStatus err; xid=%s, branchId=%s", xid, branchId);
                }
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
            // 清除缓存
            CacheCtrl.clearCacheVersion(aid);
            Log.logStd("do rollback ok;aid=%d;xid=%s;branchId=%s;", aid, xid, branchId);
        } finally {
            // 更新缓存
            LockUtil.unlock(aid);
        }
        return branchStatus;
    }
}
