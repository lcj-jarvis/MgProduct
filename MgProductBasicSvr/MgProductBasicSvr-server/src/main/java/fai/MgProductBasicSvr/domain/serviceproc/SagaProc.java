package fai.MgProductBasicSvr.domain.serviceproc;


import fai.MgProductBasicSvr.domain.repository.dao.saga.SagaDaoCtrl;
import fai.comm.fseata.client.core.context.RootContext;
import fai.comm.util.*;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/6/28 16:25
 */
public class SagaProc {

    public SagaProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = SagaDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    /**
     * 添加Saga信息
     * @param aid 用户id
     * @param xid 全局事务id
     */
    public Param addInfo(int aid, String xid) {
        Calendar now = Calendar.getInstance();
        Param addInfo = new Param();
        addInfo.setInt(SagaEntity.Info.AID, aid);
        addInfo.setString(SagaEntity.Info.XID, xid);
        addInfo.setInt(SagaEntity.Info.STATUS, SagaValObj.Status.INIT);
        addInfo.setLong(SagaEntity.Info.BRANCH_ID, RootContext.getBranchId());
        addInfo.setCalendar(SagaEntity.Info.SYS_CREATE_TIME, now);
        addInfo.setCalendar(SagaEntity.Info.SYS_UPDATE_TIME, now);

        addInfo(addInfo);
        return addInfo;
    }

    private void addInfo(Param addInfo) {
        if (Str.isEmpty(addInfo)) {
            throw new MgException(Errno.ARGS_ERROR, "addInfo error;addInfo is empty;");
        }
        int rt = m_dao.insert(addInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "add Saga Info error;addInfo=%s", addInfo);
        }
        Log.logStd("add saga ok;info=%s;", addInfo);
    }

    /**
     * 获取补偿信息
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @return {@link Errno}
     */
    public Param getInfoWithAdd(String xid, Long branchId) {
        // 如果能查到信息则直接返回
        Param sagaInfo = getInfoFromDB(xid, branchId);
        if (!Str.isEmpty(sagaInfo)) {
            return sagaInfo;
        }

        // 查找不到，需要插入一条saga数据占位，允许空补偿和防悬挂
        Log.reportErr(m_flow, Errno.NOT_FOUND, "get SagaInfo not found;xid=%s,branchId=%s", xid, branchId);

        Calendar now = Calendar.getInstance();
        Param newInfo = new Param();
        newInfo.setString(SagaEntity.Info.XID, xid);
        newInfo.setLong(SagaEntity.Info.BRANCH_ID, branchId);
        newInfo.setInt(SagaEntity.Info.AID, 0);
        newInfo.setCalendar(SagaEntity.Info.SYS_CREATE_TIME, now);
        newInfo.setCalendar(SagaEntity.Info.SYS_UPDATE_TIME, now);
        // 空补偿直接是完成状态
        newInfo.setInt(SagaEntity.Info.STATUS, SagaValObj.Status.ROLLBACK_OK);

        // 添加记录
        addInfo(newInfo);

        return null;
    }

    private Param getInfoFromDB(String xid, Long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        Ref<FaiList<Param>> listRef = new Ref<>();
        int rt = m_dao.select(searchArg, listRef);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            throw new MgException(rt, "getInfoFromDB err;xid=%s;branchId=%s;", xid, branchId);
        }
        if(listRef.value == null || listRef.value.isEmpty()) {
            return null;
        }
        return listRef.value.get(0);
    }

    /**
     * 修改Saga状态
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param status 修改状态
     */
    public void setStatus(String xid, Long branchId, int status) {
        Param updateInfo = new Param();
        updateInfo.setInt(SagaEntity.Info.STATUS, status);
        updateInfo.setCalendar(SagaEntity.Info.SYS_UPDATE_TIME, Calendar.getInstance());
        ParamUpdater updater = new ParamUpdater(updateInfo);

        ParamMatcher matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        int rt = m_dao.update(updater, matcher);
        if (rt != Errno.OK) {
            throw new MgException(rt, "setStatus err; xid=%s, branchId=%s, status=%s", xid, branchId, status);
        }
    }

    private void init(TransactionCtrl tc) {
        if (tc == null) {
            return;
        }
        if (!tc.register(m_dao)) {
            throw new MgException("registered SagaDaoCtrl err;");
        }
    }

    private int m_flow;

    private SagaDaoCtrl m_dao;
}
