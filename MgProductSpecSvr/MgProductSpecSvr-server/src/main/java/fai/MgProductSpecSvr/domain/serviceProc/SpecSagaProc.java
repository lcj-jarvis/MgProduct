package fai.MgProductSpecSvr.domain.serviceProc;

import fai.MgProductSpecSvr.domain.repository.SpecSagaDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.entity.SagaEntity;
import fai.mgproduct.comm.entity.SagaValObj;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/26 18:07
 */
public class SpecSagaProc {

    public SpecSagaProc(int flow, int aid, TransactionCtrl transactionCtrl) {
        this.m_dao = SpecSagaDaoCtrl.getInstanceWithRegistered(flow, aid, transactionCtrl);
        if(m_dao == null){
            throw new RuntimeException(String.format("SpecSagaDaoCtrl init err;flow=%s;aid=%s;", flow, aid));
        }
        this.m_flow = flow;
    }

    public int add(int aid, String xid, Long branchId) {
        Calendar now = Calendar.getInstance();
        Param data = new Param();
        data.setInt(SagaEntity.Info.AID, aid);
        data.setString(SagaEntity.Info.XID, xid);
        data.setLong(SagaEntity.Info.BRANCH_ID, branchId);
        data.setInt(SagaEntity.Info.STATUS, SagaValObj.Status.INIT);
        data.setCalendar(SagaEntity.Info.SYS_CREATE_TIME, now);
        data.setCalendar(SagaEntity.Info.SYS_UPDATE_TIME, now);
        int rt = addInfo(data);
        if (rt != Errno.OK) {
            Log.logErr("specSaga dao.insert err;flow=%d;aid=%d;data=%s", m_flow, aid, data);
            return rt;
        }
        Log.logStd("specSaga dao.insert ok;flow=%d;aid=%d", m_flow, aid);
        return rt;
    }

    private int addInfo(Param data) {
        if (Str.isEmpty(data)) {
            Log.logErr("data is empty");
            return Errno.ARGS_ERROR;
        }
        return m_dao.insert(data);
    }

    /**
     * 获取补偿信息
     *
     * @param xid      全局事务id
     * @param branchId 分支事务id
     * @param sagaInfoRef 接收查询结果
     * @return {@link Errno}
     */
    public int getInfoWithAdd(String xid, Long branchId, Ref<Param> sagaInfoRef) {
        int rt;
        if (Str.isEmpty(xid) || branchId == 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;xid or branchId is empty");
            return rt;
        }
        Param infoFromDB = getInfoFromDB(xid, branchId);
        if (!Str.isEmpty(infoFromDB)) {
            sagaInfoRef.value = infoFromDB;
            return Errno.OK;
        }
        rt = Errno.NOT_FOUND;

        // 如果找不到记录，则要添加一条空记录，允许空补偿以及防悬挂
        Log.reportErr(m_flow, Errno.NOT_FOUND, "get SagaInfo not found;xid=%s,branchId=%s", xid, branchId);
        Param info = new Param();
        info.setString(SagaEntity.Info.XID, xid);
        info.setLong(SagaEntity.Info.BRANCH_ID, branchId);
        info.setInt(SagaEntity.Info.STATUS, SagaValObj.Status.INIT);
        info.setInt(SagaEntity.Info.AID, 0);
        int addRt = addInfo(info);
        if (addRt != Errno.OK) {
            return addRt;
        }

        return rt;
    }

    private Param getInfoFromDB(String xid, Long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
        Ref<Param> infoRef = new Ref<>();
        m_dao.selectFirst(searchArg, infoRef);
        return infoRef.value;
    }

    /**
     * 设置补偿信息状态
     *
     * @param xid      全局事务id
     * @param branchId 分支事务id
     * @param status   状态
     * @return {@link Errno}
     */
    public int setStatus(String xid, Long branchId, int status) {
        ParamMatcher matcher = new ParamMatcher(SagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(SagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
        ParamUpdater updater = new ParamUpdater(new Param().setInt(SagaEntity.Info.STATUS, status));
        int rt = m_dao.update(updater, matcher);
        if (rt != Errno.OK) {
            Log.logErr(rt, "setStatus err;flow=%d;xid=%s;branchId=%d;", m_flow, xid, branchId);
            return rt;
        }
        return rt;
    }

    private int m_flow;

    private SpecSagaDaoCtrl m_dao;

}
