package fai.MgProductStoreSvr.domain.serviceProc;

import fai.MgProductStoreSvr.domain.entity.StoreSagaEntity;
import fai.MgProductStoreSvr.domain.entity.StoreSagaValObj;
import fai.MgProductStoreSvr.domain.repository.StoreSagaDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/12 14:34
 */
public class StoreSagaProc {

    public StoreSagaProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = StoreSagaDaoCtrl.getInstanceWithRegistered(flow, aid, tc);
    }

    /**
     * 添加补偿记录
     *
     * @param aid          aid
     * @param xid          全局事务id
     * @param branchId     分支事务id
     * @param prop         补偿操作
     * @return {@link Errno}
     */
    public int add(int aid, String xid, Long branchId, Param prop) {
        Calendar now = Calendar.getInstance();
        Param info = new Param();
        info.setInt(StoreSagaEntity.Info.AID, aid);
        info.setString(StoreSagaEntity.Info.XID, xid);
        info.setLong(StoreSagaEntity.Info.BRANCH_ID, branchId);
        info.setString(StoreSagaEntity.Info.PROP, prop.toJson());
        info.setInt(StoreSagaEntity.Info.STATUS, StoreSagaValObj.Status.INIT);
        info.setCalendar(StoreSagaEntity.Info.SYS_CREATE_TIME, now);
        info.setCalendar(StoreSagaEntity.Info.SYS_UPDATE_TIME, now);
        int rt = addInfo(info);
        if (rt != Errno.OK) {
            Log.logErr(rt, "storeSaga insert err;flow=%d;aid=%d;addInfo=%s", m_flow, aid, info);
        }
        return rt;
    }

    private int addInfo(Param addInfo) {
        if (Str.isEmpty(addInfo)) {
            Log.logErr("insert storeSaga error;addInfo is empty");
            return Errno.ARGS_ERROR;
        }
        return m_dao.insert(addInfo);
    }

    /**
     * 获取补偿信息
     *
     * @param xid      全局事务id
     * @param branchId 分支事务id
     * @return {@link Errno}
     */
    public int getInfoWithAdd(String xid, Long branchId, Ref<Param> infoRef) {
        int rt;
        if (Str.isEmpty(xid) || branchId == 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "arg err;xid or branchId is empty");
            return rt;
        }
        Param infoFromDB = getInfoFromDB(xid, branchId);
        if (!Str.isEmpty(infoFromDB)) {
            infoRef.value = infoFromDB;
            return Errno.OK;
        }
        rt = Errno.NOT_FOUND;

        // 如果找不到记录，则要添加一条空记录，允许空补偿以及防悬挂
        Log.reportErr(m_flow, Errno.NOT_FOUND, "get SagaInfo not found;xid=%s,branchId=%s", xid, branchId);
        Param info = new Param();
        info.setString(StoreSagaEntity.Info.XID, xid);
        info.setLong(StoreSagaEntity.Info.BRANCH_ID, branchId);
        info.setInt(StoreSagaEntity.Info.STATUS, StoreSagaValObj.Status.INIT);
        info.setInt(StoreSagaEntity.Info.AID, 0);
        info.setString(StoreSagaEntity.Info.PROP, new Param().toJson());
        int addRt = addInfo(info);
        if (addRt != Errno.OK) {
            return addRt;
        }

        return rt;
    }

    private Param getInfoFromDB(String xid, Long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(StoreSagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(StoreSagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
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
        ParamMatcher matcher = new ParamMatcher(StoreSagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(StoreSagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);
        ParamUpdater updater = new ParamUpdater(new Param().setInt(StoreSagaEntity.Info.STATUS, status));
        int rt = m_dao.update(updater, matcher);
        if (rt != Errno.OK) {
            Log.logErr(rt, "setStatus err;flow=%d;xid=%s;branchId=%d;", m_flow, xid, branchId);
            return rt;
        }
        return rt;
    }

    private int m_flow;

    private StoreSagaDaoCtrl m_dao;
}
