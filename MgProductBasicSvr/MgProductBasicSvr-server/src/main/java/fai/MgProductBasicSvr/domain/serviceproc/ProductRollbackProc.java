package fai.MgProductBasicSvr.domain.serviceproc;


import fai.MgProductBasicSvr.domain.entity.ProductSagaEntity;
import fai.MgProductBasicSvr.domain.entity.ProductSagaValObj;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRollbackDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/6/28 16:25
 */
public class ProductRollbackProc {

    public ProductRollbackProc(int flow, int aid, TransactionCtrl tc) {
        this.m_flow = flow;
        this.m_dao = ProductRollbackDaoCtrl.getInstance(flow, aid);
        init(tc);
    }

    /**
     * 添加Saga信息
     * @param aid 用户id
     * @param xid 全局事务id
     * @param rollbackInfo 补偿信息记录
     */
    public void addInfo(int aid, String xid, Param rollbackInfo) {
        Param addInfo = new Param();
        addInfo.setInt(ProductSagaEntity.Info.AID, aid);
        addInfo.setString(ProductSagaEntity.Info.XID, xid);
        addInfo.setInt(ProductSagaEntity.Info.STATUS, ProductSagaValObj.Status.INIT);
        addInfo.setString(ProductSagaEntity.Info.ROLLBACK_INFO, rollbackInfo.toJson());
        addInfo.setLong(ProductSagaEntity.Info.BRANCH_ID, 1L);
        addInfo.setCalendar(ProductSagaEntity.Info.CREATE_TIME, Calendar.getInstance());
        addInfo.setCalendar(ProductSagaEntity.Info.UPDATE_TIME, Calendar.getInstance());

        addInfo(addInfo);
    }

    private void addInfo(Param addInfo) {
        if (Str.isEmpty(addInfo)) {
            throw new MgException(Errno.ARGS_ERROR, "addInfo error;addInfo is empty;");
        }
        int rt = m_dao.insert(addInfo);
        if (rt != Errno.OK) {
            throw new MgException(rt, "add Saga Info error;addInfo=%s", addInfo);
        }
    }

    /**
     * 获取补偿信息
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param sagaInfoRef 接收补偿信息
     * @return {@link Errno}
     */
    public int getInfo(String xid, Long branchId, Ref<Param> sagaInfoRef) {
        int rt;
        // 如果能查到信息则直接返回
        Param sagaInfo = getInfo(xid, branchId);
        if (!Str.isEmpty(sagaInfo)) {
            rt = Errno.OK;
            sagaInfoRef.value = sagaInfo;
            return rt;
        }

        // 查找不到，需要插入一条saga数据占位，允许空补偿和防悬挂
        rt = Errno.NOT_FOUND;
        Log.reportErr(m_flow, rt, "get SagaInfo not found;xid=%s,branchId=%s", xid, branchId);

        Param newInfo = new Param();
        newInfo.setString(ProductSagaEntity.Info.XID, xid);
        newInfo.setLong(ProductSagaEntity.Info.BRANCH_ID, branchId);
        newInfo.setInt(ProductSagaEntity.Info.AID, 0);
        newInfo.setString(ProductSagaEntity.Info.ROLLBACK_INFO, new Param().toJson());
        newInfo.setInt(ProductSagaEntity.Info.STATUS, ProductSagaValObj.Status.INIT);

        // 添加记录
        addInfo(newInfo);

        return rt;
    }

    private Param getInfo(String xid, Long branchId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductSagaEntity.Info.XID, ParamMatcher.EQ, xid);
        searchArg.matcher.and(ProductSagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        Ref<FaiList<Param>> listRef = new Ref<>();
        m_dao.select(searchArg, listRef);
        return listRef.value == null ? null : listRef.value.get(0);
    }

    /**
     * 修改Saga状态
     * @param xid 全局事务id
     * @param branchId 分支事务id
     * @param status 修改状态
     * @return {@link Errno}
     */
    public int setStatus(String xid, Long branchId, int status) {
        int rt;

        if (status != ProductSagaValObj.Status.ROLLBACK_OK) {
            rt = Errno.ERROR;
            Log.reportErr(m_flow, rt, "status err; xid=%s, branchId=%s, status=%s", xid, branchId, status);
            return rt;
        }

        Param updateInfo = new Param();
        updateInfo.setInt(ProductSagaEntity.Info.STATUS, status);
        updateInfo.setCalendar(ProductSagaEntity.Info.UPDATE_TIME, Calendar.getInstance());
        ParamUpdater updater = new ParamUpdater(updateInfo);

        ParamMatcher matcher = new ParamMatcher(ProductSagaEntity.Info.XID, ParamMatcher.EQ, xid);
        matcher.and(ProductSagaEntity.Info.BRANCH_ID, ParamMatcher.EQ, branchId);

        rt = m_dao.update(updater, matcher);
        if (rt != Errno.OK) {
            Log.reportErr(m_flow, rt, "setInfo err; xid=%s, branchId=%s, status=%s", xid, branchId, status);
            return rt;
        }
        return rt;
    }

    private void init(TransactionCtrl tc) {
        if (tc == null) {
            return;
        }
        if (!tc.register(m_dao)) {
            throw new MgException("registered ProductBindPropDaoCtrl err;");
        }
    }

    private int m_flow;

    private ProductRollbackDaoCtrl m_dao;
}
