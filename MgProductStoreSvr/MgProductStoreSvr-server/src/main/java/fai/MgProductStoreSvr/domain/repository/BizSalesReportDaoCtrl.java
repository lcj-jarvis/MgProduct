package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.*;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class BizSalesReportDaoCtrl extends DaoCtrl {
    private BizSalesReportDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }
    public static BizSalesReportDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl transactionCtrl) {
        if(transactionCtrl == null){
            return null;
        }
        BizSalesReportDaoCtrl daoCtrl = getInstance(flow, aid);
        if(!transactionCtrl.register(daoCtrl)){
            Log.logErr("registered BizSalesReportDaoCtrl err;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        return daoCtrl;
    }
    public static BizSalesReportDaoCtrl getInstance(int flow, int aid) {
        if(m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new BizSalesReportDaoCtrl(flow, aid);
    }

    public int replace(Param data, ParamUpdater updater){
        int rt;
        if(Str.isEmpty(data) || updater == null || updater.isEmpty()) {
            rt = Errno.ERROR;
            Log.logErr("replace arg is empty;data=%s;updater=%s;", data, updater);
            return rt;
        }
        rt = openDao();
        if(rt != Errno.OK) {
            return rt;
        }

        rt = m_dao.replace(getTableName(), data, updater);
        return rt;
    }

    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }
    @Override
    protected String getTableName(){
        return TABLE_ENUM.getTable();
    }
    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_BIZ_SALES_REPORT;
}
