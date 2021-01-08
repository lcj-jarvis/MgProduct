package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.*;

import java.util.Vector;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class BizSalesReportDaoCtrl extends DaoCtrl  {
    private BizSalesReportDaoCtrl(int flow, int aid) {
        super(flow, aid);
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

    @Override
    protected DaoProxy getDaoProxy() {
        return m_daoProxy;
    }
    @Override
    protected String getTableName(){
        return TABLE_NAME;
    }
    private static final String TABLE_NAME = "bizSalesReport";
}
