package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.Log;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class SalesSummaryDaoCtrl extends DaoCtrl {

    private SalesSummaryDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public static SalesSummaryDaoCtrl getInstance(int flow, int aid) {
        if(m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new SalesSummaryDaoCtrl(flow, aid);
    }



    @Override
    protected DaoProxy getDaoProxy() {
        return m_daoProxy;
    }
    @Override
    protected String getTableName(){
        return TABLE_NAME+ "_"+ String.format("%04d", aid%1000);
    }
    private static final String TABLE_NAME = "salesSummary";


}
