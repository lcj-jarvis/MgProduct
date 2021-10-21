package fai.MgProductBasicSvr.domain.repository.dao;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductBindTagDaoCtrl extends DaoCtrl {
    private String tableName;

    public static ProductBindTagDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductBindTagDaoCtrl(flow, aid);
    }

    public ProductBindTagDaoCtrl(int flow, int aid) {
        super(flow, aid);
        setTableName(aid);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void setTableName(int aid) {
        this.tableName = TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    public void restoreTableName() {
        setTableName(this.aid);
    }

    public static void init(DaoPool daoPool) {
        m_daoPool = daoPool;
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductBindTag";

}
