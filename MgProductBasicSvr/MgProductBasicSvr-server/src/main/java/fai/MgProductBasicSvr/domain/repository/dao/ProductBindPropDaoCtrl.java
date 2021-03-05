package fai.MgProductBasicSvr.domain.repository.dao;

import fai.comm.util.Dao;
import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductBindPropDaoCtrl extends DaoCtrl {

    public ProductBindPropDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public ProductBindPropDaoCtrl(int flow, int aid, Dao dao) {
        super(flow, aid, dao);
    }

    @Override
    public String getTableName() {
        return TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    public static ProductBindPropDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductBindPropDaoCtrl(flow, aid);
    }

    public static void init(DaoPool daoPool) {
        m_daoPool = daoPool;
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductBindProp";
}
