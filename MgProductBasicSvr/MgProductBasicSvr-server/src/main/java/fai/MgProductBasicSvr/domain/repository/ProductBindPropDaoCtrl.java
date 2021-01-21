package fai.MgProductBasicSvr.domain.repository;

import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.comm.middleground.repository.DaoCtrl;

public class ProductBindPropDaoCtrl extends DaoCtrl {
    private ProductBindPropDaoCtrl(FaiSession session) {
        super(session);
    }

    @Override
    public String getTableName(int aid) {
        return TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    public static ProductBindPropDaoCtrl getInstance(FaiSession session) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductBindPropDaoCtrl(session);
    }

    public static void init(DaoPool daoPool) {
        m_daoPool = daoPool;
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductBindProp";
}
