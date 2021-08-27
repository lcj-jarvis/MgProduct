package fai.MgProductBasicSvr.domain.repository.dao.saga;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductBindPropSagaDaoCtrl extends DaoCtrl {
    public ProductBindPropSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    @Override
    public String getTableName() {
        return TABLE_PREFIX + "_" + String.format("%02d", aid % 10);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    public static ProductBindPropSagaDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductBindPropSagaDaoCtrl(flow, aid);
    }

    public static void init(DaoPool daoPool) {
        m_daoPool = daoPool;
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductBindPropSaga";
}
