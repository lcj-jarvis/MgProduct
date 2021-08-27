package fai.MgProductBasicSvr.domain.repository.dao.saga;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/6/28 16:26
 */
public class SagaDaoCtrl extends DaoCtrl {

    private SagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public static void init(DaoPool daoPool) {
        m_daoPool = daoPool;
    }

    public static SagaDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new SagaDaoCtrl(flow, aid);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_NAME = "mgPdBasicSaga";
}
