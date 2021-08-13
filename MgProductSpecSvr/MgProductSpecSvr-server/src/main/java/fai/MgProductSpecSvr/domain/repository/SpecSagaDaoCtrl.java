package fai.MgProductSpecSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/26 18:08
 */
public class SpecSagaDaoCtrl extends DaoCtrl {

    public SpecSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public static SpecSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        SpecSagaDaoCtrl sagaDaoCtrl = getInstance(flow, aid);
        if (!tc.register(sagaDaoCtrl)) {
            Log.logErr("registered SpecSagaDaoCtrl err;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        return sagaDaoCtrl;
    }

    private static SpecSagaDaoCtrl getInstance(int flow, int aid) {
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new SpecSagaDaoCtrl(flow, aid);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_NAME;
    }

    private static final String TABLE_NAME = "mgSpecSaga";
}
