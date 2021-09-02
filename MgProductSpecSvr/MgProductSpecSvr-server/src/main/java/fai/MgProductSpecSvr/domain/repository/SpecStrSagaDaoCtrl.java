package fai.MgProductSpecSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/8/12 10:00
 */
public class SpecStrSagaDaoCtrl extends DaoCtrl {

    public SpecStrSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public static SpecStrSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        SpecStrSagaDaoCtrl specStrSagaDaoCtrl = getInstance(flow, aid);
        if (!tc.register(specStrSagaDaoCtrl)) {
            Log.logErr("registered specStrSagaDaoCtrl err;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        return specStrSagaDaoCtrl;
    }

    private static SpecStrSagaDaoCtrl getInstance(int flow, int aid) {
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new SpecStrSagaDaoCtrl(flow, aid);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_NAME + "_" + String.format("%02d", aid % 10);
    }

    private static final String TABLE_NAME = "mgSpecStrSaga";
}
