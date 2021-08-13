package fai.MgProductSpecSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/8/11 13:59
 */
public class ProductSpecSagaDaoCtrl extends DaoCtrl {

    public ProductSpecSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public static ProductSpecSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        ProductSpecSagaDaoCtrl productSpecSagaDaoCtrl = getInstance(flow, aid);
        if (!tc.register(productSpecSagaDaoCtrl)) {
            Log.logErr("registered ProductSpecSagaDaoCtrl err;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        return productSpecSagaDaoCtrl;
    }

    private static ProductSpecSagaDaoCtrl getInstance(int flow, int aid) {
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new ProductSpecSagaDaoCtrl(flow, aid);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_NAME + "_" + String.format("%02d", aid % 10);
    }

    private static final String TABLE_NAME = "mgProductSpecSaga";
}
