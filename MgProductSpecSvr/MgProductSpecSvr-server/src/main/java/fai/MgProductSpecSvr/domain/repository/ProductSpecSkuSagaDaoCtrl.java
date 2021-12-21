package fai.MgProductSpecSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/8/11 14:07
 */
public class ProductSpecSkuSagaDaoCtrl extends DaoCtrl {

    public ProductSpecSkuSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public static ProductSpecSkuSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        ProductSpecSkuSagaDaoCtrl specSkuSagaDaoCtrl = getInstance(flow, aid);
        if (!tc.register(specSkuSagaDaoCtrl)) {
            Log.logErr("registered specSkuSagaDaoCtrl err;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        return specSkuSagaDaoCtrl;
    }

    private static ProductSpecSkuSagaDaoCtrl getInstance(int flow, int aid) {
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new ProductSpecSkuSagaDaoCtrl(flow, aid);
    }


    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_NAME + "_" + String.format("%03d", aid % 100);
    }

    private static final String TABLE_NAME = "mgProductSpecSkuSaga";
}
