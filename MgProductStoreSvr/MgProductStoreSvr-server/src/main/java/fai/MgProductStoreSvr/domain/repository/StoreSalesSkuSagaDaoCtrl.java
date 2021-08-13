package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * 库存销售 SKU 表的 Saga DaoCtrl
 * @author GYY
 * @version 1.0
 * @date 2021/8/2 17:30
 */
public class StoreSalesSkuSagaDaoCtrl extends DaoCtrl {

    private StoreSalesSkuSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }

    public static StoreSalesSkuSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        StoreSalesSkuSagaDaoCtrl storeSalesSkuSagaDaoCtrl = new StoreSalesSkuSagaDaoCtrl(flow, aid);
        if (!tc.register(storeSalesSkuSagaDaoCtrl)) {
            Log.logErr("register storeSalesSkuSagaDaoCtrl err;flow=%d;aid=%d", flow, aid);
            return null;
        }
        return storeSalesSkuSagaDaoCtrl;
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_ENUM.getTable() + "_" + String.format("%02d", aid % 10);
    }

    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_STORE_SALE_SKU_SAGA;
}
