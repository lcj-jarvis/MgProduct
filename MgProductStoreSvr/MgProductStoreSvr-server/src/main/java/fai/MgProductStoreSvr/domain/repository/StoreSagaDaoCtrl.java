package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GYY
 * @version 1.0
 * @date 2021/7/12 14:00
 */
public class StoreSagaDaoCtrl extends DaoCtrl {

    private StoreSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }

    public static StoreSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        StoreSagaDaoCtrl sagaDaoCtrl = new StoreSagaDaoCtrl(flow, aid);
        if (!tc.register(sagaDaoCtrl)) {
            Log.logErr("register sagaDaoCtrl err;flow=%d;aid=%d", flow, aid);
            return null;
        }
        return sagaDaoCtrl;
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_ENUM.getTable();
    }

    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_STORE_SAGA;
}
