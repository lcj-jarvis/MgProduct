package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * 出入库记录表的 Saga DaoCtrl
 * @author GYY
 * @version 1.0
 * @date 2021/8/2 19:25
 */
public class InOutStoreRecordSagaDaoCtrl extends DaoCtrl {

    public InOutStoreRecordSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }

    public static InOutStoreRecordSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        InOutStoreRecordSagaDaoCtrl inOutStoreRecordSagaDaoCtrl = new InOutStoreRecordSagaDaoCtrl(flow, aid);
        if (!tc.register(inOutStoreRecordSagaDaoCtrl)) {
            Log.logErr("register inOutStoreRecordSagaDaoCtrl err;flow=%d;aid=%d", flow, aid);
            return null;
        }
        return inOutStoreRecordSagaDaoCtrl;
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_ENUM.getTable() + "_" + String.format("%02d", aid % 10);
    }

    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_IN_OUT_STORE_RECORD_SAGA;
}