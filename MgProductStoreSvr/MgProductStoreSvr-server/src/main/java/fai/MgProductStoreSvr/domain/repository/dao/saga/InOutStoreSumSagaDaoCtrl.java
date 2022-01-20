package fai.MgProductStoreSvr.domain.repository.dao.saga;

import fai.MgProductStoreSvr.domain.repository.TableDBMapping;
import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * 出入库汇总表的 Saga DaoCtrl
 * @author GYY
 * @version 1.0
 * @date 2021/8/3 9:36
 */
public class InOutStoreSumSagaDaoCtrl  extends DaoCtrl {

    public InOutStoreSumSagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }

    public static InOutStoreSumSagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        InOutStoreSumSagaDaoCtrl inOutStoreSumSagaDaoCtrl = new InOutStoreSumSagaDaoCtrl(flow, aid);
        if (!tc.register(inOutStoreSumSagaDaoCtrl)) {
            Log.logErr("register inOutStoreSumSagaDaoCtrl err;flow=%d;aid=%d", flow, aid);
            return null;
        }
        return inOutStoreSumSagaDaoCtrl;
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_ENUM.getTable() + "_" + String.format("%03d", aid % 100);
    }

    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_IN_OUT_STORE_SUM_SAGA;
}
