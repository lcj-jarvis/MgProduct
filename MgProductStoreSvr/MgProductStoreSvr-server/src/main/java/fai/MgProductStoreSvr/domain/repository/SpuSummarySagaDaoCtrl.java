package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author GuoYuYuan
 * @version 1.0
 * @date 2021/8/4 9:53
 */
public class SpuSummarySagaDaoCtrl extends DaoCtrl {

    public SpuSummarySagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }

    public static SpuSummarySagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        SpuSummarySagaDaoCtrl spuSummarySagaDaoCtrl = new SpuSummarySagaDaoCtrl(flow, aid);
        if (!tc.register(spuSummarySagaDaoCtrl)) {
            Log.logErr("register spuSummarySagaDaoCtrl err;flow=%d;aid=%d", flow, aid);
            return null;
        }
        return spuSummarySagaDaoCtrl;
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_ENUM.getTable() + "_" + String.format("%03d", aid % 100);
    }

    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_SPU_SUMMARY_SAGA;
}
