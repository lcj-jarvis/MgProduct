package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * 商品规格库存销售 spu 表 的 Saga 表
 * @author GYY
 * @version 1.0
 * @date 2021/8/3 10:35
 */
public class SpuBizSummarySagaDaoCtrl extends DaoCtrl {

    public SpuBizSummarySagaDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }

    public static SpuBizSummarySagaDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl tc) {
        if (tc == null) {
            return null;
        }
        if (m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        SpuBizSummarySagaDaoCtrl spuBizSummarySagaDaoCtrl = new SpuBizSummarySagaDaoCtrl(flow, aid);
        if (!tc.register(spuBizSummarySagaDaoCtrl)) {
            Log.logErr("register spuBizSummarySagaDaoCtrl err;flow=%d;aid=%d", flow, aid);
            return null;
        }
        return spuBizSummarySagaDaoCtrl;
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }

    @Override
    protected String getTableName() {
        return TABLE_ENUM.getTable() + "_" + String.format("%03d", aid % 100);
    }

    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_SPU_BIZ_SUMMARY_SAGA;
}
