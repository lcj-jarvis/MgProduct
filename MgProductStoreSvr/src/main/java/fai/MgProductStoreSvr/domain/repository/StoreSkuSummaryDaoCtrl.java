package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class StoreSkuSummaryDaoCtrl extends DaoCtrl {

    private StoreSkuSummaryDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.group = TABLE_ENUM.getGroup();
    }

    public static StoreSkuSummaryDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCrtl transactionCrtl) {
        if(transactionCrtl == null){
            return null;
        }
        StoreSkuSummaryDaoCtrl daoCtrl = getInstance(flow, aid);
        if(!transactionCrtl.registered(daoCtrl)){
            Log.logErr("registered StoreSkuSummaryDaoCtrl err;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        return daoCtrl;
    }


    public static StoreSkuSummaryDaoCtrl getInstance(int flow, int aid) {
        if(m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new StoreSkuSummaryDaoCtrl(flow, aid);
    }



    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(aid, getGroup());
    }
    @Override
    protected String getTableName(){
        return TABLE_ENUM.getTable()+ "_"+ String.format("%04d", aid%1000);
    }

    public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_STORE_SKU_SUMMARY;
}
