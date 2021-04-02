package fai.MgProductSpecSvr.domain.repository;

import fai.comm.util.Dao;
import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductSpecSkuNumDaoCtrl  extends DaoCtrl {
    public ProductSpecSkuNumDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public static ProductSpecSkuNumDaoCtrl getInstance(int flow, int aid) {
        if(m_daoProxy == null) {
            Log.logErr("m_daoProxy is not init;");
            return null;
        }
        return new ProductSpecSkuNumDaoCtrl(flow, aid);
    }

    public Dao getDao(){
        super.openDao();
        Dao dao = super.getDao();
        return dao;
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoProxy.getDaoPool(getAid(), getGroup());
    }

    @Override
    public String getTableName(){ // TODO
        return TABLE_NAME + "_"+ String.format("%04d", aid%1000);
    }
    private static final String TABLE_NAME = "mgProductSpecSkuNum";
}
