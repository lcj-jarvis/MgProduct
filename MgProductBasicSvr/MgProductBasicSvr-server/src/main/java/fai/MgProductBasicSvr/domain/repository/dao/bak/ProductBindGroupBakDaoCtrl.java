package fai.MgProductBasicSvr.domain.repository.dao.bak;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductBindGroupBakDaoCtrl extends DaoCtrl {
    private String tableName;

    public ProductBindGroupBakDaoCtrl(int flow, int aid) {
        super(flow, aid);
        this.tableName = TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    @Override
    protected String getTableName() {
        return tableName;
    }

    public static ProductBindGroupBakDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductBindGroupBakDaoCtrl(flow, aid);
    }

    public static void init(DaoPool daoPool) {
        m_daoPool = daoPool;
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductBindGroup_bak";
}
