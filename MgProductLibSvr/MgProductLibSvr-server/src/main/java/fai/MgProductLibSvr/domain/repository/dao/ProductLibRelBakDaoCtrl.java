package fai.MgProductLibSvr.domain.repository.dao;

import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

/**
 * @author LuChaoJi
 * @date 2021-07-28 14:23
 */
public class ProductLibRelBakDaoCtrl extends DaoCtrl {
    private String tableName;

    public ProductLibRelBakDaoCtrl(int flow, int aid) {
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

    public static ProductLibRelBakDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductLibRelBakDaoCtrl(flow, aid);
    }

    public static void init(DaoPool daoPool) {
        m_daoPool = daoPool;
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductLibRel_bak";
}
