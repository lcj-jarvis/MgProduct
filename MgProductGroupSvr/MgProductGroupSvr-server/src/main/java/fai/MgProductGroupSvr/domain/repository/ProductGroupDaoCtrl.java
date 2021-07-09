package fai.MgProductGroupSvr.domain.repository;

import fai.MgProductGroupSvr.domain.entity.ProductGroupEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.DaoPool;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductGroupDaoCtrl extends DaoCtrl {
    private String tableName;

    public ProductGroupDaoCtrl(int flow, int aid) {
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

    public void setTableName(int aid) {
        this.tableName = TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    public void restoreTableName() {
        this.tableName = TABLE_PREFIX + "_" + String.format("%04d", this.aid % 1000);
    }

    public static ProductGroupDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductGroupDaoCtrl(flow, aid);
    }

    public Integer buildId(int aid, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.build(aid, m_dao, needLock);
    }

    public Integer updateId(int aid, int id, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.update(aid, id, m_dao, needLock);
    }

    public int restoreMaxId(boolean needLock) {
        return m_idBuilder.restoreMaxId(aid, flow, tableName, m_dao, needLock);
    }

    public void clearIdBuilderCache(int aid) {
        m_idBuilder.clearCache(aid);
    }

    public static void init(DaoPool daoPool, RedisCacheManager cache) {
        m_daoPool = daoPool;
        m_idBuilder = new IdBuilderWrapper(idBuilderConfig, cache);
    }

    private static final String TABLE_PREFIX = "mgProductGroup";
    private static DaoPool m_daoPool;
    private static IdBuilderWrapper m_idBuilder;
    private static final int ID_BUILDER_INIT = 1;
    private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
            .buildTableName(TABLE_PREFIX)
            .buildAssistTableSuffix("idBuilder")
            .buildInitValue(ID_BUILDER_INIT)
            .buildPrimaryMatchField(ProductGroupEntity.Info.AID)
            .buildAutoIncField(ProductGroupEntity.Info.GROUP_ID)
            .build();
}
