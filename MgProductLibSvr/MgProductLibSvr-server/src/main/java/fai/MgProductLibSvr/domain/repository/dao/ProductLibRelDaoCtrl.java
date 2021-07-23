package fai.MgProductLibSvr.domain.repository.dao;

import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.DaoPool;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:23
 */
public class ProductLibRelDaoCtrl extends DaoCtrl {

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductLibRel";
    private static IdBuilderWrapper m_idBuilder;
    private static final int ID_BUILDER_INIT = 1;
    private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
            .buildTableName(TABLE_PREFIX)
            .buildAssistTableSuffix("idBuilder")
            //测试的时候，不使用缓存的自增id，因为返回的自增id为一个aid下缓存的
            //.buildUseCache(false)
            .buildPrimaryMatchField(ProductLibRelEntity.Info.AID)
            .buildForeignMatchField(ProductLibRelEntity.Info.UNION_PRI_ID)
            .buildAutoIncField(ProductLibRelEntity.Info.RL_LIB_ID)
            .buildInitValue(ID_BUILDER_INIT)
            .build();
    private String tableName;

    /**
     * 对外暴露getInstance方法获取对象。
     */
    private ProductLibRelDaoCtrl(int flow, int aid) {
        super(flow, aid);
        tableName = TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    @Override
    protected String getTableName() {
        return tableName;
    }

    public static void init(DaoPool daoPool, RedisCacheManager cache) {
        m_daoPool = daoPool;
        m_idBuilder = new IdBuilderWrapper(idBuilderConfig, cache);
    }

    public static ProductLibRelDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductLibRelDaoCtrl(flow, aid);
    }

    public Integer buildId(int aid, int unionPriId, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.build(aid, unionPriId, m_dao, needLock);
    }

    public Integer updateId(int aid, int unionPriId, int id, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.update(aid, unionPriId, id, m_dao, needLock);
    }

    public void clearIdBuilderCache(int aid, int unionPriId) {
        m_idBuilder.clearCache(aid, unionPriId);
    }

    public void setTableName(int aid) {
        this.tableName = TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    public void restoreTableName() {
        this.tableName = TABLE_PREFIX + "_" + String.format("%04d", this.aid % 1000);
    }

    public int restoreMaxId(int unionPriId, boolean needLock) {
        return m_idBuilder.restoreMaxId(aid, unionPriId, flow, tableName, m_dao, needLock);
    }
}
