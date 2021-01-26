package fai.MgProductBasicSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.Dao;
import fai.comm.util.DaoPool;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductDaoCtrl extends DaoCtrl {

    public ProductDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public ProductDaoCtrl(int flow, int aid, Dao dao) {
        super(flow, aid, dao);
    }

    @Override
    public String getTableName() {
        return TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    @Override
    protected DaoPool getDaoPool() {
        return m_daoPool;
    }

    public static ProductDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductDaoCtrl(flow, aid);
    }

    public Integer buildId(int aid, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.build(aid, m_dao, needLock);
    }

    public static void clearIdBuilderCache(int aid) {
        m_idBuilder.clearCache(aid);
    }

    public Integer updateId(int aid, int id, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.update(aid, id, m_dao, needLock);
    }

    public static void init(DaoPool daoPool, RedisCacheManager cache) {
        m_daoPool = daoPool;
        m_idBuilder = new IdBuilderWrapper(idBuilderConfig, cache);
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProduct";
    private static IdBuilderWrapper m_idBuilder;
    private static final int ID_BUILDER_INIT = 1;
    private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()	// 渠道统计的stat
            .buildTableName("mgProduct")
            .buildAssistTableSuffix("idBuilder")
            .buildPrimaryMatchField("aid")
            .buildInitValue(ID_BUILDER_INIT)
            .build();
}
