package fai.MgProductBasicSvr.domain.repository.dao;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
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

    public Integer restoreMaxId(int aid, boolean needLock) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        Dao.SelectArg sltArg = new Dao.SelectArg();
        sltArg.table = getTableName();
        sltArg.searchArg = searchArg;
        sltArg.field = "max(" + ProductEntity.Info.PD_ID + ") as maxId";
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        Param maxInfo = m_dao.selectFirst(sltArg);
        if(Str.isEmpty(maxInfo)){
            rt = Errno.DAO_ERROR;
            Log.logErr(rt, "select db err;");
            return null;
        }
        int maxId = maxInfo.getInt("maxId");
        return updateId(aid, maxId, needLock);
    }

    public Integer getId(int aid) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.get(aid, m_dao);
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
