package fai.MgProductBasicSvr.domain.repository.dao;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductRelDaoCtrl extends DaoCtrl {

    public ProductRelDaoCtrl(int flow, int aid) {
        super(flow, aid);
    }

    public ProductRelDaoCtrl(int flow, int aid, Dao dao) {
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

    public static ProductRelDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductRelDaoCtrl(flow, aid);
    }

    public Integer buildId(int aid, int unionPriId, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.build(aid, unionPriId, m_dao, needLock);
    }

    public static void clearIdBuilderCache(int aid, int unionPriId) {
        m_idBuilder.clearCache(aid, unionPriId);
    }

    public Integer updateId(int aid, int unionPriId, int id, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.update(aid, unionPriId, id, m_dao, needLock);
    }

    public Integer restoreMaxId(int aid, int unionPriId, boolean needLock) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Dao.SelectArg sltArg = new Dao.SelectArg();
        sltArg.table = getTableName();
        sltArg.searchArg = searchArg;
        sltArg.field = "max(" + ProductRelEntity.Info.RL_PD_ID + ") as maxId";
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
        return updateId(aid, unionPriId, maxId, needLock);
    }

    public Integer getId(int aid, int unionPriId) {
        int rt = openDao();
        if(rt != Errno.OK) {
            return null;
        }
        return m_idBuilder.get(aid, unionPriId, m_dao);
    }

    public static void init(DaoPool daoPool, RedisCacheManager cache) {
        m_daoPool = daoPool;
        m_idBuilder = new IdBuilderWrapper(idBuilderConfig, cache);
    }

    private static DaoPool m_daoPool;
    private static final String TABLE_PREFIX = "mgProductRel";
    private static IdBuilderWrapper m_idBuilder;
    private static final int ID_BUILDER_INIT = 1;
    private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
            .buildTableName("mgProductRel")
            .buildAssistTableSuffix("idBuilder")
            .buildPrimaryMatchField("aid")
            .buildForeignMatchField("unionPriId")
            .buildInitValue(ID_BUILDER_INIT)
            .build();
}
