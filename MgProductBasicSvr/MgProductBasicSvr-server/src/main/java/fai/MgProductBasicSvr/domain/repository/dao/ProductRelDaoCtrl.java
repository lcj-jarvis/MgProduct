package fai.MgProductBasicSvr.domain.repository.dao;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductRelDaoCtrl extends DaoCtrl {
    private String tableName;

    public ProductRelDaoCtrl(int flow, int aid) {
        super(flow, aid);
        setTableName(aid);
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void setTableName(int aid) {
        this.tableName = TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
    }

    public void restoreTableName() {
        setTableName(this.aid);
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

    public void restoreMaxId(int aid, int unionPriId, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            throw new MgException(rt, "openDao err;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }
        Integer maxId = getMaxId(aid, unionPriId);
        if(maxId == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "select maxId err;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }
        // 最大值小于初始值
        if (maxId < ID_BUILDER_INIT) {
            rt = m_idBuilder.clear(aid, unionPriId, m_dao, needLock);
            if (rt != Errno.OK) {
                throw new MgException(rt, "IdBuilder clear err;flow=%d, aid=%d, unionPriId=%s", flow, aid, unionPriId);
            }
        } else {
            if (m_idBuilder.restore(aid, unionPriId, maxId, m_dao, needLock) == null) {
                rt = Errno.DAO_ERROR;
                throw new MgException(rt, "IdBuilder restore err;flow=%d, aid=%d, unionPriId=%s", flow, aid, unionPriId);
            }
        }
    }

    private Integer getMaxId(int aid, int unionPriId) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
        searchArg.matcher.and(ProductRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = select(searchArg, listRef, "max(" + ProductRelEntity.Info.RL_PD_ID + ") as " + ProductRelEntity.Info.RL_PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return null;
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logErr(rt, "select maxId err;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return null;
        }

        Param info = listRef.value.get(0);
        if (info == null) {
            Log.logErr(rt, "select maxId err;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return null;
        }
        Integer id;
        if (info.isEmpty()) {
            id = 0;
        } else {
            id = info.getInt(ProductRelEntity.Info.RL_PD_ID, 0);
        }
        return id;
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
