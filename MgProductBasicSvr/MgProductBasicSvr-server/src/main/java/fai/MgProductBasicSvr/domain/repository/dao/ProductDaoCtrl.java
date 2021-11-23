package fai.MgProductBasicSvr.domain.repository.dao;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductDaoCtrl extends DaoCtrl {
    private String tableName;

    public ProductDaoCtrl(int flow, int aid) {
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

    public static ProductDaoCtrl getInstance(int flow, int aid) {
        if(m_daoPool == null) {
            Log.logErr("m_daoPool is not init;");
            return null;
        }
        return new ProductDaoCtrl(flow, aid);
    }

    public void restoreMaxId(int aid, boolean needLock) {
        int rt = openDao();
        if(rt != Errno.OK) {
            throw new MgException(rt, "openDao err;flow=%d;aid=%d;", flow, aid);
        }
        Integer maxId = getMaxId(aid);
        if(maxId == null) {
            rt = Errno.ERROR;
            throw new MgException(rt, "select maxId err;flow=%d;aid=%d;", flow, aid);
        }
        // 最大值小于初始值
        if (maxId < ID_BUILDER_INIT) {
            rt = m_idBuilder.clear(aid, m_dao, needLock);
            if (rt != Errno.OK) {
                throw new MgException(rt, "IdBuilder clear err;flow=%d, aid=%d;", flow, aid);
            }
        } else {
            if (m_idBuilder.restore(aid, maxId, m_dao, needLock) == null) {
                rt = Errno.DAO_ERROR;
                throw new MgException(rt, "IdBuilder restore err;flow=%d, aid=%d;", flow, aid);
            }
        }
    }

    private Integer getMaxId(int aid) {
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = new ParamMatcher(ProductEntity.Info.AID, ParamMatcher.EQ, aid);
        Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
        int rt = select(searchArg, listRef, "max(" + ProductEntity.Info.PD_ID + ") as " + ProductEntity.Info.PD_ID);
        if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
            return null;
        }
        if (listRef.value == null || listRef.value.isEmpty()) {
            rt = Errno.NOT_FOUND;
            Log.logErr(rt, "select maxId err;flow=%d;aid=%d;", flow, aid);
            return null;
        }

        Param info = listRef.value.get(0);
        if (info == null) {
            Log.logErr(rt, "select maxId err;flow=%d;aid=%d;", flow, aid);
            return null;
        }
        Integer id;
        if (info.isEmpty()) {
            id = 0;
        } else {
            id = info.getInt(ProductEntity.Info.PD_ID, 0);
        }
        return id;
    }

    private String getRelTableName(int aid) {
        return  "mgProductRel_" + String.format("%04d", aid%1000);
    }

    public void updateYkPdId(int aid) {
        String sql = "select aid,max(rlPdId) from " + getRelTableName(aid) + " where aid = " + aid;
        FaiList<Param> list = m_dao.executeQuery(sql);
        if(list == null) {
            throw new MgException(Errno.ERROR, "select maxId error;sql: %s;", sql);
        }
        if(list.isEmpty()) {
            return;
        }

        Param relInfo = list.get(0);
        int maxRlPdId = relInfo.getInt("max(rlPdId)");
        sql = "select id from mgProduct_idBuilder where aid = " + aid;
        FaiList<Param> tmpList = m_dao.executeQuery(sql);
        if(tmpList == null || tmpList.isEmpty()) {
            throw new MgException(Errno.ERROR, "select idBuilder error;sql: %s;", sql);
        }
        int pdId = tmpList.get(0).getInt("id");
        if(maxRlPdId > pdId) {
            sql = "update mgProduct_idBuilder set id = " + maxRlPdId + " where aid = " + aid;
            System.out.println("update sql: " + sql);
            int rt = m_dao.executeUpdate(sql);
            if(rt != Errno.OK) {
                throw new MgException(Errno.ERROR, "update maxId error;sql: %s;", sql);
            }
            clearIdBuilderCache(aid);
        }
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
    private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
            .buildTableName("mgProduct")
            .buildAssistTableSuffix("idBuilder")
            .buildPrimaryMatchField("aid")
            .buildInitValue(ID_BUILDER_INIT)
            .build();
}
