package fai.MgProductPropSvr.domain.repository.dao;

import fai.MgProductPropSvr.domain.entity.ProductPropRelEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductPropValDaoCtrl extends DaoCtrl {
	private String tableName;

	public ProductPropValDaoCtrl(int flow, int aid) {
		super(flow, aid);
		setTableName(aid);
	}

	@Override
	public String getTableName() {
		return this.tableName;
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

	public static ProductPropValDaoCtrl getInstance(int flow, int aid) {
		if(m_daoPool == null) {
			Log.logErr("m_daoPool is not init;");
			return null;
		}
		return new ProductPropValDaoCtrl(flow, aid);
	}

	public Integer buildId(int aid, boolean needLock) {
		int rt = openDao();
		if(rt != Errno.OK) {
			return null;
		}
		return m_idBuilder.build(aid, m_dao, needLock);
	}

	public void restoreMaxId(int aid, int flow, boolean needLock) {
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
				throw new MgException(rt, "IdBuilder clear err;flow=%d;aid=%d;", flow, aid);
			}
		} else {
			if (m_idBuilder.restore(aid, maxId, m_dao, needLock) == null) {
				rt = Errno.DAO_ERROR;
				throw new MgException(rt, "IdBuilder restore err;flow=%d;aid=%d;", flow, aid);
			}
		}
	}

	private Integer getMaxId(int aid) {
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
		int rt = select(searchArg, listRef, "max(" + ProductPropValEntity.Info.PROP_VAL_ID + ") as " + ProductPropValEntity.Info.PROP_VAL_ID);
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
			id = info.getInt(ProductPropValEntity.Info.PROP_VAL_ID, 0);
		}
		return id;
	}

	public static void clearIdBuilderCache(int aid) {
		m_idBuilder.clearCache(aid);
	}

	public static void init(DaoPool daoPool, RedisCacheManager cache) {
		m_daoPool = daoPool;
		m_idBuilder = new IdBuilderWrapper(idBuilderConfig, cache);
	}

	private static DaoPool m_daoPool;
	private static final String TABLE_PREFIX = "mgProductPropVal";
	private static IdBuilderWrapper m_idBuilder;
	private static final int ID_BUILDER_INIT = 1;
	private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
	.buildTableName("mgProductPropVal")
	.buildAssistTableSuffix("idBuilder")
	.buildInitValue(ID_BUILDER_INIT)
	.build();
}
