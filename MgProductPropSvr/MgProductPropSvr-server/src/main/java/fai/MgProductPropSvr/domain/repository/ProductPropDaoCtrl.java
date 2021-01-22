package fai.MgProductPropSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.DaoCtrl;

public class ProductPropDaoCtrl extends DaoCtrl {

	public ProductPropDaoCtrl(int flow, int aid) {
		super(flow, aid);
	}

	public ProductPropDaoCtrl(int flow, int aid, Dao dao) {
		super(flow, aid, dao);
	}

	@Override
	public String getTableName() {
		return TABLE_PREFIX + "_" + String.format("%04d", getAid() % 1000);
	}

	@Override
	protected DaoPool getDaoPool() {
		return m_daoPool;
	}

	public static ProductPropDaoCtrl getInstance(int flow, int aid) {
		if(m_daoPool == null) {
			Log.logErr("m_daoPool is not init;");
			return null;
		}
		return new ProductPropDaoCtrl(flow, aid);
	}

	public Integer buildId(int aid, boolean needLock) {
		int rt = openDao();
		if(rt != Errno.OK) {
			return null;
		}
		return m_idBuilder.build(aid, m_dao, needLock);
	}

	public void clearIdBuilderCache(int aid) {
		m_idBuilder.clearCache(aid);
	}

	public static void init(DaoPool daoPool, RedisCacheManager cache) {
		m_daoPool = daoPool;
		m_idBuilder = new IdBuilderWrapper(idBuilderConfig, cache);
	}

	private static DaoPool m_daoPool;
	private static final String TABLE_PREFIX = "mgProductProp";
	private static IdBuilderWrapper m_idBuilder;
	private static final int ID_BUILDER_INIT = 1;
	private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
	.buildTableName("mgProductProp")
	.buildAssistTableSuffix("idBuilder")
	.buildInitValue(ID_BUILDER_INIT)
	.build();
}
