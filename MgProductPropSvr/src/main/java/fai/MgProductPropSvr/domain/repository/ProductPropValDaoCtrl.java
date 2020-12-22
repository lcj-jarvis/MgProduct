package fai.MgProductPropSvr.domain.repository;

import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.DaoPool;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.comm.middleground.repository.DaoCtrl;

public class ProductPropValDaoCtrl extends DaoCtrl {
	private ProductPropValDaoCtrl(FaiSession session) {
		super(session);
	}

	@Override
	public String getTableName(int aid) {
		return TABLE_PREFIX + "_" + String.format("%04d", aid % 1000);
	}

	@Override
	protected DaoPool getDaoPool() {
		return m_daoPool;
	}

	public static ProductPropValDaoCtrl getInstance(FaiSession session) {
		if(m_daoPool == null) {
			Log.logErr("m_daoPool is not init;");
			return null;
		}
		return new ProductPropValDaoCtrl(session);
	}

	public Integer buildId(int aid, boolean needLock) {
		int rt = openDao();
		if(rt != Errno.OK) {
			return null;
		}
		return m_idBuilder.build(aid, m_dao, needLock);
	}

	public static void init(DaoPool daoPool, RedisCacheManager cache) {
		m_daoPool = daoPool;
		m_idBuilder = new IdBuilderWrapper(idBuilderConfig, cache);
	}

	private static DaoPool m_daoPool;
	private static final String TABLE_PREFIX = "productPropVal";
	private static IdBuilderWrapper m_idBuilder;
	private static final int ID_BUILDER_INIT = 1;
	private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
	.buildTableName("productPropVal")
	.buildAssistTableSuffix("idBuilder")
	.buildInitValue(ID_BUILDER_INIT)
	.build();
}
