package fai.MgProductSpecSvr.domain.repository;

import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.DaoPool;
import fai.comm.util.Errno;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class ProductSpecSkuDaoCtrl extends DaoCtrl {

	private ProductSpecSkuDaoCtrl(int flow, int aid) {
		super(flow, aid);
	}

	public static ProductSpecSkuDaoCtrl getInstance(int flow, int aid) {
		if(m_daoProxy == null) {
			Log.logErr("m_daoProxy is not init;");
			return null;
		}
		return new ProductSpecSkuDaoCtrl(flow, aid);
	}

	public Long buildId(){
		return buildId(false);
	}
	public Long buildId(boolean needLock){
		int rt = openDao();
		if(rt != Errno.OK){
			return null;
		}
		Integer build = m_idBuilder.build(aid, m_dao, needLock);
		if(build == null){
			return null;
		}
		return new Long(build);
	}
	public int restoreMaxId(){
		return restoreMaxId(false);
	}
	public int restoreMaxId(boolean needLock){
		int rt = openDao();
		if(rt != Errno.OK){
			return rt;
		}
		return m_idBuilder.restoreMaxId(aid, flow, getTableName(), m_dao, needLock);
	}
	public int clearIdBuilderCache(int aid) {
		return m_idBuilder.clearCache(aid);
	}

	@Override
	protected DaoPool getDaoPool() {
		return m_daoProxy.getDaoPool(getAid(), getGroup());
	}
	@Override
	protected String getTableName(){
		return TABLE_NAME + "_"+ String.format("%04d", aid%1000);
	}
	private static final String TABLE_NAME = "productSpecSKU";

	public static void initIdBuilder(RedisCacheManager codisCache){
		if(m_idBuilder == null){
			synchronized (ProductSpecSkuDaoCtrl.class){
				if(m_idBuilder == null){
					m_idBuilder = new IdBuilderWrapper(idBuilderConfig, codisCache);
				}
			}
		}
	}
	private static final int ID_BUILDER_INIT = 1;
	private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
			.buildTableName(TABLE_NAME)
			.buildInitValue(ID_BUILDER_INIT)
			.buildAssistTableSuffix("idBuilder")
			.buildAutoIncField(ProductSpecSkuEntity.Info.SKU_ID)
			.build();
	private static IdBuilderWrapper m_idBuilder;
}
