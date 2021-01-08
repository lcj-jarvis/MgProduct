package fai.MgProductStoreSvr.domain.repository;

import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.Errno;
import fai.comm.util.Log;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class InOutStoreRecordDaoCtrl extends DaoCtrl {

	private InOutStoreRecordDaoCtrl(int flow, int aid) {
		super(flow, aid);
	}

	public static InOutStoreRecordDaoCtrl getInstance(int flow, int aid) {
		if(m_daoProxy == null) {
			Log.logErr("m_daoProxy is not init;");
			return null;
		}
		return new InOutStoreRecordDaoCtrl(flow, aid);
	}

	public Integer buildId(){
		return buildId(false);
	}
	public Integer buildId(boolean needLock){
		int rt = openDao();
		if(rt != Errno.OK){
			return null;
		}
		return m_idBuilder.build(aid, m_dao, needLock);
	}
	public int clearIdBuilderCache(int aid){
		return m_idBuilder.clearCache(aid);
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

	@Override
	protected DaoProxy getDaoProxy() {
		return m_daoProxy;
	}
	@Override
	protected String getTableName(){
		return TABLE_NAME + "_"+ String.format("%04d", aid%1000);
	}
	private static final String TABLE_NAME = "inOutStoreRecord";

	public static void initIdBuilder(RedisCacheManager codisCache){
		if(m_idBuilder == null){
			synchronized (InOutStoreRecordDaoCtrl.class){
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
			.buildAutoIncField(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID)
			.build();
	private static IdBuilderWrapper m_idBuilder;
}
