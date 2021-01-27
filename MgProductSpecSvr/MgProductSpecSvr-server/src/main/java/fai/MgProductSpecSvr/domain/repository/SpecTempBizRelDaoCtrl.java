package fai.MgProductSpecSvr.domain.repository;

import fai.MgProductSpecSvr.domain.entity.SpecTempBizRelEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.DaoCtrl;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class SpecTempBizRelDaoCtrl extends DaoCtrl {

	private SpecTempBizRelDaoCtrl(int flow, int aid) {
		super(flow, aid);
	}

	public static SpecTempBizRelDaoCtrl getInstance(int flow, int aid) {
		if(m_daoProxy == null) {
			Log.logErr("m_daoProxy is not init;");
			return null;
		}
		return new SpecTempBizRelDaoCtrl(flow, aid);
	}


	public Integer buildId(int unionPriId){
		return buildId(unionPriId,false);
	}
	public Integer buildId(int unionPriId, boolean needLock){
		int rt = openDao();
		if(rt != Errno.OK){
			return null;
		}
		return m_idBuilder.build(aid, unionPriId, m_dao, needLock);
	}
	public Integer updateId(int unionPriId, int rlTpScId){
		return updateId(unionPriId, rlTpScId, false);
	}
	public Integer updateId(int unionPriId, int rlTpScId, boolean needLock){
		int rt = openDao();
		if(rt != Errno.OK){
			return null;
		}
		return m_idBuilder.update(aid, unionPriId, rlTpScId, m_dao, needLock);
	}

	public int restoreMaxId(int unionPriId){
		return restoreMaxId(unionPriId,false);
	}
	public int restoreMaxId(int unionPriId, boolean needLock){
		int rt = openDao();
		if(rt != Errno.OK){
			return rt;
		}
		return m_idBuilder.restoreMaxId(aid, unionPriId, flow, getTableName(), m_dao, needLock);
	}
	public int clearIdBuilderCache(int aid, int unionPriId) {
		return m_idBuilder.clearCache(aid, unionPriId);
	}



	@Override
	protected DaoPool getDaoPool() {
		return m_daoProxy.getDaoPool(getAid(), getGroup());
	}
	@Override
	protected String getTableName(){
		return TABLE_NAME;
	}
	private static final String TABLE_NAME = "specTempBizRel";

	public static void initIdBuilder(RedisCacheManager codisCache){
		if(m_idBuilder == null){
			synchronized (SpecTempBizRelDaoCtrl.class){
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
			.buildPrimaryMatchField(SpecTempBizRelEntity.Info.AID)
			.buildForeignMatchField(SpecTempBizRelEntity.Info.UNION_PRI_ID)
			.buildAutoIncField(SpecTempBizRelEntity.Info.RL_TP_SC_ID)
			.build();
	private static IdBuilderWrapper m_idBuilder;
}
