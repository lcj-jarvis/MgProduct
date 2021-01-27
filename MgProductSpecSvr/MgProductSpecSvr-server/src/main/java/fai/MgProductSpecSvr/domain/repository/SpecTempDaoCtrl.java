package fai.MgProductSpecSvr.domain.repository;

import fai.MgProductSpecSvr.domain.entity.SpecTempEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.DaoCtrl;


/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class SpecTempDaoCtrl extends DaoCtrl {

	private SpecTempDaoCtrl(int flow, int aid) {
		super(flow, aid);
	}

	public static SpecTempDaoCtrl getInstance(int flow, int aid) {
		if(m_daoProxy == null) {
			Log.logErr("m_daoProxy is not init;");
			return null;
		}
		return new SpecTempDaoCtrl(flow, aid);
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
		return TABLE_NAME;
	}
	private static final String TABLE_NAME = "specTemp";

	public static void initIdBuilder(RedisCacheManager codisCache){
		if(m_idBuilder == null){
			synchronized (SpecTempDaoCtrl.class){
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
			.buildAutoIncField(SpecTempEntity.Info.TP_SC_ID)
			.build();
	private static IdBuilderWrapper m_idBuilder;


}
