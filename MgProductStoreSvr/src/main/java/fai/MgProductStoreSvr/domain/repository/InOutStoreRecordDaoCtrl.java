package fai.MgProductStoreSvr.domain.repository;

import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.DaoPool;
import fai.comm.util.Errno;
import fai.comm.util.Log;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class InOutStoreRecordDaoCtrl extends DaoCtrl {

	private InOutStoreRecordDaoCtrl(int flow, int aid) {
		super(flow, aid);
		this.group = TABLE_ENUM.getGroup();
	}

	public static InOutStoreRecordDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCrtl transactionCrtl) {
		if(transactionCrtl == null){
			return null;
		}
		InOutStoreRecordDaoCtrl daoCtrl = getInstance(flow, aid);
		if(!transactionCrtl.registered(daoCtrl)){
			Log.logErr("registered InOutStoreRecordDaoCtrl err;flow=%d;aid=%d;", flow, aid);
			return null;
		}
		return daoCtrl;
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
	protected DaoPool getDaoPool() {
		return m_daoProxy.getDaoPool(aid, getGroup());
	}
	@Override
	protected String getTableName(){
		return TABLE_ENUM.getTable() + "_"+ String.format("%04d", aid%1000);
	}

	public static void initIdBuilder(RedisCacheManager codisCache){
		if(m_idBuilder == null){
			synchronized (InOutStoreRecordDaoCtrl.class){
				if(m_idBuilder == null){
					m_idBuilder = new IdBuilderWrapper(idBuilderConfig, codisCache);
				}
			}
		}
	}
	public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_IN_OUT_STORE_RECORD;

	private static final int ID_BUILDER_INIT = 1;
	private static IdBuilderConfig idBuilderConfig = new IdBuilderConfig.HeavyweightBuilder()
			.buildTableName(TABLE_ENUM.getTable())
			.buildInitValue(ID_BUILDER_INIT)
			.buildAssistTableSuffix("idBuilder")
			.buildAutoIncField(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID)
			.build();
	private static IdBuilderWrapper m_idBuilder;


}
