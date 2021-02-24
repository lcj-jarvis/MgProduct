package fai.MgProductStoreSvr.domain.repository;

import fai.MgProductStoreSvr.domain.entity.InOutStoreRecordEntity;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoWithIdBuilderCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class InOutStoreRecordDaoCtrl extends DaoWithIdBuilderCtrl {

	private InOutStoreRecordDaoCtrl(int flow, int aid) {
		super(flow, aid, getIdBuilderWrapper());
		this.group = TABLE_ENUM.getGroup();
	}

	public static InOutStoreRecordDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl transactionCtrl) {
		if(transactionCtrl == null){
			return null;
		}
		InOutStoreRecordDaoCtrl daoCtrl = getInstance(flow, aid);
		if(!transactionCtrl.register(daoCtrl)){
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

	@Override
	protected DaoPool getDaoPool() {
		return m_daoProxy.getDaoPool(aid, getGroup());
	}
	@Override
	protected String getTableName(){
		return TABLE_ENUM.getTable() + "_"+ String.format("%04d", aid%1000);
	}

	private static IdBuilderWrapper getIdBuilderWrapper() {
		if(m_idBuilder == null){
			synchronized (InOutStoreRecordDaoCtrl.class){
				if(m_idBuilder == null){
					m_idBuilder = new IdBuilderWrapper(
							new IdBuilderConfig.HeavyweightBuilder()
									.buildTableName(TABLE_ENUM.getTable())
									.buildInitValue(ID_BUILDER_INIT)
									.buildAssistTableSuffix("idBuilder")
									.buildAutoIncField(InOutStoreRecordEntity.Info.IN_OUT_STORE_REC_ID)
									.build(),
							CacheCtrl.m_cache);
				}
			}
		}
		return m_idBuilder;
	}

	public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_IN_OUT_STORE_RECORD;

	private static final int ID_BUILDER_INIT = 1;
	private static IdBuilderWrapper m_idBuilder;
}
