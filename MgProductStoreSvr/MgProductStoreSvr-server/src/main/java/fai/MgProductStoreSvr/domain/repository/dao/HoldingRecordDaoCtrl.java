package fai.MgProductStoreSvr.domain.repository.dao;

import fai.MgProductStoreSvr.domain.repository.TableDBMapping;
import fai.comm.util.DaoPool;
import fai.comm.util.Log;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class HoldingRecordDaoCtrl extends DaoCtrl {

	private HoldingRecordDaoCtrl(int flow, int aid) {
		super(flow, aid);
		this.group = TABLE_ENUM.getGroup();
	}

	public static HoldingRecordDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl transactionCtrl) {
		if(transactionCtrl == null){
			return null;
		}
		HoldingRecordDaoCtrl daoCtrl = getInstance(flow, aid);
		if(!transactionCtrl.register(daoCtrl)){
			Log.logErr("registered HoldingRecordDaoCtrl err;flow=%d;aid=%d;", flow, aid);
			return null;
		}
		return daoCtrl;
	}

	public static HoldingRecordDaoCtrl getInstance(int flow, int aid) {
		if(m_daoProxy == null) {
			Log.logErr("m_daoProxy is not init;");
			return null;
		}
		return new HoldingRecordDaoCtrl(flow, aid);
	}


	@Override
	protected DaoPool getDaoPool() {
		return m_daoProxy.getDaoPool(aid, getGroup());
	}
	@Override
	protected String getTableName(){
		return TABLE_ENUM.getTable();
	}
	public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_HOLDING_RECORD;

}
