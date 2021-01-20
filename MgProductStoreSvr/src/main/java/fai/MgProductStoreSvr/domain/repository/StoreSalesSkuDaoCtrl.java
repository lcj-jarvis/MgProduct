package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.*;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class StoreSalesSkuDaoCtrl extends DaoCtrl {

	private StoreSalesSkuDaoCtrl(int flow, int aid) {
		super(flow, aid);
		this.group = TABLE_ENUM.getGroup();
	}

	public static StoreSalesSkuDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCrtl transactionCrtl) {
		if(transactionCrtl == null){
			return null;
		}
		StoreSalesSkuDaoCtrl daoCtrl = getInstance(flow, aid);
		if(!transactionCrtl.registered(daoCtrl)){
			Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;", flow, aid);
			return null;
		}
		return daoCtrl;
	}


	public static StoreSalesSkuDaoCtrl getInstance(int flow, int aid) {
		if(m_daoProxy == null) {
			Log.logErr("m_daoProxy is not init;");
			return null;
		}
		return new StoreSalesSkuDaoCtrl(flow, aid);
	}

	public int executeQuery(String sql, Ref<FaiList<Param>> listRef){
		int rt = openDao();
		if(rt != Errno.OK){
			return rt;
		}
		FaiList<Param> list = m_dao.executeQuery(sql);
		listRef.value = list;
		return Errno.OK;
	}

	@Override
	protected DaoPool getDaoPool() {
		return m_daoProxy.getDaoPool(aid, getGroup());
	}
	@Override
	public String getTableName(){
		return TABLE_ENUM.getTable() + "_"+ String.format("%04d", aid%1000);
	}
	public static final TableDBMapping.TableEnum TABLE_ENUM = TableDBMapping.TableEnum.MG_STORE_SALE_SKU;
}
