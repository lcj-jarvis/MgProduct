package fai.MgProductStoreSvr.domain.repository.dao;

import fai.MgProductStoreSvr.domain.repository.TableDBMapping;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * dao ctrl中不再对传进来的数据做解析校验
 * 主要是处理dao相关逻辑
 */
public class StoreSalesSkuDaoCtrl extends DaoCtrlWithoutDel {

	private StoreSalesSkuDaoCtrl(int flow, int aid) {
		super(flow, aid);
		this.group = TABLE_ENUM.getGroup();
	}

	public static StoreSalesSkuDaoCtrl getInstanceWithRegistered(int flow, int aid, TransactionCtrl transactionCtrl) {
		if(transactionCtrl == null){
			return null;
		}
		StoreSalesSkuDaoCtrl daoCtrl = getInstance(flow, aid);
		if(!transactionCtrl.register(daoCtrl)){
			Log.logErr("registered StoreSalesSkuDaoCtrl err;flow=%d;aid=%d;", flow, aid);
			return null;
		}
		return daoCtrl;
	}

	public int selectWithDel(SearchArg searchArg, Ref<FaiList<Param>> listRef, String... onlyNeedFields) {
		int rt = openDao();
		if(rt != Errno.OK){
			return rt;
		}
		Dao.SelectArg sltArg = new Dao.SelectArg();
		sltArg.table = getTableName();
		sltArg.searchArg = searchArg;
		if(onlyNeedFields != null && onlyNeedFields.length > 0){
			sltArg.field = Str.join(",", onlyNeedFields);
		}
		FaiList<Param> list = m_dao.select(sltArg);
		if(list == null) {
			rt = Errno.DAO_ERROR;
			Log.logErr(rt, "select db err;");
			return rt;
		}
		listRef.value = list;
		if(list.isEmpty()) {
			rt = Errno.NOT_FOUND;
			return rt;
		}
		return rt;
	}

	public int selectWithDel(SearchArg searchArg, Ref<FaiList<Param>> listRef) {
		return selectWithDel(searchArg, listRef, null);
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
