package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValObj;
import fai.MgProductPropSvr.domain.repository.ProductPropRelDaoCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropValCacheCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropValDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Set;

public class ProductPropValProc {

	public ProductPropValProc(int flow, int aid, TransactionCtrl tc) {
		this.m_flow = flow;
		this.m_valDao = ProductPropValDaoCtrl.getInstance(flow, aid);
		init(tc);
	}

	public FaiList<Param> getListByPropIds(int aid, HashMap<Integer, Integer> idRels) {
		int rt;
		if(idRels == null || idRels.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "args error;flow=%d;aid=%d;propIds=%s;", m_flow, aid, idRels);
		}
		Set<Integer> propIds = idRels.keySet();
		FaiList<Param> list = new FaiList<Param>();
		for(Integer propId : propIds) {
			FaiList<Param> tmpList = getList(aid, propId);
			if(Util.isEmptyList(tmpList)) {
				continue;
			}
			// 设置rlPropId
			initInfo(idRels.get(propId), tmpList);
			list.addAll(tmpList);
		}

		return list;
	}

	public void delValList(int aid, int propId, FaiList<Integer> delValIds) {
		if(delValIds == null || delValIds.isEmpty()) {
			return;
		}
		ParamMatcher matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.EQ, propId);
		matcher.and(ProductPropValEntity.Info.PROP_VAL_ID, ParamMatcher.IN, delValIds);
		int rt = m_valDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "delValList error;flow=%d;aid=%d;propId=%d;delIdList=%s", m_flow, aid, propId, delValIds);
		}
	}

	public void delValListByPropIds(int aid, FaiList<Integer> propIds) {
		if(propIds == null || propIds.isEmpty()) {
			return;
		}
		ParamMatcher matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.IN, propIds);
		int rt = m_valDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "delValList error;flow=%d;aid=%d;propIds=%s;", m_flow, aid, propIds);
		}
	}

	public void setValList(int aid, int propId, FaiList<ParamUpdater> updaterList) {
		if(updaterList == null || updaterList.isEmpty()) {
			return;
		}
		FaiList<Param> oldInfoList = getList(aid, propId);

		FaiList<Param> dataList = new FaiList<Param>();
		Calendar now = Calendar.getInstance();
		for(ParamUpdater updater : updaterList){
			Param updateInfo = updater.getData();
			int valId = updateInfo.getInt(ProductPropValEntity.Info.PROP_VAL_ID, 0);
			Param oldInfo = Misc.getFirstNullIsEmpty(oldInfoList, ProductPropValEntity.Info.PROP_VAL_ID, valId);
			if(Str.isEmpty(oldInfo)){
				continue;
			}
			oldInfo = updater.update(oldInfo, true);
			Param data = new Param();
			data.assign(oldInfo, ProductPropValEntity.Info.VAL);
			data.assign(oldInfo, ProductPropValEntity.Info.SORT);
			data.assign(oldInfo, ProductPropValEntity.Info.DATA_TYPE);
			data.setCalendar(ProductPropValEntity.Info.UPDATE_TIME, now);
			data.setInt(ProductPropValEntity.Info.AID, aid);
			data.setInt(ProductPropValEntity.Info.PROP_ID, propId);
			data.setInt(ProductPropValEntity.Info.PROP_VAL_ID, valId);
			dataList.add(data);
		}

		ParamMatcher doBatchMatcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, "?");
		doBatchMatcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.EQ, "?");
		doBatchMatcher.and(ProductPropValEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, "?");

		Param item = new Param();
		ParamUpdater doBatchUpdater = new ParamUpdater(item);
		item.setString(ProductPropValEntity.Info.VAL, "?");
		item.setString(ProductPropValEntity.Info.SORT, "?");
		item.setString(ProductPropValEntity.Info.DATA_TYPE, "?");
		item.setString(ProductPropValEntity.Info.UPDATE_TIME, "?");

		int rt = m_valDao.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, false);
		if(rt != Errno.OK){
			throw new MgException(rt, "doBatchUpdate product prop error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
		}
	}

	public void addValList(int aid, int propId, FaiList<Param> valList, boolean needLock) {
		int rt;
		if(valList == null || valList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "valList is null;flow=%d;aid=%d;", m_flow, aid);
		}
		FaiList<Param> list = getList(aid, propId);
		int count = list.size();
		if(count >= ProductPropValObj.Limit.COUNT_MAX) {
			rt = Errno.COUNT_LIMIT;
			throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductPropValObj.Limit.COUNT_MAX);
		}
		Calendar now = Calendar.getInstance();
		// 数据
		for(int i = 0; i < valList.size(); i++) {
			int valId = m_valDao.buildId(aid, needLock);
			Param info  = valList.get(i);
			info.setInt(ProductPropValEntity.Info.AID, aid);
			info.setInt(ProductPropValEntity.Info.PROP_ID, propId);
			info.setInt(ProductPropValEntity.Info.PROP_VAL_ID, valId);
			info.setCalendar(ProductPropValEntity.Info.CREATE_TIME, now);
			info.setCalendar(ProductPropValEntity.Info.UPDATE_TIME, now);
		}
		rt = m_valDao.batchInsert(valList.clone(), null, true);
		if(rt != Errno.OK) {
			throw new MgException(rt, "batch insert prop error;flow=%d;aid=%d;", m_flow, aid);
		}
	}

	private FaiList<Param> getList(int aid, int propId) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropValCacheCtrl.getCacheList(aid, propId);
		if(list != null && !list.isEmpty()) {
			return list;
		}

		Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
		// 从db获取数据
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		searchArg.matcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.EQ, propId);
		int rt = m_valDao.select(searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "getList error;flow=%d;aid=%d;propId=%d;", m_flow, aid, propId);
		}
		list = listRef.value;
		if(list == null) {
			list = new FaiList<Param>();
		}
		if (Util.isEmptyList(list)) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;aid=%d", aid);
			return list;
		}
		// 添加到缓存
		ProductPropValCacheCtrl.addCacheList(aid, propId, list);
		return list;
	}

	private void initInfo(int rlPropId, FaiList<Param> list) {
		for(Param info : list) {
			info.setInt(ProductPropValEntity.Info.RL_PROP_ID, rlPropId);
		}
	}

	private void init(TransactionCtrl tc) {
		if(!tc.register(m_valDao)) {
			throw new MgException("registered ProductPropValDaoCtrl err;");
		}
	}

	private int m_flow;
	private ProductPropValDaoCtrl m_valDao;
}
