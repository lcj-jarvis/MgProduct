package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.MgProductPropSvr.domain.repository.ProductPropValCacheCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropValDaoCtrl;
import fai.comm.util.*;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Set;

public class ProductPropValProc {

	public ProductPropValProc(int flow, ProductPropValDaoCtrl dao) {
		this.m_flow = flow;
		this.m_valDao = dao;
	}

	public int getListByPropIds(int aid, HashMap<Integer, Integer> idRels, Ref<FaiList<Param>> listRef) {
		int rt;
		if(idRels == null || idRels.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			Log.logErr(rt, "args error;flow=%d;aid=%d;propIds=%s;", m_flow, aid, idRels);
			return rt;
		}
		Set<Integer> propIds = idRels.keySet();
		FaiList<Param> list = new FaiList<Param>();
		for(Integer propId : propIds) {
			Ref<FaiList<Param>> tmpRef = new Ref<FaiList<Param>>();
			rt = getList(aid, propId, tmpRef);
			if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
				Log.logErr("get error;flow=%d;aid=%d;propId=%s;", m_flow, aid, propId);
				return rt;
			}
			// 设置rlPropId
			initInfo(idRels.get(propId), tmpRef.value);
			list.addAll(tmpRef.value);
		}
		listRef.value = list;
		if(list.isEmpty()) {
			return Errno.NOT_FOUND;
		}

		return Errno.OK;
	}

	public int delValList(int aid, int propId, FaiList<Integer> delValIds) {
		if(delValIds == null || delValIds.isEmpty()) {
			return Errno.OK;
		}
		ParamMatcher matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.EQ, propId);
		matcher.and(ProductPropValEntity.Info.PROP_VAL_ID, ParamMatcher.IN, delValIds);
		int rt = m_valDao.delete(aid, matcher);
		if(rt != Errno.OK){
			Log.logErr(rt, "delValList error;flow=%d;aid=%d;propId=%d;delIdList=%s", m_flow, aid, propId, delValIds);
			return rt;
		}

		return rt;
	}

	public int delValListByPropIds(int aid, FaiList<Integer> propIds) {
		if(propIds == null || propIds.isEmpty()) {
			return Errno.OK;
		}
		ParamMatcher matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.IN, propIds);
		int rt = m_valDao.delete(aid, matcher);
		if(rt != Errno.OK){
			Log.logErr(rt, "delValList error;flow=%d;aid=%d;propIds=%s;", m_flow, aid, propIds);
			return rt;
		}

		return rt;
	}

	public int setValList(int aid, int propId, FaiList<ParamUpdater> updaterList) {
		if(updaterList == null || updaterList.isEmpty()) {
			return Errno.OK;
		}
		Ref<FaiList<Param>> oldListRef = new Ref<FaiList<Param>>();
		int rt = getList(aid, propId, oldListRef);
		if (rt != Errno.OK) {
			Log.logErr(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}

		FaiList<Param> oldInfoList = oldListRef.value;
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

		rt = m_valDao.doBatchUpdate(aid, doBatchUpdater, doBatchMatcher, dataList, false);
		if(rt != Errno.OK){
			Log.logErr(rt, "doBatchUpdate product prop error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
			return rt;
		}
		return rt;
	}

	public int addValList(int aid, int propId, FaiList<Param> valList, boolean needLock) {
		if(valList == null || valList.isEmpty()) {
			return Errno.OK;
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
		int rt = m_valDao.batchInsert(aid, valList.clone());
		if(rt != Errno.OK) {
			Log.logErr(rt, "batch insert prop error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}

		return rt;
	}

	private int getList(int aid, int propId, Ref<FaiList<Param>> listRef) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropValCacheCtrl.getCacheList(aid, propId);
		if(list != null && !list.isEmpty()) {
			listRef.value = list;
			return Errno.OK;
		}

		// 从db获取数据
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		searchArg.matcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.EQ, propId);
		int rt = m_valDao.select(aid, searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			return rt;
		}
		if (listRef.value == null || listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;aid=%d", aid);
			return rt;
		}
		// 添加到缓存
		ProductPropValCacheCtrl.addCacheList(aid, propId, listRef.value);
		return Errno.OK;
	}

	private void initInfo(int rlPropId, FaiList<Param> list) {
		for(Param info : list) {
			info.setInt(ProductPropValEntity.Info.RL_PROP_ID, rlPropId);
		}
	}

	private int m_flow;
	private ProductPropValDaoCtrl m_valDao;
}
