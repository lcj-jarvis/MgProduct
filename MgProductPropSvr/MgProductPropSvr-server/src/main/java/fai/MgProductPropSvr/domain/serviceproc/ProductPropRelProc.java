package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgProductPropSvr.domain.common.LockUtil;
import fai.MgProductPropSvr.domain.entity.ProductPropRelEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropRelValObj;
import fai.MgProductPropSvr.domain.repository.ProductPropRelCacheCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropRelDaoCtrl;
import fai.MgProductPropSvr.interfaces.entity.ProductPropValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;

public class ProductPropRelProc {
	public ProductPropRelProc(int flow, int aid, TransactionCtrl tc) {
		this.m_flow = flow;
		this.m_relDao = ProductPropRelDaoCtrl.getInstance(flow, aid);
		init(tc);
	}

	public int addPropRelInfo(int aid, int unionPriId, int libId , Param info) {
		int rt;
		int count = getCount(aid, unionPriId, libId);
		if(count >= ProductPropRelValObj.Limit.COUNT_MAX) {
			rt = Errno.COUNT_LIMIT;
			throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductPropRelValObj.Limit.COUNT_MAX);
		}
		int rlPropId = creatAndSetId(aid, unionPriId, info);
		rt = m_relDao.insert(info);
		if(rt != Errno.OK) {
			throw new MgException(rt, "batch insert prop rel error;flow=%d;aid=%d;", m_flow, aid);
		}
		return rlPropId;
	}

	public FaiList<Integer> addPropRelList(int aid, int unionPriId, int libId, FaiList<Param> infoList) {
		int rt;
		if(infoList == null || infoList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "infoList is null;flow=%d;aid=%d;uid=%d;libId=%d;", m_flow, aid, unionPriId, libId);
		}
		int count = getCount(aid, unionPriId, libId) + infoList.size();
		if(count > ProductPropRelValObj.Limit.COUNT_MAX) {
			rt = Errno.COUNT_LIMIT;
			throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductPropRelValObj.Limit.COUNT_MAX);
		}
		FaiList<Integer> rlPropIds = new FaiList<>();
		for(int i = 0; i < infoList.size(); i++) {
			Param info = infoList.get(i);
			int rlPropId = creatAndSetId(aid, unionPriId, info);
			rlPropIds.add(rlPropId);
		}
		rt = m_relDao.batchInsert(infoList, null, false);
		if(rt != Errno.OK) {
			throw new MgException(rt, "batch insert prop rel error;flow=%d;aid=%d;", m_flow, aid);
		}
		return rlPropIds;
	}

	private int creatAndSetId(int aid, int unionPriId, Param info) {
		int rt;
		Integer rlPropId = info.getInt(ProductPropRelEntity.Info.RL_PROP_ID, 0);
		if(rlPropId <= 0) {
			rlPropId = m_relDao.buildId(aid, unionPriId, false);
			if (rlPropId == null) {
				rt = Errno.ERROR;
				throw new MgException(rt, "propId build error;flow=%d;aid=%d;", m_flow, aid);
			}
		}else {
			rlPropId = m_relDao.updateId(aid, unionPriId, rlPropId, false);
			if (rlPropId == null) {
				rt = Errno.ERROR;
				throw new MgException(rt, "rlPropId update error;flow=%d;aid=%d;", m_flow, aid);
			}
		}
		info.setInt(ProductPropRelEntity.Info.RL_PROP_ID, rlPropId);
		return rlPropId;
	}

	public void delPropList(int aid, int unionPriId, int libId, FaiList<Integer> delRlIdList) {
		int rt;
		if(delRlIdList == null || delRlIdList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delRlIdList);
		}

		ParamMatcher matcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
		matcher.and(ProductPropRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, libId);
		matcher.and(ProductPropRelEntity.Info.RL_PROP_ID, ParamMatcher.IN, delRlIdList);
		rt = m_relDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "delPropList error;flow=%d;aid=%d;delRlIdList=%s", m_flow, aid, delRlIdList);
		}
	}

	public void clearData(int aid, int unionPriId) {
		clearData(aid, new FaiList<>(Arrays.asList(unionPriId)));
	}

	public void clearData(int aid, FaiList<Integer> unionPriIds) {
		int rt;
		if(unionPriIds == null || unionPriIds.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "clearData args error, unionPriIds is null;flow=%d;aid=%d;unionPriIds=%s", m_flow, aid, unionPriIds);
		}
		ParamMatcher matcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
		rt = m_relDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "delPropList error;flow=%d;aid=%d;unionPriIds=%s", m_flow, aid, unionPriIds);
		}
		// 处理下idBuilder
		for(int unionPriId : unionPriIds) {
			m_relDao.restoreMaxId(aid, unionPriId, m_flow, false);
		}
		Log.logStd("clearData ok;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
	}

	/**
	 * 根据rlPropId list 获取 propId list
	 * @return
	 */
	public FaiList<Integer> getIdsByRlIds(int aid, int unionPriId, int libId, FaiList<Integer> rlIdList) {
		if(rlIdList == null || rlIdList.isEmpty()) {
			return null;
		}
		FaiList<Param> list = getList(aid, unionPriId, libId);
		FaiList<Integer> idList = new FaiList<Integer>();
		if(list.isEmpty()) {
			return idList;
		}

		list = Misc.getList(list, new ParamMatcher(ProductPropRelEntity.Info.RL_PROP_ID, ParamMatcher.IN, rlIdList));
		for(int i = 0; i < list.size(); i++) {
			Param info = list.get(i);
			idList.add(info.getInt(ProductPropRelEntity.Info.PROP_ID));
		}
		return idList;
	}

	public HashMap<Integer, Integer> getIdsWithRlIds(int aid, int unionPriId, int libId, FaiList<Integer> rlIdList) {
		if(rlIdList == null || rlIdList.isEmpty()) {
			return null;
		}
		FaiList<Param> list = getList(aid, unionPriId, libId);
		HashMap<Integer, Integer> idList = new HashMap<Integer, Integer>();
		if(list.isEmpty()) {
			return idList;
		}

		list = Misc.getList(list, new ParamMatcher(ProductPropRelEntity.Info.RL_PROP_ID, ParamMatcher.IN, rlIdList));
		for(int i = 0; i < list.size(); i++) {
			Param info = list.get(i);
			int propId = info.getInt(ProductPropRelEntity.Info.PROP_ID);
			int rlPropId = info.getInt(ProductPropRelEntity.Info.RL_PROP_ID);
			idList.put(propId, rlPropId);
		}
		return idList;
	}

	public void setPropList(int aid, int unionPriId, int libId, FaiList<ParamUpdater> updaterList, FaiList<ParamUpdater> propUpdaterList) {
		if(propUpdaterList != null) {
			propUpdaterList.clear();
		}
		FaiList<Param> oldInfoList = getList(aid, unionPriId, libId);
		FaiList<Param> dataList = new FaiList<Param>();
		Calendar now = Calendar.getInstance();
		for(ParamUpdater updater : updaterList){
			Param updateInfo = updater.getData();
			int rlPropId = updateInfo.getInt(ProductPropRelEntity.Info.RL_PROP_ID, 0);
			Param oldInfo = Misc.getFirstNullIsEmpty(oldInfoList, ProductPropRelEntity.Info.RL_PROP_ID, rlPropId);
			if(Str.isEmpty(oldInfo)){
				continue;
			}
			int propId = oldInfo.getInt(ProductPropRelEntity.Info.PROP_ID);
			oldInfo = updater.update(oldInfo, true);
			Param data = new Param();
			//只能修改rlFlag和sort
			int sort = oldInfo.getInt(ProductPropRelEntity.Info.SORT, 0);
			int rlFlag = oldInfo.getInt(ProductPropRelEntity.Info.RL_FLAG, 0);
			data.setInt(ProductPropRelEntity.Info.SORT, sort);
			data.setInt(ProductPropRelEntity.Info.RL_FLAG, rlFlag);
			data.setCalendar(ProductPropRelEntity.Info.UPDATE_TIME, now);

			data.assign(oldInfo, ProductPropRelEntity.Info.AID);
			data.assign(oldInfo, ProductPropRelEntity.Info.UNION_PRI_ID);
			data.assign(oldInfo, ProductPropRelEntity.Info.RL_LIB_ID);
			data.assign(oldInfo, ProductPropRelEntity.Info.RL_PROP_ID);
			dataList.add(data);
			if(propUpdaterList != null) {
				updateInfo.setInt(ProductPropRelEntity.Info.PROP_ID, propId);
				propUpdaterList.add(updater);
			}
		}

		ParamMatcher doBatchMatcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, "?");
		doBatchMatcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
		doBatchMatcher.and(ProductPropRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, "?");
		doBatchMatcher.and(ProductPropRelEntity.Info.RL_PROP_ID, ParamMatcher.EQ, "?");

		Param item = new Param();
		ParamUpdater doBatchUpdater = new ParamUpdater(item);
		item.setString(ProductPropRelEntity.Info.SORT, "?");
		item.setString(ProductPropRelEntity.Info.RL_FLAG, "?");
		item.setString(ProductPropRelEntity.Info.UPDATE_TIME, "?");
		int rt = m_relDao.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
		if(rt != Errno.OK){
			throw new MgException(rt, "doBatchUpdate product prop error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
		}
	}

	public FaiList<Param> getPropRelList(int aid, int unionPriId, int libId) {
		return getList(aid, unionPriId, libId);
	}

	private FaiList<Param> getList(int aid, int unionPriId, int libId) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropRelCacheCtrl.getCacheList(aid, unionPriId, libId);
		if(!Util.isEmptyList(list)) {
			return list;
		}

		LockUtil.PropRelLock.readLock(aid);
		try {
			// check again
			list = ProductPropRelCacheCtrl.getCacheList(aid, unionPriId, libId);
			if(!Util.isEmptyList(list)) {
				return list;
			}

			Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
			// db中获取
			SearchArg searchArg = new SearchArg();
			searchArg.matcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
			searchArg.matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
			searchArg.matcher.and(ProductPropRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, libId);
			int rt = m_relDao.select(searchArg, listRef);
			if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
				throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
			}

			list = listRef.value;
			if(list == null) {
				list = new FaiList<Param>();
			}
			if (list.isEmpty()) {
				rt = Errno.NOT_FOUND;
				Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
				return list;
			}
			// 添加到缓存
			ProductPropRelCacheCtrl.addCacheList(aid, unionPriId, libId, list);
		}finally {
			LockUtil.PropRelLock.readUnLock(aid);
		}

		return list;
	}

	public int getCount(int aid, int unionPriId, int libId) {
		FaiList<Param> list = getList(aid, unionPriId, libId);

		if(Util.isEmptyList(list)) {
			return 0;
		}
		return list.size();
	}

	public int getMaxSort(int aid, int unionPriId, int libId) {
		String sortCache = ProductPropRelCacheCtrl.getSortCache(aid, unionPriId, libId);
		if(!Str.isEmpty(sortCache)) {
			return Parser.parseInt(sortCache, ProductPropValObj.Default.SORT);
		}

		// db中获取
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		searchArg.matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
		searchArg.matcher.and(ProductPropRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, libId);
		Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
		int rt = m_relDao.select(searchArg, listRef, "max(sort) as sort");
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			return -1;
		}
		if (listRef.value == null || listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
			return ProductPropValObj.Default.SORT;
		}

		Param info = listRef.value.get(0);
		int sort = info.getInt(ProductPropRelEntity.Info.SORT, ProductPropValObj.Default.SORT);
		// 添加到缓存
		ProductPropRelCacheCtrl.setSortCache(aid, unionPriId, libId, sort);
		return sort;
	}

	private void init(TransactionCtrl tc) {
		if(!tc.register(m_relDao)) {
			throw new MgException("registered ProductPropRelDaoCtrl err;");
		}
	}

	private int m_flow;
	private ProductPropRelDaoCtrl m_relDao;
}
