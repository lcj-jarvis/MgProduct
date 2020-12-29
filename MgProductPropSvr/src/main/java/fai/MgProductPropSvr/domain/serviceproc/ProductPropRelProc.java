package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropRelEntity;
import fai.MgProductPropSvr.domain.repository.ProductPropRelCacheCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropRelDaoCtrl;
import fai.MgProductPropSvr.interfaces.entity.ProductPropValObj;
import fai.comm.util.*;

import java.util.HashMap;

public class ProductPropRelProc {
	public ProductPropRelProc(int flow, ProductPropRelDaoCtrl dao) {
		this.m_flow = flow;
		this.m_relDao = dao;
	}

	public int addPropRelInfo(int aid, Param info) {
		int rt = m_relDao.insert(aid, info, null);
		if(rt != Errno.OK) {
			Log.logErr(rt, "batch insert prop rel error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}
		return rt;
	}

	public int addPropRelList(int aid, FaiList<Param> infoList) {
		int rt = m_relDao.batchInsert(aid, infoList);
		if(rt != Errno.OK) {
			Log.logErr(rt, "batch insert prop rel error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}
		return rt;
	}

	public int delPropList(int aid, int unionPriId, int libId, FaiList<Integer> delRlIdList) {
		int rt;
		if(delRlIdList == null || delRlIdList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			Log.logErr(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delRlIdList);
			return rt;
		}

		ParamMatcher matcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
		matcher.and(ProductPropRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, libId);
		matcher.and(ProductPropRelEntity.Info.RL_PROP_ID, ParamMatcher.IN, delRlIdList);
		rt = m_relDao.delete(aid, matcher);
		if(rt != Errno.OK){
			Log.logErr(rt, "delPropList error;flow=%d;aid=%d;delRlIdList=%s", m_flow, aid, delRlIdList);
			return rt;
		}

		rt = Errno.OK;
		return rt;
	}

	/**
	 * 根据rlPropId list 获取 propId list
	 * @return
	 */
	public FaiList<Integer> getIdsByRlIds(int aid, int unionPriId, int libId, FaiList<Integer> rlIdList) {
		if(rlIdList == null || rlIdList.isEmpty()) {
			return null;
		}
		Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
		int rt = getList(aid, unionPriId, libId, listRef);
		if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
			Log.logErr(rt, "get error;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
			return null;
		}
		FaiList<Integer> idList = new FaiList<Integer>();
		FaiList<Param> list = listRef.value;
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
		Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
		int rt = getList(aid, unionPriId, libId, listRef);
		if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
			Log.logErr(rt, "get error;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
			return null;
		}
		HashMap<Integer, Integer> idList = new HashMap<Integer, Integer>();
		FaiList<Param> list = listRef.value;
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

	public int setPropList(int aid, int unionPriId, int libId, FaiList<ParamUpdater> updaterList, FaiList<ParamUpdater> propUpdaterList) {
		if(propUpdaterList != null) {
			propUpdaterList.clear();
		}
		Ref<FaiList<Param>> oldListRef = new Ref<FaiList<Param>>();
		int rt = getList(aid, unionPriId, libId, oldListRef);
		if (rt != Errno.OK) {
			Log.logErr(rt, "get error;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
			return rt;
		}
		FaiList<Param> oldInfoList = oldListRef.value;
		FaiList<Param> dataList = new FaiList<Param>();
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

			data.assign(oldInfo, ProductPropRelEntity.Info.AID);
			data.assign(oldInfo, ProductPropRelEntity.Info.UNION_PRI_ID);
			data.assign(oldInfo, ProductPropRelEntity.Info.RL_LIB_ID);
			data.assign(oldInfo, ProductPropRelEntity.Info.RL_PROP_ID);
			dataList.add(data);
			if(propUpdaterList != null) {
				updateInfo.setInt(ProductPropEntity.Info.PROP_ID, propId);
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
		rt = m_relDao.doBatchUpdate(aid, doBatchUpdater, doBatchMatcher, dataList, false);
		if(rt != Errno.OK){
			Log.logErr(rt, "doBatchUpdate product prop error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
			return rt;
		}
		rt = Errno.OK;
		return rt;
	}

	public int getPropRelList(int aid, int unionPriId, int libId, Ref<FaiList<Param>> listRef) {
		int rt = getList(aid, unionPriId, libId, listRef);
		if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
			Log.logErr(rt, "get error;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
			return rt;
		}

		return rt;
	}

	private int getList(int aid, int unionPriId, int libId, Ref<FaiList<Param>> listRef) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropRelCacheCtrl.getCacheList(aid, unionPriId, libId);
		if(list != null && !list.isEmpty()) {
			listRef.value = list;
			return Errno.OK;
		}

		// db中获取
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		searchArg.matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
		searchArg.matcher.and(ProductPropRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, libId);
		int rt = m_relDao.select(aid, searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			return rt;
		}
		if (listRef.value == null || listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;libId=%d;", m_flow, aid, unionPriId, libId);
			return rt;
		}
		// 添加到缓存
		ProductPropRelCacheCtrl.addCacheList(aid, unionPriId, libId, listRef.value);
		return Errno.OK;
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
		int rt = m_relDao.select(aid, searchArg, "max(sort) as sort", listRef);
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

	private int m_flow;
	private ProductPropRelDaoCtrl m_relDao;
}
