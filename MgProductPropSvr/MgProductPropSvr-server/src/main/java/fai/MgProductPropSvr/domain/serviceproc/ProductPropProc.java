package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgProductPropSvr.domain.common.ProductPropCheck;
import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValObj;
import fai.MgProductPropSvr.domain.repository.ProductPropCacheCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropDaoCtrl;
import fai.comm.util.*;

public class ProductPropProc {

	public ProductPropProc(int flow, ProductPropDaoCtrl dao) {
		this.m_flow = flow;
		this.m_propDao = dao;
	}

	public int addPropInfo(int aid, Param info) {
		Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
		int rt = getList(aid, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			return rt;
		}
		FaiList<Param> list = listRef.value;
		if(list == null) {
			list = new FaiList<Param>();
		}
		int count = list.size();
		if(count >= ProductPropValObj.Limit.COUNT_MAX) {
			rt = Errno.COUNT_LIMIT;
			Log.logErr(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductPropValObj.Limit.COUNT_MAX);
			return rt;
		}
		String name = info.getString(ProductPropEntity.Info.NAME);
		Param existInfo = Misc.getFirst(list, ProductPropEntity.Info.NAME, name);
		if(!Str.isEmpty(existInfo)) {
			rt = Errno.ALREADY_EXISTED;
			Log.logErr(rt, "prop name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, name);
			return rt;
		}
		rt = m_propDao.insert(info);
		if(rt != Errno.OK) {
			Log.logErr(rt, "insert prop info error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}

		return rt;
	}

	public int addPropList(int aid, FaiList<Param> propList) {
		int rt;
		if(propList == null || propList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			Log.logErr(rt, "args error;propList is null;aid=%d", aid);
			return rt;
		}
		Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
		rt = getList(aid, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			return rt;
		}
		FaiList<Param> list = listRef.value;
		if(list == null) {
			list = new FaiList<Param>();
		}
		int count = list.size() + propList.size();
		if(count > ProductPropValObj.Limit.COUNT_MAX) {
			rt = Errno.COUNT_LIMIT;
			Log.logErr(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductPropValObj.Limit.COUNT_MAX);
			return rt;
		}
		// 校验参数名是否已经存在
		for(Param info : propList) {
			String name = info.getString(ProductPropEntity.Info.NAME);
			Param existInfo = Misc.getFirst(listRef.value, ProductPropEntity.Info.NAME, name);
			if(!Str.isEmpty(existInfo)) {
				rt = Errno.ALREADY_EXISTED;
				Log.logErr(rt, "prop name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, name);
				return rt;
			}
		}
		rt = m_propDao.batchInsert(propList, null, true);
		if(rt != Errno.OK) {
			Log.logErr(rt, "batch insert prop error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}

		return rt;
	}

	public int getPropList(int aid, Ref<FaiList<Param>> listRef) {
		int rt = getList(aid, listRef);
		if (rt != Errno.OK && rt != Errno.NOT_FOUND) {
			Log.logErr(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}

		return rt;
	}

	public int setPropList(int aid, FaiList<ParamUpdater> updaterList) {
		int rt;
		for(ParamUpdater updater : updaterList){
			Param updateInfo = updater.getData();
			String name = updateInfo.getString(ProductPropEntity.Info.NAME);
			if(name != null && !ProductPropCheck.isNameValid(name)) {
				rt = Errno.ARGS_ERROR;
				Log.logErr(rt, "flow=%d;aid=%d;name=%s", m_flow, aid, name);
				return rt;
			}
		}
		Ref<FaiList<Param>> oldListRef = new Ref<FaiList<Param>>();
		rt = getList(aid, oldListRef);
		if (rt != Errno.OK) {
			Log.logErr(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}
		FaiList<Param> oldInfoList = oldListRef.value;
		FaiList<Param> dataList = new FaiList<Param>();
		for(ParamUpdater updater : updaterList){
			Param updateInfo = updater.getData();
			int propId = updateInfo.getInt(ProductPropEntity.Info.PROP_ID, 0);
			Param oldInfo = Misc.getFirstNullIsEmpty(oldInfoList, ProductPropEntity.Info.PROP_ID, propId);
			if(Str.isEmpty(oldInfo)){
				continue;
			}
			oldInfo = updater.update(oldInfo, true);
			Param data = new Param();
			data.assign(oldInfo, ProductPropEntity.Info.NAME);
			data.assign(oldInfo, ProductPropEntity.Info.FLAG);
			data.assign(oldInfo, ProductPropEntity.Info.AID);
			data.assign(oldInfo, ProductPropEntity.Info.PROP_ID);
			dataList.add(data);
		}

		ParamMatcher doBatchMatcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, "?");
		doBatchMatcher.and(ProductPropEntity.Info.PROP_ID, ParamMatcher.EQ, "?");

		Param item = new Param();
		ParamUpdater doBatchUpdater = new ParamUpdater(item);
		item.setString(ProductPropEntity.Info.NAME, "?");
		item.setString(ProductPropEntity.Info.FLAG, "?");
		rt = m_propDao.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, false);
		if(rt != Errno.OK){
			Log.logErr(rt, "doBatchUpdate product prop error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
			return rt;
		}
		rt = Errno.OK;
		return rt;
	}

	public int delPropList(int aid, FaiList<Integer> delIdList) {
		int rt;
		if(delIdList == null || delIdList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			Log.logErr(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delIdList);
			return rt;
		}

		ParamMatcher matcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropEntity.Info.PROP_ID, ParamMatcher.IN, delIdList);
		rt = m_propDao.delete(matcher);
		if(rt != Errno.OK){
			Log.logErr(rt, "delPropList error;flow=%d;aid=%d;delIdList=%s", m_flow, aid, delIdList);
			return rt;
		}

		return rt;
	}

	private int getList(int aid, Ref<FaiList<Param>> listRef) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropCacheCtrl.getCacheList(aid);
		if(list != null && !list.isEmpty()) {
			listRef.value = list;
			return Errno.OK;
		}

		// 从db获取数据
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		int rt = m_propDao.select(searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			Log.logErr(rt, "getList error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}
		if (listRef.value == null || listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;aid=%d", aid);
			return rt;
		}
		// 添加到缓存
		ProductPropCacheCtrl.addCacheList(aid, listRef.value);
		return Errno.OK;
	}

	private int m_flow;
	private ProductPropDaoCtrl m_propDao;
}
