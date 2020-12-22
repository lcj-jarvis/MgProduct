package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgProductPropSvr.domain.common.ProductPropCheck;
import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.domain.repository.ProductPropCacheCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropDaoCtrl;
import fai.comm.util.*;

public class ProductPropProc {

	public ProductPropProc(int flow, ProductPropDaoCtrl dao) {
		this.m_flow = flow;
		this.m_propDao = dao;
	}

	public int addPropInfo(int aid, Param info) {
		int rt = m_propDao.insert(aid, info, null);
		if(rt != Errno.OK) {
			Log.logErr(rt, "insert prop info error;flow=%d;aid=%d;", m_flow, aid);
			return rt;
		}

		return rt;
	}

	public int addPropList(int aid, FaiList<Param> propList) {
		int rt = m_propDao.batchInsert(aid, propList);
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
		rt = m_propDao.doBatchUpdate(aid, doBatchUpdater, doBatchMatcher, dataList, false);
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
		rt = m_propDao.delete(aid, matcher);
		if(rt != Errno.OK){
			Log.logErr(rt, "delPropList error;flow=%d;aid=%d;delIdList=%s", m_flow, aid, delIdList);
			return rt;
		}

		return rt;
	}

	private int getList(int aid, Ref<FaiList<Param>> listRef) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropCacheCtrl.getCacheList(aid);
		if(list != null || !list.isEmpty()) {
			listRef.value = list;
			return Errno.OK;
		}

		// 从db获取数据
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		int rt = m_propDao.select(aid, searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
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
