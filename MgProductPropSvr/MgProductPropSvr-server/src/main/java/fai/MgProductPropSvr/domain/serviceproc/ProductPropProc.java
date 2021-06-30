package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgProductPropSvr.domain.common.LockUtil;
import fai.MgProductPropSvr.domain.common.ProductPropCheck;
import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValObj;
import fai.MgProductPropSvr.domain.repository.ProductPropCacheCtrl;
import fai.MgProductPropSvr.domain.repository.ProductPropDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;

public class ProductPropProc {

	public ProductPropProc(int flow, int aid, TransactionCtrl tc) {
		this.m_flow = flow;
		this.m_propDao = ProductPropDaoCtrl.getInstance(flow, aid);
		init(tc);
	}

	public int addPropInfo(int aid, Param info) {
		int rt;
		FaiList<Param> list = getList(aid);
		int count = list.size();
		if(count >= ProductPropValObj.Limit.COUNT_MAX) {
			rt = Errno.COUNT_LIMIT;
			throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductPropValObj.Limit.COUNT_MAX);
		}
		String name = info.getString(ProductPropEntity.Info.NAME);
		Param existInfo = Misc.getFirst(list, ProductPropEntity.Info.NAME, name);
		if(!Str.isEmpty(existInfo)) {
			rt = Errno.ALREADY_EXISTED;
			throw new MgException(rt, "prop name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, name);
		}
		int propId = creatAndSetId(aid, info);
		rt = m_propDao.insert(info);
		if(rt != Errno.OK) {
			throw new MgException(rt, "insert prop info error;flow=%d;aid=%d;", m_flow, aid);
		}

		return propId;
	}

	public FaiList<Integer> addPropList(int aid, FaiList<Param> propList) {
		int rt;
		if(propList == null || propList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "args error;propList is null;aid=%d", aid);
		}
		FaiList<Param> list = getList(aid);
		int count = list.size() + propList.size();
		if(count > ProductPropValObj.Limit.COUNT_MAX) {
			rt = Errno.COUNT_LIMIT;
			throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductPropValObj.Limit.COUNT_MAX);
		}
		FaiList<Integer> propIds = new FaiList<>();
		// 校验参数名是否已经存在
		for(Param info : propList) {
			String name = info.getString(ProductPropEntity.Info.NAME);
			Param existInfo = Misc.getFirst(list, ProductPropEntity.Info.NAME, name);
			if(!Str.isEmpty(existInfo)) {
				rt = Errno.ALREADY_EXISTED;
				throw new MgException(rt, "prop name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, name);
			}
			int propId = creatAndSetId(aid, info);
			propIds.add(propId);
		}
		rt = m_propDao.batchInsert(propList, null, false);
		if(rt != Errno.OK) {
			throw new MgException(rt, "batch insert prop error;flow=%d;aid=%d;", m_flow, aid);
		}

		return propIds;
	}

	private int creatAndSetId(int aid, Param info) {
		int rt;
		Integer propId = info.getInt(ProductPropEntity.Info.PROP_ID, 0);
		if(propId <= 0) {
			propId = m_propDao.buildId(aid, false);
			if (propId == null) {
				rt = Errno.ERROR;
				throw new MgException(rt, "propId build error;flow=%d;aid=%d;", m_flow, aid);
			}
		}else {
			propId = m_propDao.updateId(aid, propId, false);
			if (propId == null) {
				rt = Errno.ERROR;
				throw new MgException(rt, "pdId update error;flow=%d;aid=%d;", m_flow, aid);
			}
		}
		info.setInt(ProductPropEntity.Info.PROP_ID, propId);
		return propId;
	}

	public FaiList<Param> getPropList(int aid) {
		return getList(aid);
	}

	public void setPropList(int aid, FaiList<ParamUpdater> updaterList) {
		int rt;
		for(ParamUpdater updater : updaterList){
			Param updateInfo = updater.getData();
			String name = updateInfo.getString(ProductPropEntity.Info.NAME);
			if(name != null && !ProductPropCheck.isNameValid(name)) {
				rt = Errno.ARGS_ERROR;
				throw new MgException(rt, "flow=%d;aid=%d;name=%s", m_flow, aid, name);
			}
		}
		FaiList<Param> oldInfoList = getList(aid);
		if(Util.isEmptyList(oldInfoList)) {
			throw new MgException(Errno.ARGS_ERROR, "set error;get old info is empty;flow=%d;aid=%d;", m_flow, aid);
		}
		FaiList<Param> dataList = new FaiList<Param>();
		Calendar now = Calendar.getInstance();
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
			data.setCalendar(ProductPropEntity.Info.UPDATE_TIME, now);
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
		item.setString(ProductPropEntity.Info.UPDATE_TIME, "?");
		rt = m_propDao.doBatchUpdate(doBatchUpdater, doBatchMatcher, dataList, true);
		if(rt != Errno.OK){
			throw new MgException(rt, "doBatchUpdate product prop error;flow=%d;aid=%d;updateList=%s", m_flow, aid, dataList);
		}
	}

	public void delPropList(int aid, FaiList<Integer> delIdList) {
		int rt;
		if(delIdList == null || delIdList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "args err;flow=%d;aid=%d;idList=%s", m_flow, aid, delIdList);
		}

		ParamMatcher matcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropEntity.Info.PROP_ID, ParamMatcher.IN, delIdList);
		rt = m_propDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "delPropList error;flow=%d;aid=%d;delIdList=%s", m_flow, aid, delIdList);
		}
	}

	public void clearData(int aid, FaiList<Integer> unionPriIds) {
		int rt;
		if(unionPriIds == null || unionPriIds.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "clearData args error, unionPriIds is null;flow=%d;aid=%d;unionPriIds=%s", m_flow, aid, unionPriIds);
		}
		ParamMatcher matcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		matcher.and(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
		rt = m_propDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "clearData error;flow=%d;aid=%d;unionPriIds=%s", m_flow, aid, unionPriIds);
		}
		// 处理下idBuilder
		m_propDao.restoreMaxId(aid, m_flow, false);
		Log.logStd("clearData ok;flow=%d;aid=%d;unionPriIds=%s;", m_flow, aid, unionPriIds);
	}

	public FaiList<Param> getListFromDao(int aid, SearchArg searchArg, String ... selectFields) {
		// 从db获取数据
		searchArg.matcher.and(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		Ref<FaiList<Param>> listRef = new Ref<>();
		int rt = m_propDao.select(searchArg, listRef, selectFields);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "getList error;flow=%d;aid=%d;", m_flow, aid);
		}
		FaiList<Param> list = listRef.value;
		if(list == null) {
			list = new FaiList<Param>();
		}
		if (list.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;aid=%d;match=%s;", aid, searchArg.matcher.toJson());
		}
		return list;
	}

	private FaiList<Param> getList(int aid) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropCacheCtrl.getCacheList(aid);
		if(!Util.isEmptyList(list)) {
			return list;
		}

		LockUtil.PropLock.readLock(aid);
		try {
			// check again
			list = ProductPropCacheCtrl.getCacheList(aid);
			if(!Util.isEmptyList(list)) {
				return list;
			}

			// 从db获取数据
			SearchArg searchArg = new SearchArg();
			searchArg.matcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
			Ref<FaiList<Param>> listRef = new Ref<>();
			int rt = m_propDao.select(searchArg, listRef);
			if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
				throw new MgException(rt, "getList error;flow=%d;aid=%d;", m_flow, aid);
			}
			list = listRef.value;
			if(list == null) {
				list = new FaiList<Param>();
			}
			if (list.isEmpty()) {
				rt = Errno.NOT_FOUND;
				Log.logDbg(rt, "not found;aid=%d", aid);
				return list;
			}
			// 添加到缓存
			ProductPropCacheCtrl.addCacheList(aid, list);
		}finally {
			LockUtil.PropLock.readUnLock(aid);
		}

		return list;
	}

	private void init(TransactionCtrl tc) {
		if(!tc.register(m_propDao)) {
			throw new MgException("registered ProductPropDaoCtrl err;");
		}
	}

	private int m_flow;
	private ProductPropDaoCtrl m_propDao;
}
