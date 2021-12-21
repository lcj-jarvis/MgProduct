package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductPropSvr.domain.common.LockUtil;
import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValObj;
import fai.MgProductPropSvr.domain.repository.cache.ProductPropValCacheCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropValBakDaoCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropValDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ProductPropValProc {

	public ProductPropValProc(int flow, int aid, TransactionCtrl tc) {
		this.m_flow = flow;
		this.m_valDao = ProductPropValDaoCtrl.getInstance(flow, aid);
		init(tc);
	}

	public ProductPropValProc(int flow, int aid, TransactionCtrl tc, boolean useBak) {
		this.m_flow = flow;
		this.m_valDao = ProductPropValDaoCtrl.getInstance(flow, aid);
		if(useBak) {
			this.m_bakDao = ProductPropValBakDaoCtrl.getInstance(flow, aid);
		}
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
			if(Utils.isEmptyList(tmpList)) {
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
		Log.logStd("del ok;flow=%d;aid=%d;propIds=%s;", m_flow, aid, propIds);
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

	public void batchInsert(int aid, FaiList<Param> valList, boolean needLock) {
		int rt;
		if(valList == null || valList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "valList is null;flow=%d;aid=%d;", m_flow, aid);
		}
		// 数据
		for(int i = 0; i < valList.size(); i++) {
			int valId = m_valDao.buildId(aid, needLock);
			Param info  = valList.get(i);
			info.setInt(ProductPropValEntity.Info.PROP_VAL_ID, valId);
		}
		rt = m_valDao.batchInsert(valList.clone(), null, true);
		if(rt != Errno.OK) {
			throw new MgException(rt, "batch insert prop val error;flow=%d;aid=%d;", m_flow, aid);
		}
	}

	public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, FaiList<String> fields) {
		if(searchArg == null) {
			searchArg = new SearchArg();
		}
		if(searchArg.matcher == null) {
			searchArg.matcher = new ParamMatcher();
		}
		searchArg.matcher.and(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);

		Ref<FaiList<Param>> listRef = new Ref<>();
		int rt = m_valDao.select(searchArg, listRef, fields);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
		}
		if(listRef.value == null) {
			listRef.value = new FaiList<Param>();
		}
		if (listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
		}
		return listRef.value;
	}

	public FaiList<Param> searchBakList(int aid, SearchArg searchArg) {
		if(searchArg == null) {
			searchArg = new SearchArg();
		}
		if(searchArg.matcher == null) {
			searchArg.matcher = new ParamMatcher();
		}
		searchArg.matcher.and(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);

		Ref<FaiList<Param>> listRef = new Ref<>();
		int rt = m_bakDao.select(searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
		}
		if (listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
		}
		return listRef.value;
	}

	public Param getDataStatus(int aid) {
		Param statusInfo = ProductPropValCacheCtrl.DataStatusCache.get(aid);
		if(!Str.isEmpty(statusInfo)) {
			return statusInfo;
		}

		long now = System.currentTimeMillis();
		statusInfo = new Param();
		int totalSize = getTotalSize(aid);
		statusInfo.setInt(DataStatus.Info.TOTAL_SIZE, totalSize);
		statusInfo.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, now);
		statusInfo.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, 0L);

		ProductPropValCacheCtrl.DataStatusCache.add(aid, statusInfo);
		return statusInfo;
	}

	private int getTotalSize(int aid) {
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		Ref<Integer> countRef = new Ref<>();
		int rt = m_valDao.selectCount(searchArg, countRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "get propVal count error;flow=%d;aid=%d;", m_flow, aid);
		}
		if(countRef.value == null) {
			countRef.value = 0;
		}
		return countRef.value;
	}

	private FaiList<Param> getList(int aid, int propId) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropValCacheCtrl.InfoCache.getCacheList(aid, propId);
		if(!Utils.isEmptyList(list)) {
			return list;
		}

		LockUtil.PropValLock.readLock(aid);
		try {
			// check again
			list = ProductPropValCacheCtrl.InfoCache.getCacheList(aid, propId);
			if(!Utils.isEmptyList(list)) {
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
			if (Utils.isEmptyList(list)) {
				rt = Errno.NOT_FOUND;
				Log.logDbg(rt, "not found;aid=%d", aid);
				return list;
			}
			// 添加到缓存
			ProductPropValCacheCtrl.InfoCache.addCacheList(aid, propId, list);
		}finally {
			LockUtil.PropValLock.readUnLock(aid);
		}

		return list;
	}

	public void backupData(int aid, int backupId, int backupFlag, Set<Integer> bakPropIds) {
		int rt;
		if(m_bakDao.isAutoCommit()) {
			rt = Errno.ERROR;
			throw new MgException(rt, "bakDao is auto commit;aid=%d;bakPropIds=%s;backupId=%d;backupFlag=%d;", aid, bakPropIds, backupId, backupFlag);
		}
		if(Utils.isEmptyList(bakPropIds)) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "bak bakPropIds is empty;aid=%d;bakPropIds=%s;backupId=%d;backupFlag=%d;", aid, bakPropIds, backupId, backupFlag);
		}
		FaiList<Param> fromList = new FaiList<>();
		FaiList<Param> oldBakList = new FaiList<>();
		Set<String> newBakUniqueKeySet = new HashSet<>();
		FaiList<FaiList<Integer>> propIdGroups =  Utils.splitList(new FaiList<>(bakPropIds), 1000);
		for(FaiList<Integer> propIds : propIdGroups) {
			SearchArg searchArg = new SearchArg();
			searchArg.matcher = new ParamMatcher(ProductPropValEntity.Info.PROP_ID, ParamMatcher.IN, propIds);
			FaiList<Param> list = searchFromDb(aid, searchArg, null);
			fromList.addAll(list);

			FaiList<Calendar> updateTimeList = new FaiList<>();
			for (Param fromInfo : list) {
				fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
				newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
				updateTimeList.add(fromInfo.getCalendar(ProductPropValEntity.Info.UPDATE_TIME));
			}

			// 查出已有的备份数据，通过updateTime确定数据是否已备份
			SearchArg oldBakArg = new SearchArg();
			oldBakArg.matcher = searchArg.matcher.clone();
			oldBakArg.matcher.and(ProductPropValEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
			FaiList<Param> bakList = searchBakList(aid, oldBakArg);
			oldBakList.addAll(bakList);
		}

		Set<String> oldBakUniqueKeySet = new HashSet<String>((int)(oldBakList.size()/0.75f)+1);
		for (Param oldBak : oldBakList) {
			oldBakUniqueKeySet.add(getBakUniqueKey(oldBak));
		}
		// 获取交集，说明剩下的这些是要合并的备份数据
		oldBakUniqueKeySet.retainAll(newBakUniqueKeySet);
		if(!oldBakUniqueKeySet.isEmpty()){
			// 合并标记
			ParamUpdater mergeUpdater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, true);

			// 合并条件
			ParamMatcher mergeMatcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, "?");
			mergeMatcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.EQ, "?");
			mergeMatcher.and(ProductPropValEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, "?");
			mergeMatcher.and(ProductPropValEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

			FaiList<Param> dataList = new FaiList<Param>();
			for (String bakUniqueKey : oldBakUniqueKeySet) {
				String[] keys = bakUniqueKey.split(DELIMITER);
				Calendar updateTime = Calendar.getInstance();
				updateTime.setTimeInMillis(Long.valueOf(keys[2]));
				Param data = new Param();

				// mergeUpdater start
				data.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
				// mergeUpdater end

				// mergeMatcher start
				data.setInt(ProductPropValEntity.Info.AID, aid);
				data.setInt(ProductPropValEntity.Info.PROP_ID, Integer.valueOf(keys[0]));
				data.setInt(ProductPropValEntity.Info.PROP_VAL_ID, Integer.valueOf(keys[1]));
				data.setCalendar(ProductPropValEntity.Info.UPDATE_TIME, updateTime);
				// mergeMatcher end

				dataList.add(data);
			}
			rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
			if(rt != Errno.OK) {
				throw new MgException(rt, "merge bak update err;aid=%d;bakPropIds=%s;backupId=%d;backupFlag=%d;", aid, bakPropIds, backupId, backupFlag);
			}
		}

		// 移除掉合并的数据，剩下的就是需要新增的备份数据
		newBakUniqueKeySet.removeAll(oldBakUniqueKeySet);

		for (int j = fromList.size(); --j >= 0;) {
			Param formInfo = fromList.get(j);
			if(newBakUniqueKeySet.contains(getBakUniqueKey(formInfo))){
				// 置起当前备份标识
				formInfo.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
				continue;
			}
			fromList.remove(j);
		}

		if(fromList.isEmpty()) {
			Log.logStd("backupData ok, need add bak empty;aid=%d;bakPropIds=%s;backupId=%d;backupFlag=%d;", aid, bakPropIds, backupId, backupFlag);
			return;
		}

		// 批量插入备份表
		rt = m_bakDao.batchInsert(fromList);
		if(rt != Errno.OK) {
			throw new MgException(rt, "batchInsert bak err;aid=%d;bakPropIds=%s;backupId=%d;backupFlag=%d;", aid, bakPropIds, backupId, backupFlag);
		}

		Log.logStd("backupData ok;aid=%d;bakPropIds=%s;backupId=%d;backupFlag=%d;", aid, bakPropIds, backupId, backupFlag);
	}

	public void delBackupData(int aid, int backupId, int backupFlag) {
		ParamMatcher updateMatcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
		updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

		// 先将 backupFlag 对应的备份数据取消置起
		ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
		int rt = m_bakDao.update(updater, updateMatcher);
		if(rt != Errno.OK) {
			throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		// 删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
		ParamMatcher delMatcher = new ParamMatcher(ProductPropValEntity.Info.AID, ParamMatcher.EQ, aid);
		delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
		rt = m_bakDao.delete(delMatcher);
		if(rt != Errno.OK) {
			throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		Log.logStd("delete ok;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
	}

	public void restoreBackupData(int aid, FaiList<Integer> propIds, int backupId, int backupFlag) {
		int rt;
		if(m_valDao.isAutoCommit()) {
			rt = Errno.ERROR;
			throw new MgException(rt, "dao is auto commit;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		// 先删除原表数据
		FaiList<FaiList<Integer>> propIdGroups = Utils.splitList(propIds, 1000);
		for(FaiList<Integer> curPropIds : propIdGroups) {
			delValListByPropIds(aid, curPropIds);
		}

		// 查出备份数据
		SearchArg bakSearchArg = new SearchArg();
		bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
		FaiList<Param> fromList = searchBakList(aid, bakSearchArg);

		for(Param fromInfo : fromList) {
			fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
			fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
		}

		if(!fromList.isEmpty()) {
			// 批量插入
			rt = m_valDao.batchInsert(fromList);
			if(rt != Errno.OK) {
				throw new MgException(rt, "restore insert err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
			}
		}

		// 处理idBuilder
		m_valDao.restoreMaxId(aid, m_flow, false);
		m_valDao.clearIdBuilderCache(aid);
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

		if(m_bakDao != null && !tc.register(m_bakDao)) {
			throw new MgException("registered ProductPropValBakDaoCtrl err;");
		}
	}

	private static String getBakUniqueKey(Param fromInfo) {
		return fromInfo.getInt(ProductPropValEntity.Info.PROP_ID) +
				DELIMITER +
				fromInfo.getInt(ProductPropValEntity.Info.PROP_VAL_ID) +
				DELIMITER +
				fromInfo.getCalendar(ProductPropValEntity.Info.UPDATE_TIME).getTimeInMillis();
	}

	private final static String DELIMITER = "-";

	private int m_flow;
	private ProductPropValDaoCtrl m_valDao;
	private ProductPropValBakDaoCtrl m_bakDao;
}
