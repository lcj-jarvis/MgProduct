package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductPropSvr.domain.common.LockUtil;
import fai.MgProductPropSvr.domain.common.ProductPropCheck;
import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValObj;
import fai.MgProductPropSvr.domain.repository.cache.ProductPropCacheCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropBakDaoCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropDaoCtrl;
import fai.comm.util.*;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class ProductPropProc {

	public ProductPropProc(int flow, int aid, TransactionCtrl tc) {
		this(flow, aid, tc, false);
	}

	public ProductPropProc(int flow, int aid, TransactionCtrl tc, boolean useBak) {
		this.m_flow = flow;
		this.m_propDao = ProductPropDaoCtrl.getInstance(flow, aid);
		if(useBak) {
			this.m_bakDao = ProductPropBakDaoCtrl.getInstance(flow, aid);
		}
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
		int sourceUnionPriId = info.getInt(ProductPropEntity.Info.SOURCE_UNIONPRIID);
		String name = info.getString(ProductPropEntity.Info.NAME);
		ParamMatcher existMatcher = new ParamMatcher(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.EQ, sourceUnionPriId);
		existMatcher.and(ProductPropEntity.Info.NAME, ParamMatcher.EQ, name);
		Param existInfo = Misc.getFirst(list, existMatcher);
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
			int sourceUnionPriId = info.getInt(ProductPropEntity.Info.SOURCE_UNIONPRIID);
			String name = info.getString(ProductPropEntity.Info.NAME);
			ParamMatcher existMatcher = new ParamMatcher(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.EQ, sourceUnionPriId);
			existMatcher.and(ProductPropEntity.Info.NAME, ParamMatcher.EQ, name);
			Param existInfo = Misc.getFirst(list, existMatcher);
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
		if(Utils.isEmptyList(oldInfoList)) {
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

	public void delPropList(int aid, ParamMatcher matcher) {
		int rt;
		if(matcher == null || matcher.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "matcher is null;aid=%d;", aid);
		}
		matcher.and(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);

		rt = m_propDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "delPropList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
		}
		Log.logStd("delPropList ok;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
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
		if(!Utils.isEmptyList(list)) {
			return list;
		}

		LockUtil.PropLock.readLock(aid);
		try {
			// check again
			list = ProductPropCacheCtrl.getCacheList(aid);
			if(!Utils.isEmptyList(list)) {
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

	public FaiList<Param> searchFromDb(int aid, SearchArg searchArg, String ... selectFields) {
		if(searchArg == null) {
			searchArg = new SearchArg();
		}
		if(searchArg.matcher == null) {
			searchArg.matcher = new ParamMatcher();
		}
		searchArg.matcher.and(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);

		Ref<FaiList<Param>> listRef = new Ref<>();
		// 因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tableName
		m_propDao.setTableName(aid);
		int rt = m_propDao.select(searchArg, listRef, selectFields);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
		}
		// 查完之后恢复最初的tableName
		m_propDao.restoreTableName();
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
		searchArg.matcher.and(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);

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

	public Set<Integer> backupData(int aid, int backupId, int backupFlag, FaiList<Integer> unionPriIds) {
		int rt;
		if(m_bakDao.isAutoCommit()) {
			rt = Errno.ERROR;
			throw new MgException(rt, "bakDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		}
		if(Utils.isEmptyList(unionPriIds)) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "bak propIds is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		}
		Set<Integer> propIds = new HashSet<>();
		SearchArg searchArg = new SearchArg();
		searchArg.matcher = new ParamMatcher(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
		FaiList<Param> fromList = searchFromDb(aid, searchArg);

		Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1); // 初始容量直接定为所需的最大容量，去掉不必要的扩容
		FaiList<Calendar> updateTimeList = new FaiList<>();
		for (Param fromInfo : fromList) {
			fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
			newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
			updateTimeList.add(fromInfo.getCalendar(ProductPropEntity.Info.UPDATE_TIME));
			propIds.add(fromInfo.getInt(ProductPropEntity.Info.PROP_ID));
		}

		// 查出已有的备份数据，通过updateTime确定数据是否已备份
		SearchArg oldBakArg = new SearchArg();
		oldBakArg.matcher = searchArg.matcher.clone();
		oldBakArg.matcher.and(ProductPropEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
		FaiList<Param> oldBakList = searchBakList(aid, oldBakArg);

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
			ParamMatcher mergeMatcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, "?");
			mergeMatcher.and(ProductPropEntity.Info.PROP_ID, ParamMatcher.EQ, "?");
			mergeMatcher.and(ProductPropEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

			FaiList<Param> dataList = new FaiList<Param>();
			for (String bakUniqueKey : oldBakUniqueKeySet) {
				String[] keys = bakUniqueKey.split(DELIMITER);
				Calendar updateTime = Calendar.getInstance();
				updateTime.setTimeInMillis(Long.valueOf(keys[1]));
				Param data = new Param();

				// mergeUpdater start
				data.setInt(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag);
				// mergeUpdater end

				// mergeMatcher start
				data.setInt(ProductPropEntity.Info.AID, aid);
				data.setInt(ProductPropEntity.Info.PROP_ID, Integer.valueOf(keys[0]));
				data.setCalendar(ProductPropEntity.Info.UPDATE_TIME, updateTime);
				// mergeMatcher end

				dataList.add(data);
			}
			rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
			if(rt != Errno.OK) {
				throw new MgException(rt, "merge bak update err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
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
			Log.logStd("backupData ok, need add bak empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
			return propIds;
		}

		// 批量插入备份表
		rt = m_bakDao.batchInsert(fromList);
		if(rt != Errno.OK) {
			throw new MgException(rt, "batchInsert bak err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		}

		Log.logStd("backupData ok;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		return propIds;
	}

	public void delBackupData(int aid, int backupId, int backupFlag) {
		ParamMatcher updateMatcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
		updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

		// 先将 backupFlag 对应的备份数据取消置起
		ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
		int rt = m_bakDao.update(updater, updateMatcher);
		if(rt != Errno.OK) {
			throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		// 删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
		ParamMatcher delMatcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
		rt = m_bakDao.delete(delMatcher);
		if(rt != Errno.OK) {
			throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		Log.logStd("delete ok;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
	}

	public FaiList<Integer> restoreBackupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
		int rt;
		if(m_propDao.isAutoCommit()) {
			rt = Errno.ERROR;
			throw new MgException(rt, "dao is auto commit;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		// 先删除原表数据
		ParamMatcher delMatcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, aid);
		delMatcher.and(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);

		SearchArg searchArg = new SearchArg();
		searchArg.matcher = delMatcher;
		FaiList<Param> oldList = searchFromDb(aid, searchArg, ProductPropEntity.Info.PROP_ID);
		FaiList<Integer> delPropIds = Utils.getValList(oldList, ProductPropEntity.Info.PROP_ID);

		delPropList(aid, delMatcher);

		// 查出备份数据
		SearchArg bakSearchArg = new SearchArg();
		bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
		bakSearchArg.matcher.and(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
		FaiList<Param> fromList = searchBakList(aid, bakSearchArg);

		for(Param fromInfo : fromList) {
			fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
			fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
		}

		if(!fromList.isEmpty()) {
			// 批量插入
			rt = m_propDao.batchInsert(fromList);
			if(rt != Errno.OK) {
				throw new MgException(rt, "restore insert err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
			}
		}

		// 处理idBuilder
		m_propDao.restoreMaxId(aid, m_flow, false);
		m_propDao.clearIdBuilderCache(aid);

		return delPropIds;
	}

	private void init(TransactionCtrl tc) {
		if(!tc.register(m_propDao)) {
			throw new MgException("registered ProductPropDaoCtrl err;");
		}
		if(m_bakDao != null && !tc.register(m_bakDao)) {
			throw new MgException("registered ProductPropBakDaoCtrl err;");
		}
	}

	private static String getBakUniqueKey(Param fromInfo) {
		return fromInfo.getInt(ProductPropEntity.Info.PROP_ID) +
				DELIMITER +
				fromInfo.getCalendar(ProductPropEntity.Info.UPDATE_TIME).getTimeInMillis();
	}

	private final static String DELIMITER = "-";

	private int m_flow;
	private ProductPropDaoCtrl m_propDao;
	private ProductPropBakDaoCtrl m_bakDao;
}
