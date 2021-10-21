package fai.MgProductPropSvr.domain.serviceproc;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductPropSvr.domain.common.LockUtil;
import fai.MgProductPropSvr.domain.entity.ProductPropRelEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropRelValObj;
import fai.MgProductPropSvr.domain.repository.cache.ProductPropRelCacheCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropRelBakDaoCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropRelDaoCtrl;
import fai.MgProductPropSvr.interfaces.entity.ProductPropValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.util.*;

public class ProductPropRelProc {
	public ProductPropRelProc(int flow, int aid, TransactionCtrl tc) {
		this(flow, aid, tc, false);
	}

	public ProductPropRelProc(int flow, int aid, TransactionCtrl tc, boolean useBak) {
		this.m_flow = flow;
		this.m_relDao = ProductPropRelDaoCtrl.getInstance(flow, aid);
		if(useBak) {
			this.m_bakDao = ProductPropRelBakDaoCtrl.getInstance(flow, aid);
		}
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

		delPropRelList(aid, matcher);
	}

	public void delPropRelList(int aid, ParamMatcher matcher) {
		int rt;
		if(matcher == null || matcher.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "args err, matcher is null;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher);
		}

		rt = m_relDao.delete(matcher);
		if(rt != Errno.OK){
			throw new MgException(rt, "delPropList error;flow=%d;aid=%d;matcher=%s", m_flow, aid, matcher.toJson());
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

	public FaiList<Param> searchFromDB(int aid, int unionPriId, SearchArg searchArg) {
		if(searchArg == null) {
			searchArg = new SearchArg();
		}
		if(searchArg.matcher == null) {
			searchArg.matcher = new ParamMatcher();
		}
		searchArg.matcher.and(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		searchArg.matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);

		Ref<FaiList<Param>> listRef = new Ref<>();
		// 因为克隆可能获取其他aid的数据，所以根据传进来的aid设置tablename
		m_relDao.setTableName(aid);
		int rt = m_relDao.select(searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "get error;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
		}
		// 查完之后恢复之前的tablename
		m_relDao.restoreTableName();
		if (listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;flow=%d;aid=%d;unionPriId=%d;", m_flow, aid, unionPriId);
		}
		return listRef.value;
	}

	private FaiList<Param> getList(int aid, int unionPriId, int libId) {
		// 从缓存获取数据
		FaiList<Param> list = ProductPropRelCacheCtrl.getCacheList(aid, unionPriId, libId);
		if(!Utils.isEmptyList(list)) {
			return list;
		}

		LockUtil.PropRelLock.readLock(aid);
		try {
			// check again
			list = ProductPropRelCacheCtrl.getCacheList(aid, unionPriId, libId);
			if(!Utils.isEmptyList(list)) {
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

		if(Utils.isEmptyList(list)) {
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

	public void backupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
		int rt;
		if(m_bakDao.isAutoCommit()) {
			rt = Errno.ERROR;
			throw new MgException(rt, "bakDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		}
		if(Utils.isEmptyList(unionPriIds)) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "uids is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		}
		for(int unionPriId : unionPriIds) {
			FaiList<Param> fromList = searchFromDB(aid, unionPriId, null);
			if(fromList.isEmpty()) {
				continue;
			}

			Set<String> newBakUniqueKeySet = new HashSet<>((int) (fromList.size() / 0.75f) + 1); // 初始容量直接定为所需的最大容量，去掉不必要的扩容
			FaiList<Calendar> updateTimeList = new FaiList<>();
			for (Param fromInfo : fromList) {
				fromInfo.setInt(MgBackupEntity.Comm.BACKUP_ID, backupId);
				newBakUniqueKeySet.add(getBakUniqueKey(fromInfo));
				updateTimeList.add(fromInfo.getCalendar(ProductPropRelEntity.Info.UPDATE_TIME));
			}

			SearchArg oldBakArg = new SearchArg();
			oldBakArg.matcher = new ParamMatcher(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
			oldBakArg.matcher = new ParamMatcher(ProductPropRelEntity.Info.UPDATE_TIME, ParamMatcher.IN, updateTimeList);
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
				ParamMatcher mergeMatcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, "?");
				mergeMatcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, "?");
				mergeMatcher.and(ProductPropRelEntity.Info.PROP_ID, ParamMatcher.EQ, "?");
				mergeMatcher.and(ProductPropRelEntity.Info.UPDATE_TIME, ParamMatcher.EQ, "?");

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
					data.setInt(ProductPropRelEntity.Info.AID, aid);
					data.setInt(ProductPropRelEntity.Info.UNION_PRI_ID, Integer.valueOf(keys[0]));
					data.setInt(ProductPropRelEntity.Info.PROP_ID, Integer.valueOf(keys[1]));
					data.setCalendar(ProductPropRelEntity.Info.UPDATE_TIME, updateTime);
					// mergeMatcher end

					dataList.add(data);
				}
				rt = m_bakDao.doBatchUpdate(mergeUpdater, mergeMatcher, dataList, false);
				if(rt != Errno.OK) {
					throw new MgException(rt, "merge bak update err;aid=%d;uid=%s;backupId=%d;backupFlag=%d;", aid, unionPriId, backupId, backupFlag);
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
				continue;
			}
			// 批量插入备份表
			rt = m_bakDao.batchInsert(fromList);
			if(rt != Errno.OK) {
				throw new MgException(rt, "batchInsert bak err;aid=%d;uid=%s;backupId=%d;backupFlag=%d;", aid, unionPriId, backupId, backupFlag);
			}
		}
		Log.logStd("backupData ok;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
	}

	public void delBackupData(int aid, int backupId, int backupFlag) {
		ParamMatcher updateMatcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID, ParamMatcher.GE, 0);
		updateMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);

		// 先将 backupFlag 对应的备份数据取消置起
		ParamUpdater updater = new ParamUpdater(MgBackupEntity.Comm.BACKUP_ID_FLAG, backupFlag, false);
		int rt = m_bakDao.update(updater, updateMatcher);
		if(rt != Errno.OK) {
			throw new MgException("do update err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		// 删除 backupIdFlag 为0的数据，backupIdFlag为0 说明没有一个现存备份关联到了这个数据
		ParamMatcher delMatcher = new ParamMatcher(MgBackupEntity.Info.AID, ParamMatcher.EQ, aid);
		delMatcher.and(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.EQ, 0);
		rt = m_bakDao.delete(delMatcher);
		if(rt != Errno.OK) {
			throw new MgException("do del err;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
		}

		Log.logStd("del rel bak ok;aid=%d;backupId=%d;backupFlag=%d;", aid, backupId, backupFlag);
	}

	public void restoreBackupData(int aid, FaiList<Integer> unionPriIds, int backupId, int backupFlag) {
		int rt;
		if(m_relDao.isAutoCommit()) {
			rt = Errno.ERROR;
			throw new MgException(rt, "relDao is auto commit;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		}
		if(Util.isEmptyList(unionPriIds)) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "uids is empty;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
		}

		// 先删除原表数据
		ParamMatcher delMatcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);
		delMatcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
		delPropRelList(aid, delMatcher);

		// 查出备份数据
		SearchArg bakSearchArg = new SearchArg();
		bakSearchArg.matcher = new ParamMatcher(MgBackupEntity.Comm.BACKUP_ID_FLAG, ParamMatcher.LAND, backupFlag, backupFlag);
		bakSearchArg.matcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, unionPriIds);
		FaiList<Param> fromList = searchBakList(aid, bakSearchArg);
		for(Param fromInfo : fromList) {
			fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID);
			fromInfo.remove(MgBackupEntity.Comm.BACKUP_ID_FLAG);
		}

		if(!fromList.isEmpty()) {
			// 批量插入
			rt = m_relDao.batchInsert(fromList);
			if(rt != Errno.OK) {
				throw new MgException(rt, "restore insert err;aid=%d;uids=%s;backupId=%d;backupFlag=%d;", aid, unionPriIds, backupId, backupFlag);
			}
		}

		// 处理idBuilder
		for(int unionPriId : unionPriIds) {
			m_relDao.restoreMaxId(aid, unionPriId, m_flow, false);
			m_relDao.clearIdBuilderCache(aid, unionPriId);
		}
	}

	public FaiList<Param> searchBakList(int aid, SearchArg searchArg) {
		if(searchArg == null) {
			searchArg = new SearchArg();
		}
		if(searchArg.matcher == null) {
			searchArg.matcher = new ParamMatcher();
		}
		searchArg.matcher.and(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, aid);

		Ref<FaiList<Param>> listRef = new Ref<>();
		int rt = m_bakDao.select(searchArg, listRef);
		if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
			throw new MgException(rt, "get error;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
		}
		if (listRef.value.isEmpty()) {
			rt = Errno.NOT_FOUND;
			Log.logDbg(rt, "not found;flow=%d;aid=%d;matcher=%s;", m_flow, aid, searchArg.matcher.toJson());
		}
		return listRef.value;
	}

	private void init(TransactionCtrl tc) {
		if(!tc.register(m_relDao)) {
			throw new MgException("registered ProductPropRelDaoCtrl err;");
		}
		if(m_bakDao != null && !tc.register(m_bakDao)) {
			throw new MgException("registered ProductPropRelBakDaoCtrl err;");
		}
	}

	private static String getBakUniqueKey(Param fromInfo) {
		return fromInfo.getInt(ProductPropRelEntity.Info.UNION_PRI_ID) +
				DELIMITER +
				fromInfo.getInt(ProductPropRelEntity.Info.PROP_ID) +
				DELIMITER +
				fromInfo.getCalendar(ProductPropRelEntity.Info.UPDATE_TIME).getTimeInMillis();
	}

	private final static String DELIMITER = "-";

	private int m_flow;
	private ProductPropRelDaoCtrl m_relDao;
	private ProductPropRelBakDaoCtrl m_bakDao;
}
