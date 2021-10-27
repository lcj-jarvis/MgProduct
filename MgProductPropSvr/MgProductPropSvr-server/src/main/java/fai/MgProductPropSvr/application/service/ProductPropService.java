package fai.MgProductPropSvr.application.service;

import fai.MgBackupSvr.interfaces.entity.MgBackupEntity;
import fai.MgProductPropSvr.domain.common.LockUtil;
import fai.MgProductPropSvr.domain.common.ProductPropCheck;
import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropRelEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValObj;
import fai.MgProductPropSvr.domain.repository.cache.CacheCtrl;
import fai.MgProductPropSvr.domain.repository.cache.ProductPropCacheCtrl;
import fai.MgProductPropSvr.domain.repository.cache.ProductPropRelCacheCtrl;
import fai.MgProductPropSvr.domain.repository.cache.ProductPropValCacheCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropDaoCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropRelDaoCtrl;
import fai.MgProductPropSvr.domain.repository.dao.ProductPropValDaoCtrl;
import fai.MgProductPropSvr.domain.serviceproc.ProductPropProc;
import fai.MgProductPropSvr.domain.serviceproc.ProductPropRelProc;
import fai.MgProductPropSvr.domain.serviceproc.ProductPropValProc;
import fai.MgProductPropSvr.interfaces.dto.ProductPropDto;
import fai.MgProductPropSvr.interfaces.dto.ProductPropValDto;
import fai.comm.cache.redis.RedisCacheManager;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.app.CloneDef;
import fai.comm.util.*;
import fai.comm.middleground.FaiValObj;
import fai.mgproduct.comm.DataStatus;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.BackupStatusCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class ProductPropService extends ServicePub {

	@SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
	public int addPropInfoWithVal(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, Param info, FaiList<Param> valList) throws IOException {
		int rt = Errno.ERROR;
		if(!FaiValObj.TermId.isValidTid(tid)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}
		if(Str.isEmpty(info)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, info is empty;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}

		Param propInfo = new Param();
		Param relInfo = new Param();
		int propId = 0;

		int rlPropId = 0;

		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			// 统一控制事务
			TransactionCtrl tc = new TransactionCtrl();
			int maxSort = 0;
			boolean commit = false;
			try {
				tc.setAutoCommit(false);
				ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
				// 获取参数中最大的sort
				maxSort = propRelProc.getMaxSort(aid, unionPriId, libId);
				if(maxSort < 0) {
					rt = Errno.ERROR;
					Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
					return rt;
				}
				info.setInt(ProductPropRelEntity.Info.SORT, ++maxSort);
				// 组装数据
				rt = assemblyPropInfo(flow, aid, unionPriId, tid, libId, info, propInfo, relInfo);
				if(rt != Errno.OK) {
					Log.logErr(rt, "assembly prop info error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
					return rt;
				}
				ProductPropCacheCtrl.setExpire(aid);
				ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
				// 添加参数信息
				propId = propProc.addPropInfo(aid, propInfo);
				relInfo.setInt(ProductPropRelEntity.Info.PROP_ID, propId);
				ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
				// 添加参数业务关系数据
				rlPropId = propRelProc.addPropRelInfo(aid, unionPriId, libId, relInfo);

				// 需要新增参数值
				if(valList != null && !valList.isEmpty()) {
					ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc);
					ProductPropValCacheCtrl.InfoCache.setExpire(aid, propId);

					// 新增
					valProc.addValList(aid, propId, valList, false);
				}

				commit = true;
				tc.commit();
				// 缓存处理
				ProductPropCacheCtrl.addCache(aid, propInfo);
				ProductPropRelCacheCtrl.addCache(aid, unionPriId, libId, relInfo);
				if(valList != null && !valList.isEmpty()) {
					ProductPropValCacheCtrl.InfoCache.addCacheListExist(aid, propId, valList);
					ProductPropValCacheCtrl.DataStatusCache.update(aid, valList.size(), true);
				}
				if(maxSort > 0) {
					ProductPropRelCacheCtrl.setSortCache(aid, unionPriId, libId, maxSort);
				}
			}finally {
				if(!commit) {
					tc.rollback();
					ProductPropDaoCtrl.clearIdBuilderCache(aid);
					ProductPropRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
					ProductPropValDaoCtrl.clearIdBuilderCache(aid);
				}
				tc.closeDao();
			}
		}finally {
			lock.unlock();
		}
		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		sendBuf.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropId);
		session.write(sendBuf);
		Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		return rt;
	}

	/**
	 * 批量新增指定商品库的商品参数
	 */
	@SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
	public int addPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<Param> recvInfoList) throws IOException {
		int rt = Errno.ERROR;
		if(!FaiValObj.TermId.isValidTid(tid)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}
		FaiList<Param> propList = new FaiList<Param>();
		FaiList<Param> propRelList = new FaiList<Param>();
		FaiList<Integer> rlPropIds = new FaiList<Integer>();

		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			// 统一控制事务
			TransactionCtrl tc = new TransactionCtrl();

			int maxSort = 0;
			try {
				tc.setAutoCommit(false);
				ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
				maxSort = propRelProc.getMaxSort(aid, unionPriId, libId);
				if(maxSort < 0) {
					rt = Errno.ERROR;
					Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
					return rt;
				}
				// 数据拆分, 要插入两张表，参数业务关系表和参数表
				for(Param info : recvInfoList) {
					Param propInfo = new Param();
					Param relInfo = new Param();
					// 未设置排序则默认排序值+1
					Integer sort = info.getInt(ProductPropRelEntity.Info.SORT);
					if(sort == null) {
						info.setInt(ProductPropRelEntity.Info.SORT, ++maxSort);
					}
					rt = assemblyPropInfo(flow, aid, unionPriId, tid, libId, info, propInfo, relInfo);
					if(rt != Errno.OK) {
						Log.logErr(rt, "assembly prop info error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
						return rt;
					}
					propList.add(propInfo);
					propRelList.add(relInfo);
				}
				if(propList.isEmpty() || propRelList.isEmpty()) {
					rt = Errno.ARGS_ERROR;
					Log.logErr(rt, "insert list is empty;flow=%d;aid=%d;", flow, aid);
					return rt;
				}

				boolean commit = false;
				ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
				try {
					ProductPropCacheCtrl.setExpire(aid);
					FaiList<Integer> propIds = propProc.addPropList(aid, propList);

					for(int i = 0; i < propRelList.size(); i++) {
						Param relInfo = propRelList.get(i);
						relInfo.setInt(ProductPropRelEntity.Info.PROP_ID, propIds.get(i));
					}

					ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
					rlPropIds = propRelProc.addPropRelList(aid, unionPriId, libId, propRelList);

					commit = true;
					tc.commit();
					if(ProductPropCacheCtrl.exists(aid)) {
						ProductPropCacheCtrl.addCacheList(aid, propList);
					}
					if(ProductPropRelCacheCtrl.exists(aid, unionPriId, libId)) {
						ProductPropRelCacheCtrl.addCacheList(aid, unionPriId, libId, propRelList);
					}
					if(maxSort > 0) {
						ProductPropRelCacheCtrl.setSortCache(aid, unionPriId, libId, maxSort);
					}
				} finally {
					if(!commit) {
						tc.rollback();
						ProductPropDaoCtrl.clearIdBuilderCache(aid);
						ProductPropRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
					}
				}
			}finally {
				tc.closeDao();
			}
		}finally {
			lock.unlock();
		}
		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		rlPropIds.toBuffer(sendBuf, ProductPropDto.Key.RL_PROP_IDS);
		session.write(sendBuf);
		Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		return rt;
	}

	/**
	 * 获取指定商品库的商品参数列表
	 */
	@SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
	public int getPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, SearchArg searchArg) throws IOException {
		int rt = Errno.ERROR;
		if(!FaiValObj.TermId.isValidTid(tid)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}
		//统一控制事务
		TransactionCtrl tc = new TransactionCtrl();

		FaiList<Integer> propIds = new FaiList<>();
		HashMap<Integer, Param> propMap = new HashMap<>();
		FaiList<Param> list = new FaiList<>();
		try {

			// 先查参数业务关系表
			ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
			list = propRelProc.getPropRelList(aid, unionPriId, libId);

			// 再查参数表数据
			ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
			FaiList<Param> propList = propProc.getPropList(aid);

			for(int i = 0; i < propList.size(); i++) {
				Param info = propList.get(i);
				Integer propId = info.getInt(ProductPropRelEntity.Info.PROP_ID);
				propIds.add(propId);
				propMap.put(propId, info);
			}
		}finally {
			// 关闭dao
			tc.closeDao();
		}
		// 数据整合
		for(Param info : list) {
			Integer propId = info.getInt(ProductPropRelEntity.Info.PROP_ID);
			Param propInfo = propMap.get(propId);
			if(propInfo == null) {
				Log.logErr(Errno.ERROR, "data error;flow=%d;aid=%d;unionPriId=%d;tid=%d;libId=%d;propId=%d;", flow, aid, unionPriId, tid, libId, propId);
				continue;
			}
			info.assign(propInfo);
		}

		if(searchArg.matcher == null) {
			searchArg.matcher = new ParamMatcher();
		}
		searchArg.matcher.and(ProductPropEntity.Info.PROP_ID, ParamMatcher.IN, propIds);
		if(searchArg.cmpor == null) {
			searchArg.cmpor = new ParamComparator();
			searchArg.cmpor.addKey(ProductPropRelEntity.Info.SORT);
		}

		Searcher searcher = new Searcher(searchArg);
		list = searcher.getParamList(list);

		FaiBuffer sendBuf = new FaiBuffer(true);
		list.toBuffer(sendBuf, ProductPropDto.Key.INFO_LIST, ProductPropDto.getInfoDto());
		if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
			sendBuf.putInt(ProductPropDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
		}
		session.write(sendBuf);
		rt = Errno.OK;
		Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;size=%d;", flow, aid, unionPriId, tid, list.size());
		return rt;
	}

	/**
	 * 批量删除指定商品库的商品参数列表
	 */
	@SuccessRt(value = Errno.OK)
	public int delPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<Integer> rlPropIdList) throws IOException {
		int rt = Errno.ERROR;
		if(!FaiValObj.TermId.isValidTid(tid)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}
		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			//统一控制事务
			TransactionCtrl tc = new TransactionCtrl();

			FaiList<Integer> delPropIdList = null;
			boolean commit = false;
			try {
				tc.setAutoCommit(false);
				// 删除参数业务关系数据
				ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
				delPropIdList = propRelProc.getIdsByRlIds(aid, unionPriId, libId, rlPropIdList);

				propRelProc.delPropList(aid, unionPriId, libId, rlPropIdList);

				// 删除参数数据
				ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
				propProc.delPropList(aid, delPropIdList);

				// 删除参数值数据
				ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc);
				valProc.delValListByPropIds(aid, delPropIdList);

				commit = true;
				tc.commit();
				int delCount = delPropIdList == null ? 0 : delPropIdList.size();
				ProductPropValCacheCtrl.DataStatusCache.update(aid, delCount, false);
			}finally {
				if(!commit) {
					tc.rollback();
				}
				// 删除缓存
				ProductPropCacheCtrl.delCacheList(aid, delPropIdList);
				ProductPropRelCacheCtrl.delCacheList(aid, unionPriId, libId, rlPropIdList);
				ProductPropRelCacheCtrl.delSortCache(aid, unionPriId, libId);
				ProductPropValCacheCtrl.InfoCache.delCacheList(aid, delPropIdList);

				tc.closeDao();
			}
		}finally {
			lock.unlock();
		}
		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;ids=%s;", flow, aid, unionPriId, tid, rlPropIdList);
		return rt;
	}

	/**
	 * 批量删除指定商品参数关联数据
	 */
	@SuccessRt(value = Errno.OK)
	public int clearRelData(FaiSession session, int flow, int aid, int unionPriId) throws IOException {
		int rt;
		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			//统一控制事务
			TransactionCtrl tc = new TransactionCtrl();
			try {
				boolean commit = false;
				tc.setAutoCommit(false);
				try {
					// 删除参数业务关系数据
					ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);

					propRelProc.clearData(aid, unionPriId);

					commit = true;
					tc.commit();
				}finally {
					if(!commit) {
						tc.rollback();
					}
				}
			}finally {
				tc.closeDao();
			}
			// 删除缓存
			CacheCtrl.clearAllCache(aid);
			ProductPropDaoCtrl.clearIdBuilderCache(aid);
			ProductPropRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
		}finally {
			lock.unlock();
		}
		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("clearRelData ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
		return rt;
	}

	/**
	 * 批量删除指定商品参数关联数据
	 */
	@SuccessRt(value = Errno.OK)
	public int clearAcct(FaiSession session, int flow, int aid, FaiList<Integer> unionPriIds) throws IOException {
		int rt;
		if(Util.isEmptyList(unionPriIds)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, unionPriIds is empty;flow=%d;aid=%d;rlPdIds=%s;", flow, aid, unionPriIds);
			return rt;
		}

		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			//统一控制事务
			TransactionCtrl tc = new TransactionCtrl();
			try {
				boolean commit = false;
				tc.setAutoCommit(false);
				try {
					// 删除参数业务关系数据
					ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
					propRelProc.clearData(aid, unionPriIds);

					ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
					SearchArg searchArg = new SearchArg();
					searchArg.matcher = new ParamMatcher(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, unionPriIds);
					FaiList<Param> propList = propProc.getListFromDao(aid, searchArg, ProductPropEntity.Info.PROP_ID);
					// 删除参数数据
					propProc.clearData(aid, unionPriIds);

					FaiList<Integer> propIds = new FaiList<>(propList.size());
					for(Param propInfo : propList) {
						int propId = propInfo.getInt(ProductPropEntity.Info.PROP_ID);
						propIds.add(propId);
					}
					// 删除参数值数据
					if(!propIds.isEmpty()) {
						ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc);
						valProc.delValListByPropIds(aid, propIds);
					}

					commit = true;
					tc.commit();
				}finally {
					if(!commit) {
						tc.rollback();
						ProductPropDaoCtrl.clearIdBuilderCache(aid);
						for(Integer unionPriId : unionPriIds) {
							ProductPropRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
						}
					}
				}
			}finally {
				tc.closeDao();
			}
			// 删除缓存
			CacheCtrl.clearAllCache(aid);
		}finally {
			lock.unlock();
		}
		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("clearAcct ok;flow=%d;aid=%d;unionPriIds=%s;", flow, aid, unionPriIds);
		return rt;
	}

	/**
	 * 批量修改指定商品库的商品参数列表
	 */
	@SuccessRt(value = Errno.OK)
	public int setPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<ParamUpdater> updaterList) throws IOException {
		int rt = Errno.ERROR;
		if(!FaiValObj.TermId.isValidTid(tid)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}
		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			//统一控制事务
			TransactionCtrl tc = new TransactionCtrl();

			boolean commit = false;
			FaiList<ParamUpdater> propUpdaterList = new FaiList<ParamUpdater>();
			try {
				tc.setAutoCommit(false);
				//修改参数业务关系表
				ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
				ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
				propRelProc.setPropList(aid, unionPriId, libId, updaterList, propUpdaterList);
				// 修改参数表
				if(!propUpdaterList.isEmpty()) {
					ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
					ProductPropCacheCtrl.setExpire(aid);
					propProc.setPropList(aid, propUpdaterList);
				}

				commit = true;
				tc.commit();
				// 更新缓存
				ProductPropCacheCtrl.updateCacheList(aid, propUpdaterList);
				ProductPropRelCacheCtrl.updateCacheList(aid, unionPriId, libId, updaterList);
			}finally {
				if(!commit) {
					tc.rollback();
				}
				tc.closeDao();
			}
		}finally {
			lock.unlock();
		}
		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		return rt;
	}

	/**
	 * 合并 商品参数 （增、删、改）接口
	 */
	@SuccessRt(value = Errno.OK)
	public int unionSetPropList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<Param> addList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delList) throws IOException {
		int rt;
		if (!FaiValObj.TermId.isValidTid(tid)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}
		FaiList<Param> propList = new FaiList<>();
		FaiList<Param> propRelList = new FaiList<>();
		FaiList<Integer> rlPropIds = new FaiList<>();
		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			//统一控制事务
			TransactionCtrl tc = new TransactionCtrl();

			boolean commit = false;
			int maxSort = 0;
			FaiList<ParamUpdater> propUpdaterList = new FaiList<>();
			FaiList<Integer> delPropIdList = new FaiList<>();
			try {
				tc.setAutoCommit(false);
				ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);

				// 处理delList
				if (delList != null && !delList.isEmpty()) {
					// 删除参数业务关系数据
					delPropIdList = propRelProc.getIdsByRlIds(aid, unionPriId, libId, delList);

					propRelProc.delPropList(aid, unionPriId, libId, delList);

					// 删除参数数据
					ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
					propProc.delPropList(aid, delPropIdList);

					// 删除参数值数据
					ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc);
					valProc.delValListByPropIds(aid, delPropIdList);
				}

				// 处理updaterList
				if (updaterList != null && !updaterList.isEmpty()) {
					//修改参数业务关系表
					propRelProc.setPropList(aid, unionPriId, libId, updaterList, propUpdaterList);
					// 修改参数表
					if(!propUpdaterList.isEmpty()) {
						ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
						propProc.setPropList(aid, propUpdaterList);
					}
				}

				// 处理addList
				if (addList != null && !addList.isEmpty()) {
					maxSort = addPropList(flow, aid, unionPriId, tid, libId, tc, addList, propList, propRelList, rlPropIds);
				}

				commit = true;
				tc.commit();

				// 处理缓存
				if (delList != null && !delList.isEmpty()) {
					int delCount = delPropIdList == null ? 0 : delPropIdList.size();
					ProductPropValCacheCtrl.DataStatusCache.update(aid, delCount, false);
					ProductPropCacheCtrl.delCacheList(aid, delPropIdList);
					ProductPropRelCacheCtrl.delCacheList(aid, unionPriId, libId, delList);
					ProductPropRelCacheCtrl.delSortCache(aid, unionPriId, libId);
					ProductPropValCacheCtrl.InfoCache.delCacheList(aid, delPropIdList);
				}

				if (updaterList != null && !updaterList.isEmpty()) {
					ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
					// 更新缓存
					ProductPropCacheCtrl.updateCacheList(aid, propUpdaterList);
					ProductPropRelCacheCtrl.updateCacheList(aid, unionPriId, libId, updaterList);
				}

				if (addList != null && !addList.isEmpty()) {
					ProductPropCacheCtrl.setExpire(aid);
					ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
					ProductPropCacheCtrl.addCacheList(aid, propList);
					ProductPropRelCacheCtrl.addCacheList(aid, unionPriId, libId, propRelList);
					if(maxSort > 0) {
						ProductPropRelCacheCtrl.setSortCache(aid, unionPriId, libId, maxSort);
					}
				}
			} finally {
				if(!commit) {
					tc.rollback();
					ProductPropDaoCtrl.clearIdBuilderCache(aid);
					ProductPropRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
				}
				tc.closeDao();
			}
		} finally {
			lock.unlock();
		}
		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		rlPropIds.toBuffer(sendBuf, ProductPropDto.Key.RL_PROP_IDS);
		session.write(sendBuf);
		Log.logStd("unionSetPropList ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		return rt;
	}

	/**
	 * 获取指定商品参数的参数值列表
	 */
	@SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
	public int getPropValList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<Integer> rlPropIds) throws IOException {
		int rt;
		if(!FaiValObj.TermId.isValidTid(tid)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
			return rt;
		}
		//统一控制事务
		TransactionCtrl tc = new TransactionCtrl();

		FaiList<Param> list = new FaiList<>();
		try {
			ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
			HashMap<Integer, Integer> propIdList = propRelProc.getIdsWithRlIds(aid, unionPriId, libId, rlPropIds);
			if(propIdList == null) {
				rt = Errno.ERROR;
				Log.logErr(rt, "getIdsByRlIds error;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
				return rt;
			}

			ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc);
			list = valProc.getListByPropIds(aid, propIdList);
		}finally {
			tc.closeDao();
		}

		// 按照sort字段排序
		ParamComparator comp = new ParamComparator(ProductPropValEntity.Info.SORT);
		Collections.sort(list, comp);
		FaiBuffer sendBuf = new FaiBuffer(true);
		list.toBuffer(sendBuf, ProductPropValDto.Key.INFO_LIST, ProductPropValDto.getInfoDto());
		session.write(sendBuf);
		rt = Errno.OK;
		Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		return rt;
	}

	/**
	 * 批量修改(包括增、删、改)指定商品库的商品参数值列表
	 */
	@SuccessRt(value = Errno.OK)
	public int setPropValList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, int rlPropId, FaiList<ParamUpdater> updaterList, FaiList<Integer> delValIds, FaiList<Param> addInfoList) throws IOException {
		int rt = Errno.ERROR;
		// 先根据参数业务id获取参数id
		FaiList<Integer> rlProIds = new FaiList<Integer>();
		rlProIds.add(rlPropId);
		int propId = 0;
		TransactionCtrl tc = new TransactionCtrl();
		try {
			ProductPropRelProc relProc = new ProductPropRelProc(flow, aid, tc);
			FaiList<Integer> proIds = relProc.getIdsByRlIds(aid, unionPriId, libId, rlProIds);
			if(proIds == null || proIds.isEmpty()) {
				rt = Errno.ARGS_ERROR;
				Log.logErr(rt, "product prop is not exist;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlPropId=%d;", flow, aid, unionPriId, tid, rlPropId);
				return rt;
			}
			propId = proIds.get(0);
		} finally {
			tc.closeDao();
		}

		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			boolean commit = false;
			try {
				tc.setAutoCommit(false);
				ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc);
				ProductPropValCacheCtrl.InfoCache.setExpire(aid, propId);
				// 修改
				valProc.setValList(aid, propId, updaterList);

				int addCount = 0;
				// 删除
				if(!Util.isEmptyList(delValIds)) {
					valProc.delValList(aid, propId, delValIds);
					addCount -= delValIds.size();
				}

				// 新增
				if(addInfoList != null && !addInfoList.isEmpty()) {
					valProc.addValList(aid, propId, addInfoList, false);
					addCount += addInfoList.size();
				}

				commit = true;
				tc.commit();
				// 清掉参数值缓存
				ProductPropValCacheCtrl.InfoCache.delCache(aid, propId);
				ProductPropValCacheCtrl.DataStatusCache.update(aid, addCount, true);
			}finally {
				if(!commit) {
					tc.rollback();
					ProductPropValDaoCtrl.clearIdBuilderCache(aid);
				}
				tc.closeDao();
			}
		}finally {
			lock.unlock();
		}

		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);

		rt = Errno.OK;
		Log.logStd("setPropValList ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		return rt;
	}

	@SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
	public int getPropValDataStatus(FaiSession session, int flow, int aid) throws IOException {
		int rt;
		if(aid <= 0) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
			return rt;
		}
		Param info;
		TransactionCtrl transactionCtrl = new TransactionCtrl();
		try {
			ProductPropValProc valProc = new ProductPropValProc(flow, aid, transactionCtrl);
			info = valProc.getDataStatus(aid);
		}finally {
			transactionCtrl.closeDao();
		}
		FaiBuffer sendBuf = new FaiBuffer(true);
		info.toBuffer(sendBuf, ProductPropValDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
		session.write(sendBuf);
		rt = Errno.OK;
		Log.logDbg("getPropRelDataStatus ok;flow=%d;aid=%d;", flow, aid);
		return rt;
	}

	@SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
	public int getAllPropValData(FaiSession session, int flow, int aid) throws IOException {
		int rt;
		if(aid <= 0) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
			return rt;
		}
		FaiList<Param> list;
		TransactionCtrl transactionCtrl = new TransactionCtrl();
		try {
			// 查aid下所有数据，传入空的searchArg
			SearchArg searchArg = new SearchArg();
			ProductPropValProc valProc = new ProductPropValProc(flow, aid, transactionCtrl);
			list = valProc.searchFromDb(aid, searchArg, null);

		}finally {
			transactionCtrl.closeDao();
		}
		FaiBuffer sendBuf = new FaiBuffer(true);
		list.toBuffer(sendBuf, ProductPropValDto.Key.INFO_LIST, ProductPropValDto.getInfoDto());
		session.write(sendBuf);
		rt = Errno.OK;
		Log.logDbg("get list ok;flow=%d;aid=%d;size=%d;", flow, aid, list.size());

		return rt;
	}

	@SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
	public int searchPropValFromDb(FaiSession session, int flow, int aid, SearchArg searchArg) throws IOException {
		int rt;
		if(aid <= 0) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
			return rt;
		}
		FaiList<Param> list;
		TransactionCtrl transactionCtrl = new TransactionCtrl();
		try {
			ProductPropValProc valProc = new ProductPropValProc(flow, aid, transactionCtrl);
			list = valProc.searchFromDb(aid, searchArg, null);

		}finally {
			transactionCtrl.closeDao();
		}
		FaiBuffer sendBuf = new FaiBuffer(true);
		list.toBuffer(sendBuf, ProductPropValDto.Key.INFO_LIST, ProductPropValDto.getInfoDto());
		if (searchArg != null && searchArg.totalSize != null && searchArg.totalSize.value != null) {
			sendBuf.putInt(ProductPropValDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
		}
		session.write(sendBuf);
		rt = Errno.OK;
		Log.logDbg("search from db ok;flow=%d;aid=%d;size=%d;", flow, aid, list.size());

		return rt;
	}

	@SuccessRt(value = Errno.OK)
	public int clearCache(FaiSession session, int flow, int aid) throws IOException {
		int rt;
		if(aid <= 0) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
			return rt;
		}

		Lock lock = LockUtil.getLock(aid);
		lock.lock();
		try {
			// 更新缓存数据版本号
			CacheCtrl.clearAllCache(aid);
		}finally {
			lock.unlock();
		}
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		rt = Errno.OK;
		Log.logStd("clear cache ok;flow=%d;aid=%d;", flow, aid);

		return rt;
	}

	@SuccessRt(value = Errno.OK)
	public int backupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPirIds, Param backupInfo) throws IOException {
		int rt;
		if(aid <= 0 || Str.isEmpty(backupInfo)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
			return rt;
		}

		int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
		int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
		if(backupId == 0 || backupFlag == 0) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, backupInfo error;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
			return rt;
		}

		LockUtil.BackupLock.lock(aid);
		try {
			String backupStatus = backupStatusCtrl.getStatus(BackupStatusCtrl.Action.BACKUP, aid, backupId);
			if(backupStatus != null) {
				if(backupStatusCtrl.isDoing(backupStatus)) {
					rt = Errno.ALREADY_EXISTED;
					throw new MgException(rt, "backup is doing;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
				}else if(backupStatusCtrl.isFinish(backupStatus)) {
					rt = Errno.OK;
					Log.logStd(rt, "backup is already ok;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
					FaiBuffer sendBuf = new FaiBuffer(true);
					session.write(sendBuf);
					return Errno.OK;
				}else if(backupStatusCtrl.isFail(backupStatus)) {
					rt = Errno.ALREADY_EXISTED;
					Log.logStd(rt, "backup is fail, going retry;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
				}
			}
			// 设置备份执行中
			backupStatusCtrl.setStatusIsDoing(BackupStatusCtrl.Action.BACKUP, aid, backupId);

			TransactionCtrl tc = new TransactionCtrl();
			ProductPropRelProc relProc = new ProductPropRelProc(flow, aid, tc, true);
			ProductPropProc proc = new ProductPropProc(flow, aid, tc, true);
			ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc, true);
			boolean commit = false;
			try {
				tc.setAutoCommit(false);

				// 可能之前备份没有成功，先操作删除之前的备份
				deleteBackupData(relProc, proc, valProc, aid, backupId, backupFlag);

				// 备份业务关系表数据
				relProc.backupData(aid, unionPirIds, backupId, backupFlag);

				// 备份参数表数据
				Set<Integer> propIds = proc.backupData(aid, backupId, backupFlag, unionPirIds);

				// 备份参数值表数据
				valProc.backupData(aid, backupId, backupFlag, propIds);

				commit = true;
			}finally {
				if(commit) {
					tc.commit();
					backupStatusCtrl.setStatusIsFinish(BackupStatusCtrl.Action.BACKUP, aid, backupId);
				}else {
					tc.rollback();
					backupStatusCtrl.setStatusIsFail(BackupStatusCtrl.Action.BACKUP, aid, backupId);
				}
				tc.closeDao();
			}
		}finally {
			LockUtil.BackupLock.unlock(aid);
		}

		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("backupData ok;flow=%d;aid=%d;unionPirIds=%s;backupInfo=%s;", flow, aid, unionPirIds, backupInfo);
		return rt;
	}

	@SuccessRt(Errno.OK)
	public int restoreBackupData(FaiSession session, int flow, int aid, FaiList<Integer> unionPirIds, int restoreId, Param backupInfo) throws IOException {
		int rt;
		if(aid <= 0 || Str.isEmpty(backupInfo)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
			return rt;
		}

		int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
		int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
		if(backupId == 0 || backupFlag == 0) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, backupInfo error;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
			return rt;
		}

		LockUtil.BackupLock.lock(aid);
		try {
			String backupStatus = backupStatusCtrl.getStatus(BackupStatusCtrl.Action.RESTORE, aid, restoreId);
			if(backupStatus != null) {
				if(backupStatusCtrl.isDoing(backupStatus)) {
					rt = Errno.ALREADY_EXISTED;
					throw new MgException(rt, "restore is doing;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
				}else if(backupStatusCtrl.isFinish(backupStatus)) {
					rt = Errno.OK;
					Log.logStd(rt, "restore is already ok;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
					FaiBuffer sendBuf = new FaiBuffer(true);
					session.write(sendBuf);
					return Errno.OK;
				}else if(backupStatusCtrl.isFail(backupStatus)) {
					rt = Errno.ALREADY_EXISTED;
					Log.logStd(rt, "restore is fail, going retry;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
				}
			}
			// 设置备份执行中
			backupStatusCtrl.setStatusIsDoing(BackupStatusCtrl.Action.RESTORE, aid, restoreId);

			TransactionCtrl tc = new TransactionCtrl();
			ProductPropRelProc relProc = new ProductPropRelProc(flow, aid, tc, true);
			ProductPropProc proc = new ProductPropProc(flow, aid, tc, true);
			ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc, true);
			boolean commit = false;
			try {
				tc.setAutoCommit(false);

				// 还原关系表数据
				relProc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

				// 还原参数表数据
				FaiList<Integer> propIds = proc.restoreBackupData(aid, unionPirIds, backupId, backupFlag);

				// 还原参数值表数据
				valProc.restoreBackupData(aid, propIds, backupId, backupFlag);

				commit = true;
			}finally {
				if(commit) {
					tc.commit();
					backupStatusCtrl.setStatusIsFinish(BackupStatusCtrl.Action.RESTORE, aid, restoreId);
				}else {
					tc.rollback();
					backupStatusCtrl.setStatusIsFail(BackupStatusCtrl.Action.RESTORE, aid, restoreId);
				}
				tc.closeDao();
			}
		}finally {
			LockUtil.BackupLock.unlock(aid);
		}

		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("restore backupData ok;flow=%d;aid=%d;unionPirIds=%s;restoreId=%s;backupInfo=%s;", flow, aid, unionPirIds, restoreId, backupInfo);
		return rt;
	}

	@SuccessRt(Errno.OK)
	public int delBackupData(FaiSession session, int flow, int aid, Param backupInfo) throws IOException {
		int rt;
		if(aid <= 0 || Str.isEmpty(backupInfo)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
			return rt;
		}

		int backupId = backupInfo.getInt(MgBackupEntity.Info.ID, 0);
		int backupFlag = backupInfo.getInt(MgBackupEntity.Info.BACKUP_FLAG, 0);
		if(backupId == 0 || backupFlag == 0) {
			rt = Errno.ARGS_ERROR;
			Log.logErr("args error, backupInfo error;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
			return rt;
		}

		LockUtil.BackupLock.lock(aid);
		try {
			String backupStatus = backupStatusCtrl.getStatus(BackupStatusCtrl.Action.DELETE, aid, backupId);
			if(backupStatus != null) {
				if(backupStatusCtrl.isDoing(backupStatus)) {
					rt = Errno.ALREADY_EXISTED;
					throw new MgException(rt, "del backup is doing;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
				}else if(backupStatusCtrl.isFinish(backupStatus)) {
					rt = Errno.OK;
					Log.logStd(rt, "del backup is already ok;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
					FaiBuffer sendBuf = new FaiBuffer(true);
					session.write(sendBuf);
					return Errno.OK;
				}else if(backupStatusCtrl.isFail(backupStatus)) {
					rt = Errno.ALREADY_EXISTED;
					Log.logStd(rt, "del backup is fail, going retry;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
				}
			}
			// 设置备份执行中
			backupStatusCtrl.setStatusIsDoing(BackupStatusCtrl.Action.DELETE, aid, backupId);

			TransactionCtrl tc = new TransactionCtrl();
			ProductPropRelProc relProc = new ProductPropRelProc(flow, aid, tc, true);
			ProductPropProc proc = new ProductPropProc(flow, aid, tc, true);
			ProductPropValProc valProc = new ProductPropValProc(flow, aid, tc, true);
			boolean commit = false;
			try {
				tc.setAutoCommit(false);

				// 删除备份
				deleteBackupData(relProc, proc, valProc, aid, backupId, backupFlag);

				commit = true;
			}finally {
				if(commit) {
					tc.commit();
					backupStatusCtrl.setStatusIsFinish(BackupStatusCtrl.Action.DELETE, aid, backupId);
				}else {
					tc.rollback();
					backupStatusCtrl.setStatusIsFail(BackupStatusCtrl.Action.DELETE, aid, backupId);
				}
				tc.closeDao();
			}
		}finally {
			LockUtil.BackupLock.unlock(aid);
		}

		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("del backupData ok;flow=%d;aid=%d;backupInfo=%s;", flow, aid, backupInfo);
		return rt;
	}

	@SuccessRt(value = Errno.OK)
	public int cloneData(FaiSession session, int flow, int aid, int fromAid, FaiList<Param> cloneUnionPriIds) throws IOException {
		int rt;

		doClone(flow, aid, fromAid, cloneUnionPriIds, false);

		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, aid, fromAid, cloneUnionPriIds);
		return rt;
	}

	@SuccessRt(value = Errno.OK)
	public int incrementalClone(FaiSession session, int flow, int toAid, int toUnionPriId, int fromAid, int fromUnionPriId) throws IOException {
		int rt;

		Param clonePrimary = new Param();
		clonePrimary.setInt(CloneDef.Info.FROM_PRIMARY_KEY, fromUnionPriId);
		clonePrimary.setInt(CloneDef.Info.TO_PRIMARY_KEY, toUnionPriId);
		FaiList<Param> cloneUnionPriIds = new FaiList<>();
		cloneUnionPriIds.add(clonePrimary);

		doClone(flow, toAid, fromAid, cloneUnionPriIds, true);

		rt = Errno.OK;
		FaiBuffer sendBuf = new FaiBuffer(true);
		session.write(sendBuf);
		Log.logStd("incrementalClone ok;flow=%d;toAid=%d;toUid=%d;fromAid=%d;fromUid=%d;", flow, toAid, toUnionPriId, fromAid, fromUnionPriId);
		return rt;
	}

	private void deleteBackupData(ProductPropRelProc relProc, ProductPropProc proc, ProductPropValProc valProc, int aid, int backupId, int backupFlag) {

		relProc.delBackupData(aid, backupId, backupFlag);

		proc.delBackupData(aid, backupId, backupFlag);

		valProc.delBackupData(aid, backupId, backupFlag);
	}

	private void doClone(int flow, int toAid, int fromAid, FaiList<Param> cloneUnionPriIds, boolean incrementalClone) {
		int rt;
		if(Utils.isEmptyList(cloneUnionPriIds)) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "args error, cloneUnionPriIds is empty;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
		}

		// 组装 fromUnionPriId -> toUnionPriId 映射关系
		Map<Integer, Integer> cloneUidMap = new HashMap<>();
		for(Param cloneUidInfo : cloneUnionPriIds) {
			Integer toUnionPriId = cloneUidInfo.getInt(CloneDef.Info.TO_PRIMARY_KEY);
			Integer fromUnionPriId = cloneUidInfo.getInt(CloneDef.Info.FROM_PRIMARY_KEY);
			if(toUnionPriId == null || fromUnionPriId == null) {
				rt = Errno.ARGS_ERROR;
				throw new MgException(rt, "args error, cloneUnionPriIds is error;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
			}
			cloneUidMap.put(fromUnionPriId, toUnionPriId);
		}

		Lock lock = LockUtil.getLock(toAid);
		lock.lock();
		try {
			boolean commit = false;
			TransactionCtrl tc = new TransactionCtrl();
			tc.setAutoCommit(false);
			try {
				ProductPropRelProc relProc = new ProductPropRelProc(flow, toAid, tc);
				ProductPropProc propProc = new ProductPropProc(flow, toAid, tc);
				ProductPropValProc valProc = new ProductPropValProc(flow, toAid, tc);

				if(!incrementalClone) {
					FaiList<Integer> toUnionPriIds = new FaiList<>(cloneUidMap.values());
					// 删除原有的业务关系表数据
					ParamMatcher delMatcher = new ParamMatcher(ProductPropRelEntity.Info.AID, ParamMatcher.EQ, toAid);
					delMatcher.and(ProductPropRelEntity.Info.UNION_PRI_ID, ParamMatcher.IN, toUnionPriIds);
					relProc.delPropRelList(toAid, delMatcher);

					// 删除原有的参数表数据
					delMatcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, toAid);
					delMatcher.and(ProductPropEntity.Info.SOURCE_UNIONPRIID, ParamMatcher.IN, toUnionPriIds);

					SearchArg searchArg = new SearchArg();
					searchArg.matcher = delMatcher;
					// 先获取要删除的propId
					FaiList<Param> oldPropList = propProc.searchFromDb(toAid, searchArg, ProductPropEntity.Info.PROP_ID);
					FaiList<Integer> delPropIds = Utils.getValList(oldPropList, ProductPropEntity.Info.PROP_ID);
					// 删除原有的参数表数据
					propProc.delPropList(toAid, delMatcher);

					// 删除原有的参数值表数据
					FaiList<FaiList<Integer>> delPropIdProps = Utils.splitList(delPropIds, 1000);
					for(FaiList<Integer> curPropIds : delPropIdProps) {
						valProc.delValListByPropIds(toAid, curPropIds);
					}
				}

				// propId -> addRelList
				Map<Integer, FaiList<Param>> propId_RelList = new HashMap<>();
				// propId -> sourceUnionPriId
				Map<Integer, Integer> propId_SourceUid = new HashMap<>();
				// 查出要克隆的业务关系表数据
				for(Map.Entry<Integer, Integer> entry: cloneUidMap.entrySet()) {
					int fromUnionPriId = entry.getKey();
					int toUnionPriId = entry.getValue();
					FaiList<Param> fromRelList = relProc.searchFromDB(fromAid, fromUnionPriId, null);

					if(fromRelList.isEmpty()) {
						continue;
					}

					// 查出已存在的数据 for 增量克隆
					FaiList<Param> existedList = null;
					if(incrementalClone) {
						existedList = relProc.searchFromDB(toAid, toUnionPriId, null);
					}
					for(Param fromInfo : fromRelList) {
						int rlPropId = fromInfo.getInt(ProductPropRelEntity.Info.RL_PROP_ID);
						// 如果是增量克隆，但是rlPropId已存在，则跳过
						if(incrementalClone && Misc.getFirst(existedList, ProductPropRelEntity.Info.RL_PROP_ID, rlPropId) != null) {
							continue;
						}
						Param data = fromInfo.clone();
						data.setInt(ProductPropRelEntity.Info.AID, toAid);
						data.setInt(ProductPropRelEntity.Info.UNION_PRI_ID, toUnionPriId);
						int propId = data.getInt(ProductPropRelEntity.Info.PROP_ID);
						FaiList<Param> addList = propId_RelList.get(propId);
						if(addList == null) {
							addList = new FaiList<>();
							propId_RelList.put(propId, addList);
							propId_SourceUid.put(propId, toUnionPriId);
						}
						addList.add(data);
					}

				}

				// 没有要克隆的数据
				if(propId_RelList.isEmpty()) {
					return;
				}

				// 根据 fromAid 和 propId 查出对应的分类表数据
				SearchArg propSearch = new SearchArg();
				propSearch.matcher = new ParamMatcher(ProductPropEntity.Info.AID, ParamMatcher.EQ, fromAid);
				propSearch.matcher.and(ProductPropEntity.Info.PROP_ID, ParamMatcher.IN, new FaiList<>(propId_RelList.keySet()));

				FaiList<Param> fromPropList = propProc.searchFromDb(fromAid, propSearch);
				// 这里必定是会查到数据才对
				if(fromPropList.isEmpty()) {
					rt = Errno.ERROR;
					throw new MgException(rt, "get from prop list err;flow=%d;aid=%d;fromAid=%d;matcher=%s;", flow, toAid, fromAid, propSearch.matcher.toJson());
				}

				// 查出from参数值数据
				FaiList<Param> fromPropValList = valProc.searchFromDb(fromAid, propSearch, null);

				// 这里 保持 fromPropIds 和 addPropList的顺序一致，是为了后面能得到fromPropId 和 toPropId的映射关系
				FaiList<Integer> fromPropIds = new FaiList<>();
				FaiList<Param> addPropList = new FaiList<>();
				for(Param fromInfo : fromPropList) {
					Param data = fromInfo.clone();
					data.setInt(ProductPropEntity.Info.AID, toAid);
					int fromPropId = (Integer) data.remove(ProductPropEntity.Info.PROP_ID);
					int sourceUnionPriId = propId_SourceUid.get(fromPropId);
					data.setInt(ProductPropEntity.Info.SOURCE_UNIONPRIID, sourceUnionPriId);
					fromPropIds.add(fromPropId);
					addPropList.add(data);
				}

				Map<Integer, Integer> fromPropId_toPropId = new HashMap<>();
				FaiList<Integer> propIds = propProc.addPropList(toAid, addPropList);
				// 组装fromPropId 和 toPropId的映射关系
				for(int i = 0; i < propIds.size(); i++) {
					fromPropId_toPropId.put(fromPropIds.get(i), propIds.get(i));
				}

				// 组装业务关系表增量克隆数据，设置toPropId
				FaiList<Param> addRelList = new FaiList<>();
				for(Integer fromPropId : propId_RelList.keySet()) {
					FaiList<Param> tmpAddList = propId_RelList.get(fromPropId);
					for(Param relInfo : tmpAddList) {
						int propId = fromPropId_toPropId.get(fromPropId);
						relInfo.setInt(ProductPropRelEntity.Info.PROP_ID, propId);
						addRelList.add(relInfo);
					}
				}
				// 插入业务关系表克隆数据
				relProc.insert4Clone(toAid, new FaiList<>(cloneUidMap.values()), addRelList);

				// 插入参数值表数据
				FaiList<Param> addValList = new FaiList<>();
				for(Param valInfo : fromPropValList) {
					int fromPropId = valInfo.getInt(ProductPropValEntity.Info.PROP_ID);
					int toPropId = fromPropId_toPropId.get(fromPropId);
					valInfo.setInt(ProductPropValEntity.Info.AID, toAid);
					valInfo.setInt(ProductPropValEntity.Info.PROP_ID, toPropId);
					valInfo.remove(ProductPropValEntity.Info.PROP_VAL_ID);
					addValList.add(valInfo);
				}
				valProc.batchInsert(toAid, addValList, false);

				commit = true;
			}finally {
				if(commit) {
					tc.commit();
				}else {
					tc.rollback();
				}
				tc.closeDao();
			}
		}finally {
			lock.unlock();
		}

		Log.logStd("cloneData ok;flow=%d;aid=%d;fromAid=%d;uids=%s;", flow, toAid, fromAid, cloneUnionPriIds);
	}

	private int addPropList(int flow, int aid, int unionPriId, int tid, int libId, TransactionCtrl tc, FaiList<Param> addList, FaiList<Param> propList, FaiList<Param> propRelList, FaiList<Integer> rlPropIds) {
		int rt;
		ProductPropRelProc propRelProc = new ProductPropRelProc(flow, aid, tc);
		int maxSort = propRelProc.getMaxSort(aid, unionPriId, libId);
		if(maxSort < 0) {
			rt = Errno.ERROR;
			throw new MgException(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
		}
		// 数据拆分, 要插入两张表，参数业务关系表和参数表
		for(Param info : addList) {
			Param propInfo = new Param();
			Param relInfo = new Param();
			// 未设置排序则默认排序值+1
			Integer sort = info.getInt(ProductPropRelEntity.Info.SORT);
			if(sort == null) {
				info.setInt(ProductPropRelEntity.Info.SORT, ++maxSort);
			}
			rt = assemblyPropInfo(flow, aid, unionPriId, tid, libId, info, propInfo, relInfo);
			if(rt != Errno.OK) {
				throw new MgException(rt, "assembly prop info error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
			}
			propList.add(propInfo);
			propRelList.add(relInfo);
		}
		if(propList.isEmpty() || propRelList.isEmpty()) {
			rt = Errno.ARGS_ERROR;
			throw new MgException(rt, "insert list is empty;flow=%d;aid=%d;", flow, aid);
		}

		ProductPropProc propProc = new ProductPropProc(flow, aid, tc);
		FaiList<Integer> propIds = propProc.addPropList(aid, propList);

		for(int i = 0; i < propRelList.size(); i++) {
			Param relInfo = propRelList.get(i);
			relInfo.setInt(ProductPropRelEntity.Info.PROP_ID, propIds.get(i));
		}

		rlPropIds.addAll(propRelProc.addPropRelList(aid, unionPriId, libId, propRelList));

		return maxSort;
	}


	private int assemblyPropInfo(int flow, int aid, int unionPriId, int tid, int libId, Param recvInfo, Param propInfo, Param relInfo) {
		int rt = Errno.OK;
		String name = recvInfo.getString(ProductPropEntity.Info.NAME, "");
		if(!ProductPropCheck.isNameValid(name)) {
			rt = Errno.ARGS_ERROR;
			Log.logErr(rt, "name is not valid;flow=%d;aid=%d;name=%d;", flow, aid, name);
			return rt;
		}
		int type = recvInfo.getInt(ProductPropEntity.Info.TYPE, ProductPropValObj.Type.DEFAULT);
		int flag = recvInfo.getInt(ProductPropEntity.Info.FLAG, 0);
		Calendar now = Calendar.getInstance();
		Calendar createTime = recvInfo.getCalendar(ProductPropRelEntity.Info.CREATE_TIME, now);
		Calendar updateTime = recvInfo.getCalendar(ProductPropRelEntity.Info.UPDATE_TIME, now);

		// 参数表数据
		propInfo.setInt(ProductPropEntity.Info.AID, aid);
		propInfo.setInt(ProductPropEntity.Info.SOURCE_TID, tid);
		propInfo.setInt(ProductPropEntity.Info.SOURCE_UNIONPRIID, unionPriId);
		propInfo.setString(ProductPropEntity.Info.NAME, name);
		propInfo.setInt(ProductPropEntity.Info.TYPE, type);
		propInfo.setInt(ProductPropEntity.Info.FLAG, flag);
		propInfo.setCalendar(ProductPropEntity.Info.CREATE_TIME, createTime);
		propInfo.setCalendar(ProductPropEntity.Info.UPDATE_TIME, updateTime);

		// 参数业务关系表数据
		int sort = recvInfo.getInt(ProductPropRelEntity.Info.SORT, 0);
		int rlFlag = recvInfo.getInt(ProductPropRelEntity.Info.RL_FLAG, 0);
		relInfo.setInt(ProductPropRelEntity.Info.AID, aid);
		relInfo.setInt(ProductPropRelEntity.Info.UNION_PRI_ID, unionPriId);
		relInfo.setInt(ProductPropRelEntity.Info.RL_LIB_ID, libId);
		relInfo.setCalendar(ProductPropRelEntity.Info.CREATE_TIME, createTime);
		relInfo.setCalendar(ProductPropRelEntity.Info.UPDATE_TIME, updateTime);
		relInfo.setInt(ProductPropRelEntity.Info.SORT, sort);
		relInfo.setInt(ProductPropRelEntity.Info.RL_FLAG, rlFlag);
		return rt;
	}

	public void initBackupStatus(RedisCacheManager cache) {
		backupStatusCtrl = new BackupStatusCtrl(BAK_TYPE, cache);
	}

	private BackupStatusCtrl backupStatusCtrl;
	private static final String BAK_TYPE = "mgPdProp";
}
