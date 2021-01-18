package fai.MgProductPropSvr.application.service;

import fai.MgProductPropSvr.domain.common.LockUtil;
import fai.MgProductPropSvr.domain.common.ProductPropCheck;
import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropRelEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.MgProductPropSvr.domain.entity.ProductPropValObj;
import fai.MgProductPropSvr.domain.repository.*;
import fai.MgProductPropSvr.domain.serviceproc.ProductPropProc;
import fai.MgProductPropSvr.domain.serviceproc.ProductPropRelProc;
import fai.MgProductPropSvr.domain.serviceproc.ProductPropValProc;
import fai.MgProductPropSvr.interfaces.dto.ProductPropDto;
import fai.MgProductPropSvr.interfaces.dto.ProductPropValDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.comm.middleground.FaiValObj;
import fai.comm.middleground.repository.TransactionCtrl;
import fai.comm.middleground.service.ServicePub;

import java.io.IOException;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;

public class ProductPropService extends ServicePub {

	public int addPropInfoWithVal(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, Param info, FaiList<Param> valList) throws IOException {
		int rt = Errno.ERROR;
		Oss.SvrStat stat = new Oss.SvrStat(flow);
		try {
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
				ProductPropDaoCtrl propDao = ProductPropDaoCtrl.getInstance(session);
				ProductPropRelDaoCtrl relDao = ProductPropRelDaoCtrl.getInstance(session);
				ProductPropValDaoCtrl valDao = ProductPropValDaoCtrl.getInstance(session);
				// 统一控制事务
				TransactionCtrl transactionCtrl = new TransactionCtrl();
				transactionCtrl.register(propDao);
				transactionCtrl.register(relDao);
				if(valList != null && !valList.isEmpty()) {
					transactionCtrl.register(valDao);
				}
				int maxSort = 0;
				transactionCtrl.setAutoCommit(false);
				try {
					ProductPropRelProc propRelProc = new ProductPropRelProc(flow, relDao);
					// 获取参数中最大的sort
					maxSort = propRelProc.getMaxSort(aid, unionPriId, libId);
					if(maxSort < 0) {
						rt = Errno.ERROR;
						Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
						return rt;
					}
					info.setInt(ProductPropRelEntity.Info.SORT, ++maxSort);
					// 组装数据
					rt = assemblyPropInfo(flow, aid, unionPriId, tid, libId, propDao, relDao, info, propInfo, relInfo);
					if(rt != Errno.OK) {
						Log.logErr(rt, "assembly prop info error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
						return rt;
					}
					rlPropId = relInfo.getInt(ProductPropRelEntity.Info.RL_PROP_ID);
					propId = relInfo.getInt(ProductPropEntity.Info.PROP_ID);
					ProductPropCacheCtrl.setExpire(aid);
					ProductPropProc propProc = new ProductPropProc(flow, propDao);
					// 添加参数信息
					rt = propProc.addPropInfo(aid, propInfo);
					if(rt != Errno.OK) {
						Log.logErr(rt, "batch insert prop error;flow=%d;aid=%d;", flow, aid);
						return rt;
					}

					ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
					// 添加参数业务关系数据
					rt = propRelProc.addPropRelInfo(aid, unionPriId, libId, relInfo);
					if(rt != Errno.OK) {
						Log.logErr(rt, "batch insert prop rel error;flow=%d;aid=%d;", flow, aid);
						return rt;
					}

					// 需要新增参数值
					if(valList != null && !valList.isEmpty()) {
						ProductPropValProc valProc = new ProductPropValProc(flow, valDao);
						ProductPropValCacheCtrl.setExpire(aid, propId);

						// 新增
						rt = valProc.addValList(aid, propId, valList, false);
						if(rt != Errno.OK) {
							Log.logErr(rt, "add prop val list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;valList=%d;", flow, unionPriId, tid, valList);
							return rt;
						}
					}

				}finally {
					if(rt != Errno.OK) {
						transactionCtrl.rollback();
						propDao.clearIdBuilderCache(aid);
						relDao.clearIdBuilderCache(aid, unionPriId);
						valDao.clearIdBuilderCache(aid);
					}else {
						transactionCtrl.commit();
						// 缓存处理
						ProductPropCacheCtrl.addCache(aid, propInfo);
						ProductPropRelCacheCtrl.addCache(aid, unionPriId, libId, relInfo);
						if(valList != null && !valList.isEmpty() && ProductPropValCacheCtrl.exists(aid, propId)) {
							ProductPropValCacheCtrl.addCacheList(aid, propId, valList);
						}
						if(maxSort > 0) {
							ProductPropRelCacheCtrl.setSortCache(aid, unionPriId, libId, maxSort);
						}
					}
					transactionCtrl.closeDao();
				}
			}finally {
				lock.unlock();
			}
			rt = Errno.OK;
			FaiBuffer sendBuf = new FaiBuffer(true);
			sendBuf.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropId);
			session.write(sendBuf);
			Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		}finally {
			stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
		}
		return rt;
	}

	/**
	 * 批量新增指定商品库的商品参数
	 */
	public int addPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<Param> recvInfoList) throws IOException {
		int rt = Errno.ERROR;
		Oss.SvrStat stat = new Oss.SvrStat(flow);
		try {
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
				ProductPropDaoCtrl propDao = ProductPropDaoCtrl.getInstance(session);
				ProductPropRelDaoCtrl relDao = ProductPropRelDaoCtrl.getInstance(session);
				// 统一控制事务
				TransactionCtrl transactionCtrl = new TransactionCtrl();
				transactionCtrl.register(propDao);
				transactionCtrl.register(relDao);
				transactionCtrl.setAutoCommit(false);

				int maxSort = 0;
				try {
					ProductPropRelProc propRelProc = new ProductPropRelProc(flow, relDao);
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
						info.setInt(ProductPropRelEntity.Info.SORT, ++maxSort);
						rt = assemblyPropInfo(flow, aid, unionPriId, tid, libId, propDao, relDao, info, propInfo, relInfo);
						if(rt != Errno.OK) {
							Log.logErr(rt, "assembly prop info error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
							return rt;
						}
						int rlPropId = relInfo.getInt(ProductPropRelEntity.Info.RL_PROP_ID);
						rlPropIds.add(rlPropId);
						propList.add(propInfo);
						propRelList.add(relInfo);
					}
					if(propList.isEmpty() || propRelList.isEmpty()) {
						rt = Errno.ARGS_ERROR;
						Log.logErr(rt, "insert list is empty;flow=%d;aid=%d;", flow, aid);
						return rt;
					}

					try {
						ProductPropCacheCtrl.setExpire(aid);
						ProductPropProc propProc = new ProductPropProc(flow, propDao);
						rt = propProc.addPropList(aid, propList.clone());
						if(rt != Errno.OK) {
							Log.logErr(rt, "batch insert prop error;flow=%d;aid=%d;", flow, aid);
							return rt;
						}

						ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
						rt = propRelProc.addPropRelList(aid, unionPriId, libId, propRelList.clone());
						if(rt != Errno.OK) {
							Log.logErr(rt, "batch insert prop rel error;flow=%d;aid=%d;", flow, aid);
							return rt;
						}
					} finally {
						if(rt != Errno.OK) {
							transactionCtrl.rollback();
							propDao.clearIdBuilderCache(aid);
							relDao.clearIdBuilderCache(aid, unionPriId);
						}else {
							transactionCtrl.commit();
							if(ProductPropCacheCtrl.exists(aid)) {
								ProductPropCacheCtrl.addCacheList(aid, propList);
							}
							if(ProductPropRelCacheCtrl.exists(aid, unionPriId, libId)) {
								ProductPropRelCacheCtrl.addCacheList(aid, unionPriId, libId, propRelList);
							}
							if(maxSort > 0) {
								ProductPropRelCacheCtrl.setSortCache(aid, unionPriId, libId, maxSort);
							}
						}
					}
				}finally {
					transactionCtrl.closeDao();
				}
			}finally {
				lock.unlock();
			}
			rt = Errno.OK;
			FaiBuffer sendBuf = new FaiBuffer(true);
			rlPropIds.toBuffer(sendBuf, ProductPropDto.Key.RL_PROP_IDS);
			session.write(sendBuf);
			Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		}finally {
			stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
		}
		return rt;
	}

	/**
	 * 获取指定商品库的商品参数列表
	 */
	public int getPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, SearchArg searchArg) throws IOException {
		int rt = Errno.ERROR;
		Oss.SvrStat stat = new Oss.SvrStat(flow);
		try {
			if(!FaiValObj.TermId.isValidTid(tid)) {
				rt = Errno.ARGS_ERROR;
				Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
				return rt;
			}
			//统一控制事务
			TransactionCtrl transactionCtrl = new TransactionCtrl();

			FaiList<Integer> propIds = new FaiList<Integer>();
			HashMap<Integer, Param> propMap = new HashMap<Integer, Param>();
			Ref<FaiList<Param>> relListRef = new Ref<FaiList<Param>>();
			try {
				ProductPropDaoCtrl propDao = ProductPropDaoCtrl.getInstance(session);
				ProductPropRelDaoCtrl relDao = ProductPropRelDaoCtrl.getInstance(session);
				transactionCtrl.register(propDao);
				transactionCtrl.register(relDao);

				// 先查参数业务关系表
				ProductPropRelProc propRelProc = new ProductPropRelProc(flow, relDao);
				rt = propRelProc.getPropRelList(aid, unionPriId, libId, relListRef);
				if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
					return rt;
				}

				Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
				ProductPropProc propProc = new ProductPropProc(flow, propDao);
				rt = propProc.getPropList(aid, listRef);
				if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
					return rt;
				}

				for(int i = 0; i < listRef.value.size(); i++) {
					Param info = listRef.value.get(i);
					Integer propId = info.getInt(ProductPropRelEntity.Info.PROP_ID);
					propIds.add(propId);
					propMap.put(propId, info);
				}
			}finally {
				// 关闭dao
				transactionCtrl.closeDao();
			}
			FaiList<Param> list = relListRef.value;
			// 数据整合
			for(Param info : list) {
				Integer propId = info.getInt(ProductPropRelEntity.Info.PROP_ID);
				Param propInfo = propMap.get(propId);
				if(propInfo == null) {
					rt = Errno.ERROR;
					Log.logErr(rt, "data error;flow=%d;aid=%d;unionPriId=%d;tid=%d;libId=%d;propId=%d;", flow, aid, unionPriId, tid, libId, propId);
					return rt;
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
		}finally {
			stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
		}
		return rt;
	}

	/**
	 * 批量删除指定商品库的商品参数列表
	 */
	public int delPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<Integer> rlPropIdList) throws IOException {
		int rt = Errno.ERROR;
		Oss.SvrStat stat = new Oss.SvrStat(flow);
		try {
			if(!FaiValObj.TermId.isValidTid(tid)) {
				rt = Errno.ARGS_ERROR;
				Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
				return rt;
			}
			Lock lock = LockUtil.getLock(aid);
			lock.lock();
			try {
				ProductPropDaoCtrl propDao = ProductPropDaoCtrl.getInstance(session);
				ProductPropRelDaoCtrl relDao = ProductPropRelDaoCtrl.getInstance(session);
				ProductPropValDaoCtrl valDao = ProductPropValDaoCtrl.getInstance(session);
				//统一控制事务
				TransactionCtrl transactionCtrl = new TransactionCtrl();
				transactionCtrl.register(propDao);
				transactionCtrl.register(relDao);
				transactionCtrl.register(valDao);

				transactionCtrl.setAutoCommit(false);
				FaiList<Integer> delPropIdList = null;
				try {
					// 删除参数业务关系数据
					ProductPropRelProc propRelProc = new ProductPropRelProc(flow, relDao);
					rt = propRelProc.delPropList(aid, unionPriId, libId, rlPropIdList);
					if(rt != Errno.OK) {
						Log.logErr(rt, "del prop rel list error;flow=%d;aid=%d;", flow, aid);
						return rt;
					}

					// 删除参数数据
					delPropIdList = propRelProc.getIdsByRlIds(aid, unionPriId, libId, rlPropIdList);
					ProductPropProc propProc = new ProductPropProc(flow, propDao);
					rt = propProc.delPropList(aid, delPropIdList);
					if(rt != Errno.OK) {
						Log.logErr(rt, "del prop list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;ids=%s;", flow, aid, unionPriId, tid, delPropIdList);
						return rt;
					}

					// 删除参数值数据
					ProductPropValProc valProc = new ProductPropValProc(flow, valDao);
					rt = valProc.delValListByPropIds(aid, delPropIdList);
					if(rt != Errno.OK) {
						Log.logErr(rt, "del prop val list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;propIds=%s;", flow, aid, unionPriId, tid, delPropIdList);
						return rt;
					}
				}finally {
					if(rt != Errno.OK) {
						transactionCtrl.rollback();
					}else {
						transactionCtrl.commit();
					}
					// 删除缓存
					ProductPropCacheCtrl.delCacheList(aid, delPropIdList);
					ProductPropRelCacheCtrl.delCacheList(aid, unionPriId, libId, rlPropIdList);
					ProductPropRelCacheCtrl.delSortCache(aid, unionPriId, libId);
					ProductPropValCacheCtrl.delCacheList(aid, delPropIdList);

					transactionCtrl.closeDao();
				}
			}finally {
				lock.unlock();
			}
			rt = Errno.OK;
			FaiBuffer sendBuf = new FaiBuffer(true);
			session.write(sendBuf);
			Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;ids=%s;", flow, aid, unionPriId, tid, rlPropIdList);
		}finally {
			stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
		}
		return rt;
	}

	/**
	 * 批量修改指定商品库的商品参数列表
	 */
	public int setPropInfoList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<ParamUpdater> updaterList) throws IOException {
		int rt = Errno.ERROR;
		Oss.SvrStat stat = new Oss.SvrStat(flow);
		try {
			if(!FaiValObj.TermId.isValidTid(tid)) {
				rt = Errno.ARGS_ERROR;
				Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
				return rt;
			}
			Lock lock = LockUtil.getLock(aid);
			lock.lock();
			try {
				ProductPropDaoCtrl propDao = ProductPropDaoCtrl.getInstance(session);
				ProductPropRelDaoCtrl relDao = ProductPropRelDaoCtrl.getInstance(session);
				//统一控制事务
				TransactionCtrl transactionCtrl = new TransactionCtrl();
				transactionCtrl.register(propDao);
				transactionCtrl.register(relDao);

				transactionCtrl.setAutoCommit(false);
				FaiList<ParamUpdater> propUpdaterList = new FaiList<ParamUpdater>();
				try {
					//修改参数业务关系表
					ProductPropRelProc propRelProc = new ProductPropRelProc(flow, relDao);
					ProductPropRelCacheCtrl.setExpire(aid, unionPriId, libId);
					rt = propRelProc.setPropList(aid, unionPriId, libId, updaterList, propUpdaterList);
					if(rt != Errno.OK) {
						Log.logErr(rt, "set prop rel list error;flow=%d;aid=%d;", flow, aid);
						return rt;
					}
					// 修改参数表
					if(!propUpdaterList.isEmpty()) {
						ProductPropProc propProc = new ProductPropProc(flow, propDao);
						ProductPropCacheCtrl.setExpire(aid);
						rt = propProc.setPropList(aid, propUpdaterList);
						if(rt != Errno.OK) {
							Log.logErr(rt, "set prop list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
							return rt;
						}
					}
				}finally {
					if(rt != Errno.OK) {
						transactionCtrl.rollback();
					}else {
						transactionCtrl.commit();
						// 更新缓存
						ProductPropCacheCtrl.updateCacheList(aid, propUpdaterList);
						ProductPropRelCacheCtrl.updateCacheList(aid, unionPriId, libId, updaterList);
					}
					transactionCtrl.closeDao();
				}
			}finally {
				lock.unlock();
			}
			rt = Errno.OK;
			FaiBuffer sendBuf = new FaiBuffer(true);
			session.write(sendBuf);
			Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		}finally {
			stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
		}
		return rt;
	}

	/**
	 * 获取指定商品参数的参数值列表
	 */
	public int getPropValList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, FaiList<Integer> rlPropIds) throws IOException {
		int rt = Errno.ERROR;
		Oss.SvrStat stat = new Oss.SvrStat(flow);
		try {
			if(!FaiValObj.TermId.isValidTid(tid)) {
				rt = Errno.ARGS_ERROR;
				Log.logErr("args error, args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
				return rt;
			}
			ProductPropRelDaoCtrl relDao = ProductPropRelDaoCtrl.getInstance(session);
			ProductPropValDaoCtrl valDao = ProductPropValDaoCtrl.getInstance(session);
			//统一控制事务
			TransactionCtrl transactionCtrl = new TransactionCtrl();
			transactionCtrl.register(relDao);
			transactionCtrl.register(valDao);

			Ref<FaiList<Param>> listRef = new Ref<FaiList<Param>>();
			try {
				ProductPropRelProc propRelProc = new ProductPropRelProc(flow, relDao);
				HashMap<Integer, Integer> propIdList = propRelProc.getIdsWithRlIds(aid, unionPriId, libId, rlPropIds);
				if(propIdList == null) {
					rt = Errno.ERROR;
					Log.logErr(rt, "getIdsByRlIds error;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
					return rt;
				}

				ProductPropValProc valProc = new ProductPropValProc(flow, valDao);
				rt = valProc.getListByPropIds(aid, propIdList, listRef);
				if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
					return rt;
				}
			}finally {
				transactionCtrl.closeDao();
			}

			FaiList<Param> list = listRef.value;
			// 按照sort字段排序
			ParamComparator comp = new ParamComparator(ProductPropValEntity.Info.SORT);
			Collections.sort(list, comp);
			FaiBuffer sendBuf = new FaiBuffer(true);
			list.toBuffer(sendBuf, ProductPropValDto.Key.INFO_LIST, ProductPropValDto.getInfoDto());
			session.write(sendBuf);
			rt = Errno.OK;
			Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		}finally {
			stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
		}
		return rt;
	}

	/**
	 * 批量修改(包括增、删、改)指定商品库的商品参数值列表
	 */
	public int setPropValList(FaiSession session, int flow, int aid, int unionPriId, int tid, int libId, int rlPropId, FaiList<ParamUpdater> updaterList, FaiList<Integer> delValIds, FaiList<Param> addInfoList) {
		int rt = Errno.ERROR;
		Oss.SvrStat stat = new Oss.SvrStat(flow);
		try {
			// 先根据参数业务id获取参数id
			FaiList<Integer> rlProIds = new FaiList<Integer>();
			rlProIds.add(rlPropId);
			int propId = 0;
			ProductPropRelDaoCtrl relDao = ProductPropRelDaoCtrl.getInstance(session);
			try {
				ProductPropRelProc relProc = new ProductPropRelProc(flow, relDao);
				FaiList<Integer> proIds = relProc.getIdsByRlIds(aid, unionPriId, libId, rlProIds);
				if(proIds == null || proIds.isEmpty()) {
					rt = Errno.ARGS_ERROR;
					Log.logErr(rt, "product prop is not exist;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlPropId=%d;", flow, aid, unionPriId, tid, rlPropId);
					return rt;
				}
				propId = proIds.get(0);
			} finally {
				relDao.closeDao();
			}

			Lock lock = LockUtil.getLock(aid);
			lock.lock();
			try {
				ProductPropValDaoCtrl valDao = ProductPropValDaoCtrl.getInstance(session);
				//统一控制事务
				TransactionCtrl transactionCtrl = new TransactionCtrl();
				transactionCtrl.register(valDao);

				transactionCtrl.setAutoCommit(false);
				try {

					ProductPropValProc valProc = new ProductPropValProc(flow, valDao);
					ProductPropValCacheCtrl.setExpire(aid, propId);
					// 修改
					rt = valProc.setValList(aid, propId, updaterList);
					if(rt != Errno.OK) {
						Log.logErr(rt, "set prop val list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlPropId=%d;", flow, aid, unionPriId, tid, rlPropId);
						return rt;
					}

					// 删除
					rt = valProc.delValList(aid, propId, delValIds);
					if(rt != Errno.OK) {
						Log.logErr(rt, "del prop val list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlPropId=%d;delValIds=%s;", flow, aid, unionPriId, tid, rlPropId, delValIds);
						return rt;
					}

					// 新增
					if(addInfoList != null && !addInfoList.isEmpty()) {
						rt = valProc.addValList(aid, propId, addInfoList, false);
						if(rt != Errno.OK) {
							Log.logErr(rt, "add prop val list error;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlPropId=%d;", flow, aid, unionPriId, tid, rlPropId);
							return rt;
						}
					}
				}finally {
					if(rt != Errno.OK) {
						transactionCtrl.rollback();
						valDao.clearIdBuilderCache(aid);
					}else {
						transactionCtrl.commit();
						// 清掉参数值缓存
						ProductPropValCacheCtrl.delCache(aid, propId);
					}
					transactionCtrl.closeDao();
				}
			}finally {
				lock.unlock();
			}
			Log.logStd("setPropValList ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
		}finally {
			stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
		}
		return rt;
	}

	private int assemblyPropInfo(int flow, int aid, int unionPriId, int tid, int libId, ProductPropDaoCtrl propDao, ProductPropRelDaoCtrl relDao, Param recvInfo, Param propInfo, Param relInfo) {
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
		int propId = propDao.buildId(aid, false);
		propInfo.setInt(ProductPropEntity.Info.AID, aid);
		propInfo.setInt(ProductPropEntity.Info.PROP_ID, propId);
		propInfo.setInt(ProductPropEntity.Info.SOURCE_TID, tid);
		propInfo.setInt(ProductPropEntity.Info.SOURCE_UNIONPRIID, unionPriId);
		propInfo.setString(ProductPropEntity.Info.NAME, name);
		propInfo.setInt(ProductPropEntity.Info.TYPE, type);
		propInfo.setInt(ProductPropEntity.Info.FLAG, flag);
		propInfo.setCalendar(ProductPropEntity.Info.CREATE_TIME, createTime);
		propInfo.setCalendar(ProductPropEntity.Info.UPDATE_TIME, updateTime);

		// 参数业务关系表数据
		int rlPropId = relDao.buildId(aid, unionPriId, false);
		int sort = recvInfo.getInt(ProductPropRelEntity.Info.SORT, 0);
		int rlFlag = recvInfo.getInt(ProductPropRelEntity.Info.RL_FLAG, 0);
		relInfo.setInt(ProductPropRelEntity.Info.AID, aid);
		relInfo.setInt(ProductPropRelEntity.Info.RL_PROP_ID, rlPropId);
		relInfo.setInt(ProductPropRelEntity.Info.PROP_ID, propId);
		relInfo.setInt(ProductPropRelEntity.Info.UNION_PRI_ID, unionPriId);
		relInfo.setInt(ProductPropRelEntity.Info.RL_LIB_ID, libId);
		relInfo.setCalendar(ProductPropRelEntity.Info.CREATE_TIME, createTime);
		relInfo.setCalendar(ProductPropRelEntity.Info.UPDATE_TIME, updateTime);
		relInfo.setInt(ProductPropRelEntity.Info.SORT, sort);
		relInfo.setInt(ProductPropRelEntity.Info.RL_FLAG, rlFlag);
		return rt;
	}
}
