package fai.MgProductPropSvr.domain.repository;

import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.MgProductPropSvr.interfaces.dto.ProductPropValDto;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

public class ProductPropValCacheCtrl extends CacheCtrl {

	public static FaiList<Param> getCacheList(int aid, int propId) {
		String cacheKey = getCacheKey(aid, propId);
		return m_cache.hgetAllFaiList(cacheKey, ProductPropValDto.Key.INFO, ProductPropValDto.getCacheInfoDto());
	}

	public static void addCacheListExist(int aid, int propId, FaiList<Param> list) {
		if(list == null || list.isEmpty()) {
			return;
		}
		if(!exists(aid, propId)) {
			return;
		}
		addCacheList(aid, propId, list);
	}

	public static void addCacheList(int aid, int propId, FaiList<Param> list) {
		if(list == null || list.isEmpty()) {
			return;
		}
		String cacheKey = getCacheKey(aid, propId);
		m_cache.hmsetFaiList(cacheKey, ProductPropValEntity.Info.PROP_VAL_ID, Var.Type.INT, list, ProductPropValDto.Key.INFO, ProductPropValDto.getCacheInfoDto());
	}

	public static void delCache(int aid, int propId) {
		String cacheKey = getCacheKey(aid, propId);
		if(!m_cache.exists(cacheKey)) {
			return;
		}
		m_cache.del(cacheKey);
	}

	public static void delCacheList(int aid, FaiList<Integer> propIds) {
		if(propIds == null || propIds.isEmpty()) {
			return;
		}
		String[] cacheKeys = new String[propIds.size()];
		for(int i = 0; i < propIds.size(); i++) {
			cacheKeys[i] = getCacheKey(aid, propIds.get(i));
		}
		m_cache.del(cacheKeys);
	}

	public static boolean exists(int aid, int propId) {
		String cacheKey = getCacheKey(aid, propId);
		return m_cache.exists(cacheKey);
	}

	public static void setExpire(int aid, int propId) {
		String cacheKey = getCacheKey(aid, propId);
		m_cache.expire(cacheKey, EXPIRE_SECOND);
	}

	public static String getCacheKey(int aid, int propId) {
		return CACHE_KEY + "-" + aid + "-" + propId;
	}

	private static final int EXPIRE_SECOND = 10;
	private static final String CACHE_KEY = "MG_productPropVal";

	/** 数据状态缓存 **/
	public static class DataStatusCache {

		public static Param get(int aid) {
			String cacheKey = getCacheKey(aid);
			Param info = m_cache.getParam(cacheKey, ProductPropValDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
			if(!Str.isEmpty(info)) {
				expire(aid, 6*3600);
			}
			return info;
		}

		public static void add(int aid, Param info) {
			String cacheKey = getCacheKey(aid);
			m_cache.setParam(cacheKey, info, ProductPropValDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
		}

		public static void update(int aid) {
			update(aid, 0);
		}

		public static void update(int aid, int count) {
			update(aid, count, true);
		}

		public static void update(int aid, int count, boolean add) {
			Param info = new Param();
			info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, System.currentTimeMillis());
			ParamUpdater updater = new ParamUpdater(info);
			if(count != 0) {
				String op = add ? ParamUpdater.INC : ParamUpdater.DEC;
				updater.add(DataStatus.Info.TOTAL_SIZE, op, count);
			}
			m_cache.updateParam(getCacheKey(aid), updater, ProductPropValDto.Key.DATA_STATUS, DataStatus.Dto.getDataStatusDto());
		}

		public static void expire(int aid, int second) {
			m_cache.expire(getCacheKey(aid), second);
		}

		public static String getCacheKey(int aid) {
			return DATA_STATUS_CACHE_KEY + "-" + aid;
		}
		private static final String DATA_STATUS_CACHE_KEY = "MG_pdPropValDS";
	}
}
