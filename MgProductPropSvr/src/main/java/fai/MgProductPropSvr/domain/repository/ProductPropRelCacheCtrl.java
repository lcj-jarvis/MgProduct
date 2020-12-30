package fai.MgProductPropSvr.domain.repository;

import fai.MgProductPropSvr.domain.entity.ProductPropRelEntity;
import fai.MgProductPropSvr.interfaces.dto.ProductPropRelDto;
import fai.comm.util.*;

public class ProductPropRelCacheCtrl extends CacheCtrl {

	public static FaiList<Param> getCacheList(int aid, int unionPriId, int libId) {
		String cacheKey = getCacheKey(aid, unionPriId, libId);
		return m_cache.hgetAllFaiList(cacheKey, ProductPropRelDto.Key.INFO, ProductPropRelDto.getCacheInfoDto());
	}

	public static void delCache(int aid, int unionPriId, int libId) {
		String cacheKey = getCacheKey(aid, unionPriId, libId);
		m_cache.del(cacheKey);
	}

	public static void addCache(int aid, int uninoId, int libId, Param info) {
		if(Str.isEmpty(info)) {
			return;
		}
		String cacheKey = getCacheKey(aid, uninoId, libId);
		int rlPropId = info.getInt(ProductPropRelEntity.Info.RL_PROP_ID, 0);
		m_cache.hsetParam(true, cacheKey, String.valueOf(rlPropId), info, ProductPropRelDto.Key.INFO, ProductPropRelDto.getCacheInfoDto());
	}

	public static void addCacheList(int aid, int uninoId, int libId, FaiList<Param> list) {
		if(list == null || list.isEmpty()) {
			return;
		}
		String cacheKey = getCacheKey(aid, uninoId, libId);
		m_cache.hmsetFaiList(cacheKey, ProductPropRelEntity.Info.RL_PROP_ID, Var.Type.INT, list, ProductPropRelDto.Key.INFO, ProductPropRelDto.getCacheInfoDto());
	}

	public static void delCacheList(int aid, int uninoId, int libId, FaiList<Integer> rlPropIds) {
		if(rlPropIds == null || rlPropIds.isEmpty()) {
			return;
		}
		String cacheKey = getCacheKey(aid, uninoId, libId);
		if(!m_cache.exists(cacheKey)) {
			return;
		}
		String[] rlPropIdStrs = new String[rlPropIds.size()];
		for(int i = 0; i < rlPropIds.size(); i++) {
			rlPropIdStrs[i] = String.valueOf(rlPropIds.get(i));
		}
		m_cache.hdel(cacheKey, rlPropIdStrs);
	}

	public static void updateCacheList(int aid, int uninoId, int libId, FaiList<ParamUpdater> updaterList) {
		if(updaterList == null || updaterList.isEmpty()) {
			return;
		}
		String cacheKey = getCacheKey(aid, uninoId, libId);
		if(!m_cache.exists(cacheKey)) {
			return;
		}
		for(ParamUpdater updater : updaterList) {
			Param info = updater.getData();
			int rlPropId = info.getInt(ProductPropRelEntity.Info.RL_PROP_ID, 0);
			m_cache.hsetParam(cacheKey, String.valueOf(rlPropId), updater, ProductPropRelDto.Key.INFO, ProductPropRelDto.getCacheInfoDto());
		}
	}

	public static boolean exists(int aid, int uninoId, int libId) {
		String cacheKey = getCacheKey(aid, uninoId, libId);
		return m_cache.exists(cacheKey);
	}

	public static void setExpire(int aid, int uninoId, int libId) {
		String cacheKey = getCacheKey(aid, uninoId, libId);
		m_cache.expire(cacheKey, EXPIRE_SECOND);
	}

	public static String getCacheKey(int aid, int unionPriId, int libId) {
		return CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + libId;
	}

	public static void setSortCache(int aid, int unionPriId, int libId, int sort) {
		if(sort < 0) {
			return;
		}
		String cacheKey = getSortCacheKey(aid, unionPriId, libId);
		m_cache.set(cacheKey, String.valueOf(sort));
	}

	public static void delSortCache(int aid, int unionPriId, int libId) {
		String cacheKey = getSortCacheKey(aid, unionPriId, libId);
		if(!m_cache.exists(cacheKey)) {
			return;
		}
		m_cache.del(cacheKey);
	}

	public static String getSortCache(int aid, int unionPriId, int libId) {
		String cacheKey = getSortCacheKey(aid, unionPriId, libId);
		if(!m_cache.exists(cacheKey)) {
			return null;
		}
		return m_cache.get(cacheKey);
	}

	public static String getSortCacheKey(int aid, int unionPriId, int libId) {
		return SORT_CACHE_KEY + "-" + aid + "-" + unionPriId + "-" + libId;
	}

	private static final int EXPIRE_SECOND = 10;
	private static final String CACHE_KEY = "MG_productPropRel";
	private static final String SORT_CACHE_KEY = "MG_productPropRelSort";
}
