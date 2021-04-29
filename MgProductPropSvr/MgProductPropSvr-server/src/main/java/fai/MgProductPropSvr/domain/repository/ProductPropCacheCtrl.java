package fai.MgProductPropSvr.domain.repository;

import fai.MgProductPropSvr.domain.entity.ProductPropEntity;
import fai.MgProductPropSvr.interfaces.dto.ProductPropDto;
import fai.comm.util.*;

public class ProductPropCacheCtrl extends CacheCtrl {

	public static FaiList<Param> getCacheList(int aid) {
		String cacheKey = getCacheKey(aid);
		return m_cache.hgetAllFaiList(cacheKey, ProductPropDto.Key.INFO, ProductPropDto.getCacheInfoDto());
	}

	public static void delCache(int aid) {
		m_cache.del(getCacheKey(aid));
	}

	public static void delCacheList(int aid, FaiList<Integer> propIds) {
		if(propIds == null || propIds.isEmpty()) {
			return;
		}
		String cacheKey = getCacheKey(aid);
		if(!m_cache.exists(cacheKey)) {
			return;
		}
		String[] propIdStrs = new String[propIds.size()];
		for(int i = 0; i < propIds.size(); i++) {
			propIdStrs[i] = String.valueOf(propIds.get(i));
		}
		m_cache.hdel(cacheKey, propIdStrs);
	}

	public static void updateCacheList(int aid, FaiList<ParamUpdater> updaterList) {
		if(updaterList == null || updaterList.isEmpty()) {
			return;
		}
		String cacheKey = getCacheKey(aid);
		if(!m_cache.exists(cacheKey)) {
			return;
		}
		for(ParamUpdater updater : updaterList) {
			Param info = updater.getData();
			int propId = info.getInt(ProductPropEntity.Info.PROP_ID, 0);
			m_cache.hsetParam(cacheKey, String.valueOf(propId), updater, ProductPropDto.Key.INFO, ProductPropDto.getCacheInfoDto());
		}
	}

	public static void addCache(int aid, Param info) {
		if(Str.isEmpty(info)) {
			return;
		}
		String cacheKey = getCacheKey(aid);
		int propId = info.getInt(ProductPropEntity.Info.PROP_ID, 0);
		m_cache.hsetParam(true, cacheKey, String.valueOf(propId), info, ProductPropDto.Key.INFO, ProductPropDto.getCacheInfoDto());
	}

	public static void addCacheList(int aid, FaiList<Param> list) {
		if(list == null || list.isEmpty()) {
			return;
		}
		String cacheKey = getCacheKey(aid);
		m_cache.hmsetFaiList(cacheKey, ProductPropEntity.Info.PROP_ID, Var.Type.INT, list, ProductPropDto.Key.INFO, ProductPropDto.getCacheInfoDto());
	}

	public static boolean exists(int aid) {
		String cacheKey = getCacheKey(aid);
		return m_cache.exists(cacheKey);
	}

	public static void setExpire(int aid) {
		String cacheKey = getCacheKey(aid);
		m_cache.expire(cacheKey, EXPIRE_SECOND);
	}

	public static String getCacheKey(int aid) {
		return wrapCacheVersion(CACHE_KEY + "-" + aid, aid);
	}

	private static final int EXPIRE_SECOND = 10;
	private static final String CACHE_KEY = "MG_productProp";
}
