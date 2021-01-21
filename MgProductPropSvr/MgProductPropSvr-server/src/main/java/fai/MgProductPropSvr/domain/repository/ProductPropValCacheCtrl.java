package fai.MgProductPropSvr.domain.repository;

import fai.MgProductPropSvr.domain.entity.ProductPropValEntity;
import fai.MgProductPropSvr.interfaces.dto.ProductPropValDto;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.Var;

public class ProductPropValCacheCtrl extends CacheCtrl {

	public static FaiList<Param> getCacheList(int aid, int propId) {
		String cacheKey = getCacheKey(aid, propId);
		return m_cache.hgetAllFaiList(cacheKey, ProductPropValDto.Key.INFO, ProductPropValDto.getCacheInfoDto());
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
}
