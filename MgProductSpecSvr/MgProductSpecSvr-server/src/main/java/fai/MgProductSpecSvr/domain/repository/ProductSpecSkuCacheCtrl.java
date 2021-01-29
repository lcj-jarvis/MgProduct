package fai.MgProductSpecSvr.domain.repository;


import fai.comm.util.FaiList;
import fai.comm.util.Param;

import java.util.Iterator;
import java.util.Set;

public class ProductSpecSkuCacheCtrl extends CacheCtrl {

	/**
	 * 初始化缓存
	 */
	public static boolean initCache(int aid, int pdId, FaiList<Param> infoList){
		if(infoList == null || infoList.isEmpty()){
			return false;
		}
		return false;
	}


	public static boolean delCache(int aid, Set<Integer> pdIdSet) {
		if(pdIdSet == null || pdIdSet.isEmpty()){
			return true;
		}
		String[] keys = new String[pdIdSet.size()];
		Iterator<Integer> iterator = pdIdSet.iterator();
		int i = 0;
		while (iterator.hasNext()){
			keys[i++] = getCacheKey(aid, iterator.next());
		}
		return m_cache.del(keys);
	}

	/**
	 * 移除缓存
	 */
	public static boolean delCache(int aid, int pdId){
		return m_cache.del(getCacheKey(aid, pdId));
	}


	public static void setCacheDirty(int aid, Set<Integer> pdIdSet){
		if(pdIdSet == null){
			return;
		}
		for (Integer pdId : pdIdSet) {
			setCacheDirty(aid, pdId);
		}
	}
	public static void setCacheDirty(int aid, int pdId){
		String cacheKey = getCacheKey(aid, pdId);
		m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND);
	}


	private static String getCacheKey(int aid, int pdId){
		return CACHE_KEY_PREFIX+"-"+aid+"-"+pdId;
	}

	private static final String CACHE_KEY_PREFIX = "MG_productSpecSku";
}
