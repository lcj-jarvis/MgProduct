package fai.MgProductSpecSvr.domain.repository;


import fai.MgProductSpecSvr.domain.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Var;

import java.util.Iterator;
import java.util.Set;

public class ProductSpecCacheCtrl extends CacheCtrl {


	/**
	 * 初始化缓存
	 */
	public static boolean initPdScList(int aid, int pdId, FaiList<Param> infoList){
		if(infoList == null || infoList.isEmpty()){
			return false;
		}
		return m_cache.hmsetFaiList(getCacheKey(aid, pdId), ProductSpecEntity.Info.PD_SC_ID, Var.Type.INT, infoList, ProductSpecDto.Key.INFO, ProductSpecDto.CacheDto.getCacheDto());
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

	public static FaiList<Param> getPdScList(int aid, int pdId){
		if(!m_cache.exists(getCacheKey(aid, pdId))){
			return null;
		}
		return m_cache.hgetAllFaiList(getCacheKey(aid, pdId), ProductSpecDto.Key.INFO, ProductSpecDto.CacheDto.getCacheDto());
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
		return wrapCacheVersion(CACHE_KEY_PREFIX+":"+aid+"-"+pdId, aid);
	}

	private static final String CACHE_KEY_PREFIX = "MG_productSpec";



}
