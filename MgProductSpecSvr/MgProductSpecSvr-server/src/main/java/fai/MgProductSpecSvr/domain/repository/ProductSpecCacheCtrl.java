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

	/**
	 * 移除缓存
	 */
	public static boolean delCache(int aid, int pdId){
		if(!m_cache.exists(getCacheKey(aid, pdId))){
			return true;
		}
		return m_cache.del(getCacheKey(aid, pdId));
	}

	public static boolean hasCache(int aid, int pdId){
		return m_cache.exists(getCacheKey(aid, pdId));
	}

	public static Param getPdScInfo(int aid, int pdId, int pdScId){
		return m_cache.hgetParam(getCacheKey(aid, pdId), pdScId, ProductSpecDto.Key.INFO, ProductSpecDto.CacheDto.getCacheDto());
	}
	public static FaiList<Param> getPdScList(int aid, int pdId, FaiList<Integer> pdScIdList){
		if(pdScIdList == null || pdScIdList.isEmpty()){
			return null;
		}
		if(!m_cache.exists(getCacheKey(aid, pdId))){
			return null;
		}
		FaiList<String> psScIdStrList = new FaiList<>(pdScIdList.size());
		pdScIdList.forEach(pdScId ->{
			psScIdStrList.add(String.valueOf(pdScId));
		});
		try {
			FaiList<Param> list = m_cache.hmget(getCacheKey(aid, pdId), ProductSpecDto.Key.INFO, ProductSpecDto.CacheDto.getCacheDto(), psScIdStrList);
			if(list == null){
				return null;
			}
			for (int i = list.size() - 1; i >= 0; i--) {
				if(list.get(i) == null){
					list.remove(i);
				}
			}
			return list;
		} catch (Exception e) {
			Log.logErr(e);
		}
		return null;
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
		return CACHE_KEY_PREFIX+"-"+aid+"-"+pdId;
	}

	private static final String CACHE_KEY_PREFIX = "MG_productSpec";



}
