package fai.MgProductSpecSvr.domain.repository;


import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Var;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProductSpecSkuCacheCtrl extends CacheCtrl {

	/**
	 * 初始化缓存
	 */
	public static boolean initInfoCache(int aid, FaiList<Param> infoList){
		if(infoList == null || infoList.isEmpty()){
			return false;
		}
		boolean boo = m_cache.hmsetFaiList(getInfoCacheKey(aid), ProductSpecSkuEntity.Info.SKU_ID, Var.Type.LONG, infoList, ProductSpecSkuDto.Key.INFO, ProductSpecSkuDto.CacheDto.getCacheDto());
		Log.logDbg("whalelog  aid=%s;boo=%s;", aid, boo);
		return boo;
	}

	public static boolean initInfoWithRelCache(int aid, int pdId, FaiList<Param> infoList) {
		boolean boo = initInfoCache(aid, infoList);
		Log.logDbg("whalelog  aid=%s;boo=%s;", aid, boo);
		if(boo){
			boo = initRelCache(aid, pdId, infoList);
			Log.logDbg("whalelog 1 aid=%s;boo=%s;", aid, boo);
		}
		return boo;
	}

	private static boolean initRelCache(int aid, int pdId, FaiList<Param> infoList) {
		String pdIdSkuIdRelCacheKey = getPdIdSkuIdRelCacheKey(aid);
		FaiList<Long> skuIdList = Utils.getValList(infoList, ProductSpecSkuEntity.Info.SKU_ID);
		return m_cache.hset(pdIdSkuIdRelCacheKey, String.valueOf(pdId), skuIdList.toJson());
	}


	public static boolean delInfoCache(int aid, Set<Long> skuIdSet) {
		if(skuIdSet == null || skuIdSet.isEmpty()){
			return true;
		}
		Set<String> skuIdStrSet = skuIdSet.stream().map(String::valueOf).collect(Collectors.toSet());
		String infoCacheKey = getInfoCacheKey(aid);
		return m_cache.hdel(infoCacheKey, skuIdStrSet.toArray(new String[]{}));
	}

	public static boolean delRelCache(int aid, Set<Integer> needDelPdIdSet) {
		if(needDelPdIdSet == null || needDelPdIdSet.isEmpty()){
			return true;
		}
		Set<String> skuIdStrSet = needDelPdIdSet.stream().map(String::valueOf).collect(Collectors.toSet());
		String pdIdSkuIdRelCacheKey = getPdIdSkuIdRelCacheKey(aid);
		return m_cache.hdel(pdIdSkuIdRelCacheKey, skuIdStrSet.toArray(new String[]{}));
	}

	public static FaiList<Param> getList(int aid, FaiList<Long> skuIdList) {
		String infoCacheKey = getInfoCacheKey(aid);
		List<String> skuIdStrList = skuIdList.stream().map(String::valueOf).collect(Collectors.toList());
		FaiList<Param> cacheList = null;
		try {
			cacheList = m_cache.hmget(infoCacheKey, ProductSpecSkuDto.Key.INFO, ProductSpecSkuDto.CacheDto.getCacheDto(), skuIdStrList);
		} catch (Exception e) {
			Log.logErr(e, "hmget err aid=%s;skuIdList=%s;", aid, skuIdList);
		}
		return cacheList;
	}

	public static FaiList<Param> getListByPdId(int aid, int pdId) {
		String skuIdSetStr = m_cache.hget(getPdIdSkuIdRelCacheKey(aid), String.valueOf(pdId));
		if(skuIdSetStr == null){
			return null;
		}
		FaiList<Long> skuIdList = FaiList.parseLongList(skuIdSetStr);
		return getList(aid, skuIdList);
	}


	public static void setInfoCacheDirty(int aid){
		String cacheKey = getInfoCacheKey(aid);
		m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND);
	}

	public static void setRelCacheDirty(int aid){
		String pdIdSkuIdRelCacheKey = getPdIdSkuIdRelCacheKey(aid);
		m_cache.expire(pdIdSkuIdRelCacheKey, DIRTY_EXPIRE_SECOND);
	}

	/**
	 * 数据缓存
	 */
	private static String getInfoCacheKey(int aid){
		return CACHE_KEY_PREFIX+":"+aid;
	}

	/**
	 * 关系缓存
	 */
	private static String getPdIdSkuIdRelCacheKey(int aid){
		return CACHE_KEY_PREFIX+":pdIdSkuIdRel-"+aid;
	}


	private static final String CACHE_KEY_PREFIX = "MG_productSpecSku";



}
