package fai.MgProductSpecSvr.domain.repository;


import fai.MgProductSpecSvr.domain.comm.Utils;
import fai.MgProductSpecSvr.domain.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecSkuDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Var;

import java.util.*;
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
	 * 批量获取缓存
	 * codis集群是支持mget/mset  api中没支持可能是考虑到redisCluster集群不支持这两个命令所以没提供相应的api
	 */
	public static Map<Integer, Long> batchGetSkuIdRepresentSpu(int aid, List<Integer> pdIdList, boolean retainZero){
		if(pdIdList == null || pdIdList.isEmpty()){
			return new HashMap<>();
		}
		pdIdList.removeAll(Collections.singletonList(null)); // 移除所有null值
		Map<Integer, Long> result = new HashMap<>(pdIdList.size()*4/3+1);
		for (Integer pdId : pdIdList) {
			Long skuId = getSkuIdRepresentSpu(aid, pdId, retainZero);
			if(skuId == null){
				continue;
			}
			result.put(pdId, skuId);
		}
		return result; // 保留
	}
	public static Long getSkuIdRepresentSpu(int aid, int pdId){
		return getSkuIdRepresentSpu(aid, pdId, false);
	}
	public static Long getSkuIdRepresentSpu(int aid, int pdId, boolean retainZero){
		String skuIdRepresentSpuCacheKey = getSkuIdRepresentSpuCacheKey(aid, pdId);
		String skuIdStr = m_cache.get(skuIdRepresentSpuCacheKey);
		Long skuId = null;
		try {
			skuId = Long.parseLong(skuIdStr);
		}catch (NumberFormatException e){
		}
		if(!retainZero && (skuId == null || skuId == 0)){
			return null;
		}
		return skuId;
	}

	/**
	 * 批量设置缓存
	 * codis集群是支持mget/mset  api中没支持可能是考虑到redisCluster集群不支持这两个命令所以没提供相应的api
	 */
	public static boolean batchSetSkuIdRepresentSpu(int aid, Map<Integer, Long> pdIdSkuIdMap){
		if(pdIdSkuIdMap == null || pdIdSkuIdMap.isEmpty()){
			return true;
		}
		boolean boo  = true;
		for (Map.Entry<Integer, Long> pdIdSKuIdEntry : pdIdSkuIdMap.entrySet()) {
			int pdId = pdIdSKuIdEntry.getKey();

			long skuId = pdIdSKuIdEntry.getValue();
			boo &= setSkuIdRepresentSpu(aid, pdId, skuId);
		}
		return boo;
	}

	public static boolean setSkuIdRepresentSpu(int aid, int pdId, long skuId){
		String skuIdRepresentSpuCacheKey = getSkuIdRepresentSpuCacheKey(aid, pdId);
		return m_cache.set(skuIdRepresentSpuCacheKey, String.valueOf(skuId));
	}
	public static boolean delSkuIdRepresentSpu(int aid, Set<Integer> needDelPdIdSet) {
		if(needDelPdIdSet == null || needDelPdIdSet.isEmpty()){
			return true;
		}
		String[] keys = new String[needDelPdIdSet.size()];
		int idx = 0;
		for (Integer pdId : needDelPdIdSet) {
			keys[idx++] = getSkuIdRepresentSpuCacheKey(aid, pdId);
		}
		return m_cache.del(keys);
	}

	public static boolean delAllCache(int aid){
		return m_cache.del(getInfoCacheKey(aid), getPdIdSkuIdRelCacheKey(aid));
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

	/**
	 * 代表spu的skuId
	 */
	private static String getSkuIdRepresentSpuCacheKey(int aid, int pdId){
		return wrapCacheVersion(CACHE_KEY_PREFIX+":skuIdRepresentSpu-"+aid+"-"+pdId, aid);
	}


	private static final String CACHE_KEY_PREFIX = "MG_productSpecSku";

}
