package fai.MgProductSpecSvr.domain.repository;


import fai.comm.util.Parser;

import java.util.Set;
import java.util.stream.Collectors;

public class SpecTempBizRelCacheCtrl extends CacheCtrl {


	/**
	 * 设置id关联缓存
	 */
	public static boolean setRlTpScId(int aid, int unionPriId, int rlTpScId, int tpScId){
		return m_cache.hset(getRlTpScIdCacheKey(aid, unionPriId), String.valueOf(rlTpScId), String.valueOf(tpScId));
	}

	/**
	 * 获取关联id
	 */
	public static int getRlTpScId(int aid, int unionPriId, int rlTpScId){
		return Parser.parseInt(m_cache.hget(getRlTpScIdCacheKey(aid, unionPriId), String.valueOf(rlTpScId)), -1);
	}

	/**
	 * 删除关联id的缓存
	 */
	public static boolean delRlTpScId(int aid, int unionPriId, Set<Integer> rlTpScIdSet){
		if(rlTpScIdSet == null || rlTpScIdSet.isEmpty()){
			return true;
		}
		String[] rlTpScIdStrSet = rlTpScIdSet.stream().map(String::valueOf).collect(Collectors.toList()).toArray(new String[]{});
		return m_cache.hdel(getRlTpScIdCacheKey(aid, unionPriId), rlTpScIdStrSet);
	}

	/**
	 * 关联id的缓存，数据不缓存
	 */
	public static void setRlTpScIdCacheDirty(int aid, int unionPriId){
		String cacheKey = getRlTpScIdCacheKey(aid, unionPriId);
		m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND);
	}

	/**
	 * 关联id 缓存key
	 */
	private static String getRlTpScIdCacheKey(int aid, int unionPriId){
		return wrapCacheVersion(CACHE_KEY_PREFIX+"-rlTpScId:"+aid+"-"+unionPriId, aid);
	}

	private static String getCacheKey(int aid, int unionPriId){
		return wrapCacheVersion(CACHE_KEY_PREFIX+":"+aid+"-"+unionPriId, aid);
	}

	private static final String CACHE_KEY_PREFIX = "MG_specTempBizRel";
}
