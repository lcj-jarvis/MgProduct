package fai.MgProductSpecSvr.domain.repository;


import fai.MgProductSpecSvr.domain.entity.SpecTempBizRelEntity;
import fai.MgProductSpecSvr.interfaces.dto.SpecTempDetailDto;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.Parser;
import fai.comm.util.Var;

import java.util.Set;
import java.util.stream.Collectors;

public class SpecTempBizRelCacheCtrl extends CacheCtrl {



	public static boolean setCacheInfo(int aid, int unionPriId, int rlTpScId, int tpScId) {

		return false;
	}
	public static boolean setCacheList(int aid, int unionPriId, FaiList<Param> infoList) {
		String cacheHashInfoKey = getCacheKey(aid, unionPriId);
		 m_cache.hmsetFaiList(cacheHashInfoKey, SpecTempBizRelEntity.Info.RL_LIB_ID, Var.Type.INT, infoList, SpecTempDetailDto.Key.INFO, SpecTempDetailDto.CacheDto.getDtoDef());
		return false;
	}
	public static boolean setCacheInfo(int aid, int unionPriId, int rlLibId, Param info) {

		return false;
	}


	public static boolean setTpScId(int aid, int unionPriId, int rlTpScId, int tpScId){
		return m_cache.hset(getRlTpScIdCachKey(aid, unionPriId), String.valueOf(rlTpScId), String.valueOf(tpScId));
	}
	public static int getTpScId(int aid, int unionPriId, int rlTpScId){
		return Parser.parseInt(m_cache.hget(getRlTpScIdCachKey(aid, unionPriId), String.valueOf(rlTpScId)), -1);
	}

	public static boolean delTpScId(int aid, int unionPriId, Set<Integer> rlTpScIdSet){
		if(rlTpScIdSet == null || rlTpScIdSet.isEmpty()){
			return true;
		}
		String[] rlTpScIdStrSet = rlTpScIdSet.stream().map(String::valueOf).collect(Collectors.toList()).toArray(new String[]{});
		return m_cache.hdel(getRlTpScIdCachKey(aid, unionPriId), rlTpScIdStrSet);
	}

	public static boolean delTpScId(int aid, int unionPriId, int rlTpScId){
		return m_cache.hdel(getRlTpScIdCachKey(aid, unionPriId), String.valueOf(rlTpScId));
	}


	public static void setCacheDirty(int aid, Set<Integer> unionPriIdSet){
		if(unionPriIdSet == null || unionPriIdSet.isEmpty()){
			return;
		}
		for (Integer unionPriId : unionPriIdSet) {
			setCacheDirty(aid, unionPriId);
		}
	}
	public static void setCacheDirty(int aid, int unionPriId){
		String cacheKey = getCacheKey(aid, unionPriId);
		m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND);
	}

	public static void setRlTpScIdCacheDirty(int aid, Set<Integer> unionPriIdSet){
		if(unionPriIdSet == null || unionPriIdSet.isEmpty()){
			return;
		}
		for (Integer unionPriId : unionPriIdSet) {
			setRlTpScIdCacheDirty(aid, unionPriId);
		}
	}

	public static void setRlTpScIdCacheDirty(int aid, int unionPriId){
		String cacheKey = getRlTpScIdCachKey(aid, unionPriId);
		m_cache.expire(cacheKey, DIRTY_EXPIRE_SECOND);
	}

	private static String getRlTpScIdCachKey(int aid, int unionPriId){
		return CACHE_KEY_PREFIX+"-rlTpScId:"+aid+"-"+unionPriId;
	}

	private static String getCacheKey(int aid, int unionPriId){
		return CACHE_KEY_PREFIX+":"+aid+"-"+unionPriId;
	}

	private static final String CACHE_KEY_PREFIX = "MG_specTempBizRel";
}
