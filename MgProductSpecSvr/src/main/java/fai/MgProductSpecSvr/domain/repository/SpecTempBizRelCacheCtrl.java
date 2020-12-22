package fai.MgProductSpecSvr.domain.repository;


import fai.MgProductSpecSvr.domain.entity.SpecTempBizRelEntity;
import fai.MgProductSpecSvr.interfaces.dto.SpecTempDetailDto;
import fai.comm.util.FaiList;
import fai.comm.util.Param;
import fai.comm.util.Parser;
import fai.comm.util.Var;

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
	//private static final String CACHE_RL_TP_SC_ID__TP_SC_ID = "rlTpScId-tpScId";

	private static String getRlTpScIdCachKey(int aid, int unionPriId){
		return CACHE_KEY_PREFIX+"-rlTpScId-"+aid+"-"+unionPriId;
	}

	private static String getCacheKey(int aid, int unionPriId){
		return CACHE_KEY_PREFIX+"-"+"-"+aid+"-"+unionPriId;
	}

	private static final String CACHE_KEY_PREFIX = "specTempBizRel";
}
