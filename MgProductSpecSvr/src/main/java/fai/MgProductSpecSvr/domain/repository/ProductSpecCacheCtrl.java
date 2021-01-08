package fai.MgProductSpecSvr.domain.repository;


import fai.MgProductSpecSvr.domain.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.interfaces.dto.ProductSpecDto;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.comm.util.Var;

public class ProductSpecCacheCtrl extends CacheCtrl {

	/**
	 * 追加缓存
	 */
	public static boolean appendPdScList(int aid, int pdId, FaiList<Param> infoList){
		if(!m_cache.exists(getCacheKey(aid, pdId))){
			return true;
		}
		if(initPdScList(aid, pdId, infoList)){
			return true;
		}
		if(delAllCache(aid, pdId)){
			return true;
		}
		Log.logErr("delAllCache error;aid=%s;pdId=%s;", aid, pdId);
		return false;
	}

	/**
	 * 初始化缓存
	 */
	public static boolean initPdScList(int aid, int pdId, FaiList<Param> infoList){
		if(infoList == null){
			return false;
		}
		return m_cache.hmsetFaiList(getCacheKey(aid, pdId), ProductSpecEntity.Info.PD_SC_ID, Var.Type.INT, infoList, ProductSpecDto.Key.INFO, ProductSpecDto.CacheDto.getCacheDto());
	}

	/**
	 * 移除部分缓存
	 */
	public static boolean removeCache(int aid, int pdId, FaiList<Integer> pdScIdList){
		if(pdScIdList == null || pdScIdList.isEmpty()){
			return false;
		}
		if(!m_cache.exists(getCacheKey(aid, pdId))){
			return true;
		}
		String[] keys = new String[pdScIdList.size()];
		for (int i = 0; i < pdScIdList.size(); i++) {
			keys[i] = String.valueOf(pdScIdList.get(i));
		}
		if(m_cache.hdel(getCacheKey(aid, pdId), keys)){
			return true;
		}
		if(delAllCache(aid, pdId)){
			return true;
		}
		Log.logErr("delAllCache error;aid=%s;pdId=%s;", aid, pdId);
		return false;
	}
	/**
	 * 移除全部缓存
	 */
	public static boolean delAllCache(int aid, int pdId){
		if(!m_cache.exists(getCacheKey(aid, pdId))){
			return true;
		}
		return m_cache.del(getCacheKey(aid, pdId));
	}

	/**
	 * 替换缓存
	 */
	public static boolean replaceCache(int aid, int pdId, FaiList<Param> cacheDataList) {
		return appendPdScList(aid, pdId, cacheDataList);
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



	private static String getCacheKey(int aid, int pdId){
		return CACHE_KEY_PREFIX+"-"+aid+"-"+pdId;
	}

	private static final String CACHE_KEY_PREFIX = "MG_productSpec";


}
