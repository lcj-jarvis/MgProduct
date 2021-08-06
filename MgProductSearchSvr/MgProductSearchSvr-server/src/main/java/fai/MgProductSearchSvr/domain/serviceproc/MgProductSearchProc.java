package fai.MgProductSearchSvr.domain.serviceproc;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearch;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCache;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

/**
 * @author LuChaoJi
 * @date 2021-08-06 9:48
 */
public class MgProductSearchProc {

    // 根据 ProductRelEntity.Info.RL_PD_ID 去重
    public FaiList<Param> removeRepeatedByKey(FaiList<Param> resultList, String resultListKey){
        FaiList<Param> filterList = new FaiList<Param>();
        if(resultList.isEmpty()){
            return filterList;
        }
        HashSet<Integer> idSetList = new HashSet<Integer>();  // 去重集合
        for(Param info : resultList){
            int id = info.getInt(resultListKey);
            if(idSetList.contains(id)){
                continue;
            }
            idSetList.add(id);
            filterList.add(info);
        }
        return filterList;
    }

    // 由ParamList 提取业务商品 idList
    public FaiList<Integer> toIdList(FaiList<Param> resultList, String key){
        FaiList<Integer> idList = new FaiList<Integer>();
        for(Param info : resultList){
            Integer keyVal = info.getInt(key);
            if(keyVal != null){
                idList.add(keyVal);
            }
        }
        return idList;
    }

    // 根据 set 的缓存重新过滤数据
    public FaiList<Param> searchListFilterBySearchResultList(FaiList<Param> resultList, String resultListKey, FaiList<Param> searchList, String searchListKey){
        FaiList<Param> filterList = new FaiList<Param>();
        HashSet<Integer> resultSetList = faiListToHashIdSet(resultList, resultListKey);  // 转换为 set 的集合
        for(Param info : searchList){
            if(resultSetList.contains(info.getInt(searchListKey))){
                filterList.add(info);
            }
        }
        return filterList;
    }

    public FaiList<Param> resultListFixedRlPdId(int aid, int unionPriId, int flow, FaiList<Param> resultList, FaiList<Param> includeRlPdIdResultList){
        FaiList<Param> filterList = new FaiList<Param>();
        String key = ProductEntity.Info.PD_ID;
        // 转换为 set 的集合
        HashMap<Integer, Param> searchHashMap = faiListToHashMap(includeRlPdIdResultList, key);
        for(Param info : resultList){
            Param matchInfo = searchHashMap.get(info.getInt(key));
            if(matchInfo != null){
                info.assign(matchInfo, ProductRelEntity.Info.RL_PD_ID);
            }else{
                info.assign(info, ProductRelEntity.Info.PD_ID, ProductRelEntity.Info.RL_PD_ID);
                Log.logErr("matchInfo null err, aid=%d;unionPriId=%d;flow=%d;info=%s;", aid, unionPriId, flow, info);
            }
        }
        return filterList;
    }


    public FaiList<Param> getSearchResult(ParamMatcher searchMatcher, FaiList<Param> searchList, FaiList<Param> resultList, String searchKey){
        // 非第一次搜索，进入 set<id> 的过滤逻辑，减少搜索集合，提高性能
        if(resultList != null && !resultList.isEmpty()){
            searchList = searchListFilterBySearchResultList(resultList, searchKey, searchList, searchKey);
        }
        if(searchList.isEmpty()){
            return searchList;
        }
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = searchMatcher;
        Searcher searcher = new Searcher(searchArg);
        searchList = searcher.getParamList(searchList);
        return searchList;
    }

    public Param getDataStatusInfoFromEachSvr(int aid, int unionPriId, int tid, int flow, String tableName, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli){
        /*
        //先不考虑优化
        MgProductSearch.SearchTableNameEnum searchTableName = MgProductSearch.SearchTableNameEnum.MG_PRODUCT;
        int rt = Errno.OK;
        switch (searchTableName) {
            case MG_PRODUCT:
                rt = mgProductBasicCli.getPdDataStatus(aid, remoteDataStatusInfo);
                break;
            case MG_PRODUCT_REL:
                rt = mgProductBasicCli.getPdRelDataStatus(aid, unionPriId, remoteDataStatusInfo);
                break;
            case MG_PRODUCT_BIND_PROP:
                rt = mgProductBasicCli.getBindPropDataStatus(aid, unionPriId, remoteDataStatusInfo);
                break;
            case MG_PRODUCT_BIND_GROUP:
                rt = mgProductBasicCli.getBindGroupDataStatus(aid, unionPriId, remoteDataStatusInfo);
                break;
            case MG_PRODUCT_BIND_TAG:
                rt = mgProductBasicCli.getBindTagDataStatus(aid, unionPriId, remoteDataStatusInfo);
                break;
            case MG_SPU_BIZ_SUMMARY:
                rt = mgProductStoreCli.getSpuBizSummaryDataStatus(aid, tid, unionPriId, remoteDataStatusInfo);
                break;
        }
        if (rt != Errno.OK) {
            Log.logErr(rt,"get "+ searchTableName +" DataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
        }
        return remoteDataStatusInfo;*/

        Param remoteDataStatusInfo = new Param();
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            int rt = mgProductBasicCli.getPdDataStatus(aid, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getPdDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getPdDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }
        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            // 从远端获取数据
            int rt = mgProductBasicCli.getPdRelDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getPdRelDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getPdRelDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // 从远端获取数据
            int rt = mgProductBasicCli.getBindPropDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindPropDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindPropDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // 从远端获取数据
            int rt = mgProductBasicCli.getBindGroupDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindGroupDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName.equals(tableName)){
            // 从远端获取数据
            int rt = mgProductBasicCli.getBindTagDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindGroupDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            int rt = mgProductStoreCli.getSpuBizSummaryDataStatus(aid, tid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getSpuBizSummaryDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getSpuBizSummaryDataStatus,remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }
        return remoteDataStatusInfo;
    }

    public FaiList<Param> getSearchDataAndSearchResultList(int flow, int aid, int tid, int unionPriId, Param searchSorterInfo, FaiList<Param> resultList, MgProductBasicCli mgProductBasicCli, MgProductStoreCli mgProductStoreCli){
        String tableName = searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE);
        // 所有的表都有这个字段，用这个字段作为 filter 的过滤条件
        String searchKey = ProductEntity.Info.PD_ID;
        ParamMatcher searchMatcher = (ParamMatcher) searchSorterInfo.getObject(SearchSorterInfo.SEARCH_MATCHER);
        int dataCount = searchSorterInfo.getInt(SearchSorterInfo.DATA_COUNT);
        int dataLoadFromDbThreshold = getLoadFromDbThreshold(tableName);
        boolean needLoadFromDb = dataCount > dataLoadFromDbThreshold;
        if(!needLoadFromDb && searchMatcher.isEmpty() && !resultList.isEmpty()){
            FaiList<Integer> idList = toIdList(resultList, searchKey);
            searchMatcher.and(searchKey, ParamMatcher.IN, idList);
        }
        boolean needGetDataFromRemote = searchSorterInfo.getBoolean(SearchSorterInfo.NEED_GET_DATA_FROM_REMOTE);
        // 直接从缓存中拿
        if(!needGetDataFromRemote){
            // ParamListCache1 localMgProductSearchData = getLocalMgProductSearchDataCache(unionPriId);
            ParamListCache1 localMgProductSearchData = MgProductSearchCache.getLocalMgProductSearchDataCache(unionPriId);
            String cacheKey = MgProductSearchCache.getLocalMgProductSearchDataCacheKey(aid, tableName);
            if(localMgProductSearchData.containsKey(cacheKey)){
                FaiList<Param> searchList = localMgProductSearchData.get(cacheKey);
                searchSorterInfo.setList(SearchSorterInfo.SEARCH_DATA_LIST, searchList);
                resultList = getSearchResult(searchMatcher, searchList, resultList, searchKey);
                //Log.logDbg("needGetDataFromRemote=%s;tableName=%s;searchDataList=%s;", needGetDataFromRemote, tableName, searchSorterInfo.getList(SearchSorterInfo.SEARCH_DATA_LIST));
                return resultList;
            }else{
                // 可能缓存被回收了，乐观点，重新获取数据吧
                // searchSorterInfo.setList(SearchSorterInfo.SEARCH_DATA_LIST, new FaiList<Param>());
                Log.logStd("cache1==null; flow=%s;aid=%d;unionPriId=%d;searchSorterInfo=%s;", flow, aid, unionPriId, searchSorterInfo);
            }
        }

        // 需要发包获取数据
        SearchArg searchArg = new SearchArg();
        searchArg.matcher = searchMatcher;
        // 如果需要发包到svr进行搜索，加上 上一次的搜索结果的 idList
        if(needLoadFromDb && !resultList.isEmpty()){
            FaiList<Integer> idList = toIdList(resultList, searchKey);
            searchArg.matcher.and(searchKey, ParamMatcher.IN, idList);
        }
        FaiList<Param> searchDataList = new FaiList<Param>();   // 需要真正获取的数据

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchPdFromDb(aid, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchPdFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchPdFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllPdData(aid, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllPdData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllPdData, searchDataList=%s;", searchDataList);
            }
        }


        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchPdRelFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchPdRelFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchPdRelFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllPdRelData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllPdRelData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllPdRelData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchBindPropFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindPropFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllBindPropData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindPropData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindPropData, searchDataList=%s;aid=%s;unionPriId=%s;", searchDataList, aid, unionPriId);
            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductBasicCli.searchBindGroupFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchBindGroupFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindGroupFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductBasicCli.getAllBindGroupData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindGroupData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindGroupData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
            }else{

            }
        }

        if(MgProductSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // 从远端获取数据, 待完善
            if(needLoadFromDb){
                int rt = mgProductStoreCli.searchSpuBizSummaryFromDb(aid, tid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchSpuBizSummaryFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchSpuBizSummaryFromDb, searchDataList=%s;", searchDataList);
            }else{
                int rt = mgProductStoreCli.getSpuBizSummaryAllData(aid, tid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getSpuBizSummaryAllData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getSpuBizSummaryAllData, searchDataList=%s;", searchDataList);
            }
        }
        searchSorterInfo.setList(SearchSorterInfo.SEARCH_DATA_LIST, searchDataList);

        // 全量 load 数据时，才做本地缓存
        if(!needLoadFromDb){
            // 进行搜索
            resultList = getSearchResult(searchMatcher, searchDataList, resultList, searchKey);

            // 设置 各个表 的全量本地缓存
            //ParamListCache1 localMgProductSearchData = getLocalMgProductSearchDataCache(unionPriId);
            //localMgProductSearchData.put(getLocalMgProductSearchDataCacheKey(aid, tableName), searchDataList);
            ParamListCache1 localMgProductSearchData = MgProductSearchCache.getLocalMgProductSearchDataCache(unionPriId);
            localMgProductSearchData.put(MgProductSearchCache.getLocalMgProductSearchDataCacheKey(aid, tableName), searchDataList);


            // 设置 各个表本地 缓存的时间
            Param dataStatusInfo = new Param();
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.TOTAL_SIZE);

            //m_localDataStatusCache.put(getDataStatusCacheKey(aid, unionPriId, tableName), dataStatusInfo);
            String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, tableName);
            MgProductSearchCache.LocalDataStatusCache.addLocalDataStatusCache(cacheKey, dataStatusInfo);
        }else{
            // 后面上线后需要干掉getSearchResult(searchMatcher, searchDataList, resultList, searchKey) 调用，
            // 因为已经 把 matcher 放到 client 中，远端已经进行过了搜索
            //resultList = getSearchResult(searchMatcher, searchDataList, resultList, searchKey);
            resultList = searchDataList;
        }
        //Log.logDbg("needGetDataFromRemote=%s;needLoadFromDb=%s;tableName=%s;dataCount=%s;dataLoadFromDbThreshold=%s;resultList=%s;", needGetDataFromRemote, needLoadFromDb, tableName, dataCount, dataLoadFromDbThreshold, resultList);
        return resultList;
    }

    // 根据指定的 key，把 FaiList 转换为 map
    public static HashMap<Integer, Param> faiListToHashMap(FaiList<Param> resultList, String key){
        HashMap<Integer, Param> searchHashMap = new HashMap<Integer, Param>();
        for(Param p : resultList){
            searchHashMap.put(p.getInt(key), p);
        }
        return searchHashMap;
    }

    // 根据指定的 key，把 FaiList 转换为 keySet
    public static HashSet<Integer> faiListToHashIdSet(FaiList<Param> resultList, String key){
        HashSet<Integer> searchSetList = new HashSet<Integer>();
        for(Param p : resultList){
            searchSetList.add(p.getInt(key));
        }
        return searchSetList;
    }

    //  各个表从 svr load 数据的排序
    public static int getLoadFromDbThreshold(String tableName){
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Str.isEmpty(conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey))){
            return defaultThreshold;
        }
        return conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey).getInt(tableName, defaultThreshold);
    }

    // 用于从哪个表的数据开始做数据搜索，做各个表的数据量大小排优处理, 由小表的数据到大表的数据做搜索
    public static final class SearchSorterInfo{
        public static final String SEARCH_TABLE = "st";
        public static final String DATA_COUNT = "dc";
        public static final String NEED_GET_DATA_FROM_REMOTE = "ngdfr";
        public static final String SEARCH_DATA_LIST = "sdl";
        public static final String SEARCH_MATCHER = "sm";
    }
}
