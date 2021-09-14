package fai.MgProductSearchSvr.domain.serviceproc;

import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.interfaces.utils.BaseMgProductSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductDbSearch;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductSearchSvr.domain.repository.cache.MgProductSearchCache;
import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

/**
 * @author Lu
 * @date 2021-08-06 9:48
 */
public class MgProductSearchProc {

    /**
     * 根据 ProductEntity.Info.PD_ID 去重
     * @param resultList 去重的集合
     * @param resultListKey 去重的key
     * @return 返回去重后的集合
     */
    public FaiList<Param> removeRepeatedByKey(FaiList<Param> resultList, String resultListKey){
        FaiList<Param> filterList = new FaiList<Param>();
        if(resultList.isEmpty()){
            return filterList;
        }
        // 去重集合
        HashSet<Integer> idSetList = new HashSet<>();
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

    /**
     * 由ParamList 提取业务商品 idList
     * @param resultList 要提取的List
     * @param key 提取的key
     * @return 返回提取后的List
     */
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

    // 可以理解为取resultList和searchList根据resultListKey的交集部分，但是返回的交集部分的结果是在searchList的结果
    // 根据 set 的缓存重新过滤数据
    // 先获取包含在resultList中searchKey对应的数据，保存到Set中。
    // 再遍历searchList，保存searchList中包含在Set中的数据到List中，最后返回List。
    public FaiList<Param> searchListFilterBySearchResultList(FaiList<Param> resultList, String resultListKey, FaiList<Param> searchList, String searchListKey){
        FaiList<Param> filterList = new FaiList<Param>();
        // 转换为 set 的集合
        HashSet<Integer> resultSetList = faiListToHashIdSet(resultList, resultListKey);
        for(Param info : searchList){
            if(resultSetList.contains(info.getInt(searchListKey))){
                filterList.add(info);
            }
        }
        return filterList;
    }

    public FaiList<Param> getSearchResult(ParamMatcher searchMatcher, FaiList<Param> searchList, FaiList<Param> resultList, String searchKey){
        // 非第一次搜索，进入 set<id> 的过滤逻辑，减少搜索集合，提高性能
        if(resultList != null && !resultList.isEmpty()){
            // 先获取包含在resultList中searchKey对应的数据，保存到Set中。
            // 再遍历searchList，保存searchList中包含在Set中的数据到List中，最后返回List。
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

    /**
     * 从远端获取数据
     */
    public Param getDataStatusInfoFromEachSvr(int aid, int unionPriId, int tid, int flow, String tableName,
                                              MgProductBasicCli mgProductBasicCli,
                                              MgProductStoreCli mgProductStoreCli,
                                              MgProductSpecCli mgProductSpecCli){
        Param remoteDataStatusInfo = new Param();
        int rt;
        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName().equals(tableName)){
            // 从远端获取数据, 待完善
            rt = mgProductBasicCli.getPdDataStatus(aid, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getPdDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            // Log.logDbg("getPdDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName().equals(tableName)){
            // 从远端获取数据
            rt = mgProductBasicCli.getPdRelDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getPdRelDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getPdRelDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName().equals(tableName)){
            // 从远端获取数据
            rt = mgProductBasicCli.getBindPropDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindPropDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindPropDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName().equals(tableName)){
            // 从远端获取数据
            rt = mgProductBasicCli.getBindGroupDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindGroupDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName().equals(tableName)){
            // 从远端获取数据
            rt = mgProductBasicCli.getBindTagDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getBindTagDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            //Log.logDbg("getBindGroupDataStatus, remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName().equals(tableName)){
            // 从远端获取数据, 待完善
            rt = mgProductStoreCli.getSpuBizSummaryDataStatus(aid, tid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getSpuBizSummaryDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            // Log.logDbg("getSpuBizSummaryDataStatus,remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName().equals(tableName)){
            // 从远端获取数据, 待完善
            rt = mgProductSpecCli.getSkuCodeDataStatus(aid, unionPriId, remoteDataStatusInfo);
            if(rt != Errno.OK){
                Log.logErr(rt,"getSkuCodeDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
            }
            // Log.logDbg("getSkuCodeDataStatus,remoteDataStatusInfo=%s;", remoteDataStatusInfo);
        }
        return remoteDataStatusInfo;
    }

    /**
     * 返回联合多个表进行查询得到的结果（实际上是逐个表进行查询，然后根据PdId取结果的交集）
     */
    public FaiList<Param> getSearchDataAndSearchResultList(int flow, int aid, int tid, int unionPriId,
                                                           Param searchSorterInfo, FaiList<Param> resultList,
                                                           MgProductBasicCli mgProductBasicCli,
                                                           MgProductStoreCli mgProductStoreCli,
                                                           MgProductSpecCli mgProductSpecCli){
        String tableName = searchSorterInfo.getString(SearchSorterInfo.SEARCH_TABLE);
        // 所有的表都有这个字段，用这个字段作为 filter 的过滤条件
        String searchKey = ProductEntity.Info.PD_ID;
        ParamMatcher searchMatcher = (ParamMatcher) searchSorterInfo.getObject(SearchSorterInfo.SEARCH_MATCHER);
        int dataCount = searchSorterInfo.getInt(SearchSorterInfo.DATA_COUNT);
        int dataLoadFromDbThreshold = getLoadFromDbThreshold(tableName);
        boolean needLoadFromDb = dataCount > dataLoadFromDbThreshold;
        if(!needLoadFromDb && searchMatcher.isEmpty() && !resultList.isEmpty()){
            // 给补偿的搜索表补偿过滤条件
            // 获取resultList中的idList
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
                // 可以理解为先取resultList中对应searchKey的数据和searchList取交集，然后再取searchMatcher中满足条件的。
                resultList = getSearchResult(searchMatcher, searchList, resultList, searchKey);
                // Log.logDbg("needGetDataFromRemote=%s;tableName=%s;searchDataList=%s;", needGetDataFromRemote, tableName, searchSorterInfo.getList(SearchSorterInfo.SEARCH_DATA_LIST));
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
            // 如果是按条件搜索db，就要加上前面搜索结果的PdId作为搜索的条件
            FaiList<Integer> idList = toIdList(resultList, searchKey);
            searchArg.matcher.and(searchKey, ParamMatcher.IN, idList);
        }
        // 需要真正获取的数据
        FaiList<Param> searchDataList = getDataFromRemote(flow, aid, tid, unionPriId, tableName, needLoadFromDb, searchArg, mgProductBasicCli, mgProductStoreCli, mgProductSpecCli);
        searchSorterInfo.setList(SearchSorterInfo.SEARCH_DATA_LIST, searchDataList);

        // 全量 load 数据时，才做本地缓存
        if(!needLoadFromDb){
            // 进行搜索
            resultList = getSearchResult(searchMatcher, searchDataList, resultList, searchKey);

            // 设置各个表的全量本地缓存
            ParamListCache1 localMgProductSearchData = MgProductSearchCache.getLocalMgProductSearchDataCache(unionPriId);
            localMgProductSearchData.put(MgProductSearchCache.getLocalMgProductSearchDataCacheKey(aid, tableName), searchDataList);

            // 设置各个表的本地缓存时间
            Param dataStatusInfo = new Param();
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
            dataStatusInfo.assign(searchSorterInfo, DataStatus.Info.TOTAL_SIZE);

            String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, tableName);
            MgProductSearchCache.LocalDataStatusCache.addLocalDataStatusCache(cacheKey, dataStatusInfo);
        } else {
            // TODO 需要注意的一点就是，按照条件进行搜索的时候，要看搜索结果的字段是否包含排序的字段，不然也排不了序

            // 后面上线后需要干掉getSearchResult(searchMatcher, searchDataList, resultList, searchKey) 调用，
            // 因为已经 把 matcher 放到 client 中，远端已经进行过了搜索
            //resultList = getSearchResult(searchMatcher, searchDataList, resultList, searchKey);
            resultList = searchDataList;
        }
        //Log.logDbg("needGetDataFromRemote=%s;needLoadFromDb=%s;tableName=%s;dataCount=%s;dataLoadFromDbThreshold=%s;resultList=%s;", needGetDataFromRemote, needLoadFromDb, tableName, dataCount, dataLoadFromDbThreshold, resultList);
        return resultList;
    }

    public FaiList<Param> getDataFromRemote(int flow, int aid, int tid, int unionPriId,
                                            String tableName, boolean needLoadFromDb, SearchArg searchArg,
                                            MgProductBasicCli mgProductBasicCli,
                                            MgProductStoreCli mgProductStoreCli,
                                            MgProductSpecCli mgProductSpecCli) {
        int rt;
        // 需要真正获取的数据
        FaiList<Param> searchDataList = new FaiList<>();
        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName().equals(tableName)){
            if(needLoadFromDb){
                rt = mgProductBasicCli.searchPdFromDb(aid, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchPdFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchPdFromDb, searchDataList=%s;", searchDataList);
            }else{
                rt = mgProductBasicCli.getAllPdData(aid, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllPdData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllPdData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName().equals(tableName)){
            if(needLoadFromDb){
                rt = mgProductBasicCli.searchPdRelFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchPdRelFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchPdRelFromDb, searchDataList=%s;", searchDataList);
            }else{
                rt = mgProductBasicCli.getAllPdRelData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllPdRelData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllPdRelData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName().equals(tableName)){
            if(needLoadFromDb){
                rt = mgProductBasicCli.searchBindPropFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getBindGroupDataStatus err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindPropFromDb, searchDataList=%s;", searchDataList);
            }else{
                rt = mgProductBasicCli.getAllBindPropData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindPropData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindPropData, searchDataList=%s;aid=%s;unionPriId=%s;", searchDataList, aid, unionPriId);
            }
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName().equals(tableName)){
            if(needLoadFromDb){
                rt = mgProductBasicCli.searchBindGroupFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchBindGroupFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindGroupFromDb, searchDataList=%s;", searchDataList);
            }else{
                rt = mgProductBasicCli.getAllBindGroupData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindGroupData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindGroupData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName().equals(tableName)){
            if(needLoadFromDb){
                rt = mgProductBasicCli.getPdBindTagFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchBindTagFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchBindGroupFromDb, searchDataList=%s;", searchDataList);
            }else{
                rt = mgProductBasicCli.getAllPdBindTag(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getAllBindTagData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getAllBindGroupData, searchDataList=%s;", searchDataList);
            }
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName().equals(tableName)){
            if(needLoadFromDb){
                rt = mgProductStoreCli.searchSpuBizSummaryFromDb(aid, tid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchSpuBizSummaryFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchSpuBizSummaryFromDb, searchDataList=%s;", searchDataList);
            }else{
                rt = mgProductStoreCli.getSpuBizSummaryAllData(aid, tid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getSpuBizSummaryAllData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getSpuBizSummaryAllData, searchDataList=%s;", searchDataList);
            }
        }

        // 目前提供的接口查询结果
        // 只有这三个字段
        // Info.SKU_CODE,
        // Info.PD_ID,
        // Info.SKU_ID
        if (MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName().equals(tableName)) {
            if(needLoadFromDb){
                rt = mgProductSpecCli.searchSkuCodeFromDb(aid, unionPriId, searchArg, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"searchSkuCodeFromDb err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("searchSkuCodeFromDb, searchDataList=%s;", searchDataList);
            }else{
                rt = mgProductSpecCli.getSkuCodeAllData(aid, unionPriId, searchDataList);
                if(rt != Errno.OK){
                    Log.logErr(rt,"getSkuCodeAllData err, aid=%d;unionPriId=%d;flow=%d;", aid, unionPriId, flow);
                }
                //Log.logDbg("getSkuCodeAllData, searchDataList=%s;", searchDataList);
            }
        }
        return searchDataList;
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

        return conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey)
            .getInt(tableName, defaultThreshold);
    }

    // 使用es中获取到PdId使用In Sql的阈值。
    public int getInSqlThreshold() {
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Objects.isNull(conf.getInt(MgProductSearchSvr.SvrConfigGlobalConf.useIdFromEsAsInSqlThresholdKey))){
            return defaultThreshold;
        }
        return conf.getInt(MgProductSearchSvr.SvrConfigGlobalConf.useIdFromEsAsInSqlThresholdKey, defaultThreshold);
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
