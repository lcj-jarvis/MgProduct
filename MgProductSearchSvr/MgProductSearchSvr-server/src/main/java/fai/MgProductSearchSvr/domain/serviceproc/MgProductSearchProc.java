package fai.MgProductSearchSvr.domain.serviceproc;

import fai.MgProductBasicSvr.interfaces.cli.async.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductEntity;
import fai.MgProductInfSvr.interfaces.utils.BaseMgProductSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductDbSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductEsSearch;
import fai.MgProductInfSvr.interfaces.utils.MgProductSearchResult;
import fai.MgProductSearchSvr.application.MgProductSearchSvr;
import fai.MgProductSearchSvr.domain.comm.ParseData;
import fai.MgProductSearchSvr.domain.repository.MgProductSearchCache;
import fai.MgProductSpecSvr.interfaces.cli.async.MgProductSpecCli;
import fai.MgProductStoreSvr.interfaces.cli.async.MgProductStoreCli;
import fai.comm.jnetkit.server.fai.RemoteStandResult;
import fai.comm.rpc.client.DefaultFuture;
import fai.comm.rpc.client.FaiClientProxyFactory;
import fai.comm.util.*;
import fai.mgproduct.comm.DataStatus;
import fai.middleground.svrutil.exception.MgException;
import sun.plugin.com.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import static fai.MgProductInfSvr.interfaces.utils.BaseMgProductSearch.BaseSearchInfo.SEARCH_KEYWORD;
import static fai.MgProductSearchSvr.domain.serviceproc.MgProductSearchProc.DbSearchSorterInfo.*;

/**
 * @author Lu
 * @date 2021-08-06 9:48
 */
public class MgProductSearchProc {

    /**
     * 异步获取“es数据来源的表”对应的管理态和访客态最新的修改时间
     * @param manageDataMaxChangeTime 管理态最新的修改时间
     * @param visitorDataMaxChangeTime 访客态最新的修改时间
     */
    public void asyncGetEsDataStatus(int flow, int aid, int unionPriId, Ref<Long> manageDataMaxChangeTime, Ref<Long> visitorDataMaxChangeTime) {
        // final避免后面改到，ConcurrentSkipListSet，相当于线程安全的TreeSet，可以避免加锁
        final ConcurrentSkipListSet<Long> manageDataChangeTimeSet = new ConcurrentSkipListSet<>();
        final ConcurrentSkipListSet<Long> visitorDataChangeTimeSet = new ConcurrentSkipListSet<>();

        // 目前es的数据来源与商品表和商品业务表，所以搜索这两张表的数据状态
        MgProductBasicCli asyncMgProductBasicCli = FaiClientProxyFactory.createProxy(MgProductBasicCli.class);

        // 异步获取数据状态
        DefaultFuture pdDataStatus = asyncMgProductBasicCli.getPdDataStatus(flow, aid);
        DefaultFuture pdRelDataStatus = asyncMgProductBasicCli.getPdRelDataStatus(flow, aid, unionPriId);

        Map<String, CompletableFuture> tableNameMappingFuture = new HashMap<>(16);
        tableNameMappingFuture.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName(), pdDataStatus);
        tableNameMappingFuture.put(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName(), pdRelDataStatus);

        CountDownLatch countDownLatch = new CountDownLatch(tableNameMappingFuture.size());
        tableNameMappingFuture.forEach((tableName, dataStatusFuture) -> {
            // 异步回调获取结果
            dataStatusFuture.whenComplete((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess()) {
                    Integer getParamKey = ParseData.TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.get(tableName);
                    Param statusInfo = result.getObject(getParamKey, Param.class);
                    manageDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME));
                    visitorDataChangeTimeSet.add(statusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
                } else {
                    int rt = result.getRt();
                    throw new MgException(rt, "es get " + tableName + " table dataStatus error;flow=%d,aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                }
                Log.logStd("finish es get " + tableName + " table dataStatus;flow=%d,aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                // 回调完成就减一
                countDownLatch.countDown();
            });
        });

        try {
            // 同步阻塞等待回调执行完成
            countDownLatch.await();
        } catch (Exception e) {
            throw new MgException(Errno.ERROR, "waiting get es dataStatus time out;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
        }

        manageDataMaxChangeTime.value = manageDataChangeTimeSet.last();
        visitorDataMaxChangeTime.value = visitorDataChangeTimeSet.last();
    }

    /**
     * 异步获取SearchKeyword搜索条件所在的表的数据状态
     */
    public void asyncGetSearchKeyWordTableDataStatus(int flow, int aid, int unionPriId, int tid,
                                                     MgProductDbSearch mgProductDbSearch,
                                                     Map<String, CompletableFuture> searchKeyWordTable_dataStatusFuture,
                                                     Map<String, ParamMatcher> searchKeyWordTable_searchMatcher,
                                                     MgProductBasicCli asyncMgProductBasicCli,
                                                     MgProductStoreCli asyncMgProductStoreCli,
                                                     MgProductSpecCli asyncMgProductSpecCli) {
        // 关键字用作商品名称搜索
        ParamMatcher searchMatcher = mgProductDbSearch.getProductBasicSkwSearchMatcher(null);
        String tableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName();
        if (!searchMatcher.isEmpty()) {
            Log.logStd("use searchkeyword in " + tableName +" table search;get " + tableName + " table dataStatus;flow=%d;aid=%d,unionPriId=%d", flow, aid, unionPriId);
            searchKeyWordTable_searchMatcher.put(tableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, tableName, searchKeyWordTable_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }

        // 关键字用作条形码搜索
        searchMatcher = mgProductDbSearch.getProductSpecSkuCodeSkwSearchMatcher(null);
        tableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName();
        if (!searchMatcher.isEmpty()) {
            Log.logStd("use searchkeyword in " + tableName +" table search;get " + tableName + " table dataStatus;flow=%d;aid=%d,unionPriId=%d", flow, aid, unionPriId);
            searchKeyWordTable_searchMatcher.put(tableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, tableName, searchKeyWordTable_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }

    }

    public void asyncGetSearchTableDataStatus(int flow, int aid, int unionPriId, int tid,
                                              MgProductDbSearch mgProductDbSearch,
                                              MgProductEsSearch mgProductEsSearch,
                                              Map<String, CompletableFuture> table_dataStatusFuture,
                                              Map<String, ParamMatcher> table_searchMatcher,
                                              MgProductBasicCli asyncMgProductBasicCli,
                                              MgProductStoreCli asyncMgProductStoreCli,
                                              MgProductSpecCli asyncMgProductSpecCli) {

        // 1、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
        ParamMatcher searchMatcher = mgProductDbSearch.getProductBindPropSearchMatcher(null);
        String searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName();
        if (!searchMatcher.isEmpty()) {
            table_searchMatcher.put(searchTableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }

        // 2、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductSpuBizSummarySearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName();
        if (!searchMatcher.isEmpty()) {
            table_searchMatcher.put(searchTableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }

        // 3、"商品与标签关联表" mgProductBindTag_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductBindTagSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName();
        if (!searchMatcher.isEmpty()) {
            table_searchMatcher.put(searchTableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }

        // 4、在 "商品与分类关联表" mgProductBindGroup_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductBindGroupSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName();
        if (!searchMatcher.isEmpty()) {
            table_searchMatcher.put(searchTableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }

        // 5、在 "商品业务关系表" mgProductRel_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductRelSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName();
        if (mgProductDbSearch.isEmpty()) {
            // 执行到这里说明es和db的搜索条件都为空.因为es搜索条件不为空，db搜索条件为空的情况，在前面已经直接返回es里的搜索结果了。
            // es和db的搜索条件都为空，就补充一张MG_PRODUCT_REL作为空搜索条件的表
            table_searchMatcher.put(searchTableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        } else {
            // 执行到这里说明es的搜索条件为空，db的搜索条件不为空，或者es的不为空，db的搜索条件不为空

            // 只要es的搜索条件不为空，都要去检查es数据来源的表的数据状态，目前es数据来源于商品表，商品业务表
            if (searchMatcher.isEmpty()) {
                if (mgProductEsSearch != null && !mgProductEsSearch.isEmpty()) {
                    // 只需要获取数据状态的信息，不需要设置搜索
                   // table_SearchMatcher.put(searchTableName, searchMatcher);
                    asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
                }
            } else {
                table_searchMatcher.put(searchTableName, searchMatcher);
                asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
            }
        }

        // 6、在 "商品基础表" mgProduct_xxxx 搜索
        searchMatcher = mgProductDbSearch.getProductBasicSearchMatcher(null);
        searchTableName = MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName();
        // 只要es的搜索条件不为空，都要去检查es数据来源的表的数据状态，目前es数据来源于商品表，商品业务表
        if (searchMatcher.isEmpty()) {
            if (mgProductEsSearch != null && !mgProductEsSearch.isEmpty()) {
                // 只需要获取数据状态的信息，不需要设置搜索
                // table_SearchMatcher.put(searchTableName, searchMatcher);
                asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
            }
        } else {
            table_searchMatcher.put(searchTableName, searchMatcher);
            asyncGetDataStatus(flow, aid, unionPriId, tid, searchTableName, table_dataStatusFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
        }
    }

    /**
     * 回调获取各个表的数据状态
     * @param tableNameMappingFuture 保存了表名映射获取数据状态的DefaultFuture
     * @param tableNameMappingSearchMatcher 保存了表名映射对应的搜索条件
     * @param manageDataChangeTimeSet 用于保存每张表关联态的修改时间
     * @param visitorDataChangeTimeSet 用于保存每张表访客态的修改时间
     * @param tableMappingSearchAndSortInfo 表名映射“包含搜索和排序等信息的Param”
     */
    public void callbackGetDataStatus(int flow, int aid, int unionPriId,
                                      MgProductDbSearch mgProductDbSearch,
                                      Map<String, CompletableFuture> tableNameMappingFuture,
                                      Map<String, ParamMatcher> tableNameMappingSearchMatcher,
                                      final Set<Long> manageDataChangeTimeSet,
                                      final Set<Long> visitorDataChangeTimeSet,
                                      final Map<String, Param> tableMappingSearchAndSortInfo,
                                      CountDownLatch countDownLatch) {
        /**
         * 为什么不用CompletableFuture的allOf方法呢，而用CountDownLatch呢?
         * 因为CompletableFuture的allOf方法只是等待所有的CompletableFuture返回结果就停止阻塞，
         * 而不是等待CompletableFuture的whenComplete回调执行完才停止阻塞。
         */
        tableNameMappingFuture.forEach((tableName, defaultFuture) -> {
            defaultFuture.whenComplete((BiConsumer<RemoteStandResult, Throwable>) (result, ex) -> {
                if (result.isSuccess()) {

                    Log.logStd(" begin to callback get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                    // 获取缓存在本地的数据状态
                    String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, tableName);
                    Param localDataStatusCacheInfo = MgProductSearchCache.LocalDataStatusCache.getLocalDataStatusCache(cacheKey);

                    // 获取远程的数据状态
                    Integer getParamKey = ParseData.TABLE_NAME_MAPPING_PARSE_DATA_STATUS_KEY.get(tableName);
                    Param remoteDataStatusInfo = result.getObject(getParamKey, Param.class);

                    // 该表对应的查询条件
                    ParamMatcher matcher = tableNameMappingSearchMatcher.get(tableName);
                    initSearchAndSortInfoTask(tableName, matcher, localDataStatusCacheInfo, remoteDataStatusInfo, mgProductDbSearch, manageDataChangeTimeSet, visitorDataChangeTimeSet, tableMappingSearchAndSortInfo);

                    Log.logStd(" finish getting " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                    // countDownLatch减一
                    countDownLatch.countDown();
                } else {
                    int rt = result.getRt();
                    throw new MgException(rt, "get " + tableName + " table dataStatus error;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                }
            });
        });
    }

    /**
     * 根据tableName异步获取该表的数据状态
     * @param tableName 要获取数据状态的表
     * @param tableNameMappingFuture 表名映射获取数据状态的DefaultFuture
     */
    public void asyncGetDataStatus(int flow, int aid, int unionPriId, int tid,
                                    String tableName,
                                    Map<String, CompletableFuture> tableNameMappingFuture,
                                    MgProductBasicCli asyncMgProductBasicCli,
                                    MgProductStoreCli asyncMgProductStoreCli,
                                    MgProductSpecCli asyncMgProductSpecCli) {
        // 从远端获取数据
        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture pdDaStatus = asyncMgProductBasicCli.getPdDataStatus(flow, aid);
            tableNameMappingFuture.put(tableName, pdDaStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture pdRelDataStatus = asyncMgProductBasicCli.getPdRelDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, pdRelDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture bindPropDataStatus = asyncMgProductBasicCli.getBindPropDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, bindPropDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture bindGroupDataStatus = asyncMgProductBasicCli.getBindGroupDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, bindGroupDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture bindTagDataStatus = asyncMgProductBasicCli.getBindTagDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, bindTagDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture spuBizSummaryDataStatus = asyncMgProductStoreCli.getSpuBizSummaryDataStatus(flow, aid, tid, unionPriId);
            tableNameMappingFuture.put(tableName, spuBizSummaryDataStatus);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table dataStatus;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
            DefaultFuture skuCodeDataStatus = asyncMgProductSpecCli.getSkuCodeDataStatus(flow, aid, unionPriId);
            tableNameMappingFuture.put(tableName, skuCodeDataStatus);
        }
    }

    /**
     * 初始化每张表的搜索、排序、数据状态等信息，并保存这些信息到tableMappingSearchAndSortInfo中
     * @param tableName 表名
     * @param searchMatcher 该表对应的搜索条件
     * @param localDataStatusCacheInfo 从本地缓存获取的数据状态
     * @param remoteDataStatusInfo 从远程获取的数据状态
     * @param manageDataChangeTimeSet 用于保存每张表关联态的修改时间
     * @param visitorDataChangeTimeSet 用于保存每张表访客态的修改时间
     * @param tableMappingSearchAndSortInfo 表名映射“包含搜索和排序等信息的Param”
     */
    public void initSearchAndSortInfoTask(String tableName,
                                          ParamMatcher searchMatcher,
                                          Param localDataStatusCacheInfo,
                                          Param remoteDataStatusInfo,
                                          MgProductDbSearch mgProductDbSearch,
                                          final Set<Long> manageDataChangeTimeSet,
                                          final Set<Long> visitorDataChangeTimeSet,
                                          final Map<String, Param> tableMappingSearchAndSortInfo) {
        // 首先判断本地缓存的数据和状态
        boolean needGetDataFromRemote = false;
        boolean isOnlySearchManageData = false;
        if(!Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            // 是否只查管理态的数据
            isOnlySearchManageData = mgProductDbSearch.getIsOnlySearchManageData(tableName);
            /*
                （1）管理态数据变动，影响所有的缓存, 因为管理变动可能会导致访客的数据变动
                （2）如果有搜索访客字段，并且是访客字段时间有变动，需要 reload 数据
                 以上两种情况满足其中一个就行
             */
            needGetDataFromRemote = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME) ||
                (!isOnlySearchManageData && localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME) < remoteDataStatusInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
            if (needGetDataFromRemote) {
                localDataStatusCacheInfo = remoteDataStatusInfo;
            }
        } else if (Str.isEmpty(localDataStatusCacheInfo) && !Str.isEmpty(remoteDataStatusInfo)){
            // 本地没有数据，如果进入搜索逻辑，则需要重新reload数据，赋值到新的 cache
            localDataStatusCacheInfo = remoteDataStatusInfo;
            needGetDataFromRemote = true;
        } else if (Str.isEmpty(localDataStatusCacheInfo) && Str.isEmpty(remoteDataStatusInfo)){
            throw new MgException(Errno.ERROR, "searchTableName=%s;localDataStatusCacheInfo == null && remoteDataStatusInfo == null err", tableName);
        }

        // 这里的Set为ConcurrentSkipListSet，相当于线程安全的TreeSet，不用加锁
        manageDataChangeTimeSet.add(localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME));
        if (!isOnlySearchManageData) {
            // 这里的Set为ConcurrentSkipListSet，相当于线程安全的TreeSet，不用加锁
            visitorDataChangeTimeSet.add(localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME));
        }
        int dataAllSize = localDataStatusCacheInfo.getInt(DataStatus.Info.TOTAL_SIZE);
        long manageDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
        long visitorDataUpdateTime = localDataStatusCacheInfo.getLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME);

        // 因为如果该表的searchMatcher为null，说明es有搜索条件，db没有搜索这张表，但是该表又是es数据来源的表，
        // 只是需要获取数据状态来判断缓存是否失效。所以只需要执行到获取管理态和访客态的时间
        if (searchMatcher != null) {
            // 设置需要搜索的信息
            initTableMappingSearchAndSortInfo(tableMappingSearchAndSortInfo, dataAllSize, manageDataUpdateTime, visitorDataUpdateTime, tableName, needGetDataFromRemote, searchMatcher);
        }
    }

    /**
     * 设置需要搜索的信息和排序
     * @param tableMappingSearchAndSortInfo 线程安全的Map,表名映射“包含搜索和排序等信息的Param”
     * @param dataAllSize 数据的总记录数
     * @param manageDataUpdateTime 管理态最新的更新时间
     * @param visitorDataUpdateTime 访客态最新的更新时间
     * @param searchTableName 搜索的表
     * @param needGetDataFromRemote 是否需要调用远程的cli从db获取数据
     * @param searchMatcher 搜索的条件
     */
    private void initTableMappingSearchAndSortInfo(final Map<String, Param> tableMappingSearchAndSortInfo, int dataAllSize, long manageDataUpdateTime, long visitorDataUpdateTime, String searchTableName, boolean needGetDataFromRemote, ParamMatcher searchMatcher) {
        Param info = new Param();
        info.setString(DbSearchSorterInfo.SEARCH_TABLE, searchTableName);
        info.setInt(DbSearchSorterInfo.DATA_COUNT, dataAllSize);
        info.setBoolean(DbSearchSorterInfo.NEED_GET_DATA_FROM_REMOTE, needGetDataFromRemote);
        info.setLong(DataStatus.Info.MANAGE_LAST_UPDATE_TIME, manageDataUpdateTime);
        info.setLong(DataStatus.Info.VISITOR_LAST_UPDATE_TIME, visitorDataUpdateTime);
        info.setInt(DataStatus.Info.TOTAL_SIZE, dataAllSize);
        info.setObject(SEARCH_MATCHER, searchMatcher);
        // 线程安全的Map
        tableMappingSearchAndSortInfo.put(searchTableName, info);
    }

    /**
     * 整合并添加搜索结果到缓存
     * @param idList 缓存的pdId List
     * @param manageDataCacheTime 管理态数据最新的修改时间
     * @param visitDataCacheTime  访客态数据的最新修改时间
     * @param cacheKey 缓存的key
     * @return 返回包含缓存结果的Param
     */
    public Param integrateAndAddCache(FaiList<Integer> idList, long total, Long manageDataCacheTime, Long visitDataCacheTime, String cacheKey) {
        // 缓存的数据
        Param resultCacheInfo = new Param();
        // 分页后的结果
        resultCacheInfo.setList(MgProductSearchResult.Info.ID_LIST, idList);
        // 搜索结果的总条数
        resultCacheInfo.setLong(MgProductSearchResult.Info.TOTAL, total);
        resultCacheInfo.setLong(MgProductSearchResult.Info.MANAGE_DATA_CACHE_TIME, manageDataCacheTime);
        resultCacheInfo.setLong(MgProductSearchResult.Info.VISTOR_DATA_CACHE_TIME, visitDataCacheTime);
        // 缓存处理，先删除再添加
        MgProductSearchCache.ResultCache.delCache(cacheKey);
        MgProductSearchCache.ResultCache.addCacheInfo(cacheKey, resultCacheInfo);
        return resultCacheInfo;
    }

    /**
     *  作用
     * （1）如果es中搜索过的字段，db中不再搜索
     */
    public void processSearchArgs(Param esSearchParam, Param dbSearchParam) {
        // 避免NPE
        if (esSearchParam == null) {
            esSearchParam = new Param();
        }
        if (dbSearchParam == null) {
            dbSearchParam = new Param();
        }

        // 如果关键词已经在es用做了商品名称搜索，就不在db中用作商品名称搜索了
        boolean useSearchKeywordAsPdNameSearchInEs = esSearchParam.containsKey(SEARCH_KEYWORD) &&
            esSearchParam.getBoolean(BaseMgProductSearch.BaseSearchInfo.ENABLE_SEARCH_PRODUCT_NAME, false);
        if (useSearchKeywordAsPdNameSearchInEs) {
            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.ENABLE_SEARCH_PRODUCT_NAME);
        }

        // TODO 满足门店
        // 将es的其他条件，整合到db
        boolean hasSearchKeywordSearch = !esSearchParam.isEmpty() && !Str.isEmpty(esSearchParam.getString(SEARCH_KEYWORD));
        if (hasSearchKeywordSearch && !dbSearchParam.isEmpty()) {
            dbSearchParam.setInt(BaseMgProductSearch.BaseSearchInfo.UP_SALES_STATUS, esSearchParam.getInt(BaseMgProductSearch.BaseSearchInfo.UP_SALES_STATUS));
        }

        // 不移除以下代码的话：自定义排序 > db里设置的第一排序 > db里设置的第二排序 > es里设置的排序
        // 根据看情况要不要移除吧，目前先移除
        // 判断第一排序或者第二排序是否在es中
//        boolean sortKeyInEs = esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY) ||
//            esSearchParam.containsKey(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
//        if (sortKeyInEs) {
//            // 如果第一排序或者第二排序在es，则移除db里的第一排序和第二排序
//            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.FIRST_COMPARATOR_KEY);
//            dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.FIRST_COMPARATOR_TABLE);
//            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING);
//            dbSearchParam.remove(BaseMgProductSearch.BaseSearchInfo.SECOND_COMPARATOR_KEY);
//            dbSearchParam.remove(MgProductDbSearch.DbSearchInfo.SECOND_COMPARATOR_TABLE);
//        }
    }

    /**
     * TODO 【注意】搜索结果返回的只是部分字段，如果要排序，要注意排序字段是否存在搜索结果中
     * 从远端异步获取数据
     * @param tableName 搜索的表
     * @param searchArg 搜索条件
     * @param addDataToLocalCache true表示全量load数据，并将数据到本地缓存
     * @param tableNameMappingRemoteGetDataFuture 表名映射“从远程获取数据的异步任务”
     */
    private void asyncGetDataFromRemote(int flow, int aid, int tid, int unionPriId,
                                       String tableName, SearchArg searchArg, boolean addDataToLocalCache,
                                       Map<String, CompletableFuture> tableNameMappingRemoteGetDataFuture,
                                       MgProductBasicCli asyncMgProductBasicCli,
                                       MgProductStoreCli asyncMgProductStoreCli,
                                       MgProductSpecCli asyncMgProductSpecCli) {

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllPdData(flow, aid) : asyncMgProductBasicCli.searchPdFromDb(flow, aid, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdRelDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllPdRelData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchPdRelFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdRelDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdBindPropDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllBindPropData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchBindPropFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdBindPropDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdBindGroupDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllBindGroupData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchBindGroupFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdBindGroupDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture pdBindTagDataFuture = addDataToLocalCache ? asyncMgProductBasicCli.getAllPdBindTagData(flow, aid, unionPriId) : asyncMgProductBasicCli.searchBindTagFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, pdBindTagDataFuture);
        }

        if(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.getSearchTableName().equals(tableName)){
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%s;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture spuBizSummaryDataFuture = addDataToLocalCache ? asyncMgProductStoreCli.getSpuBizSummaryAllData(flow, aid, tid, unionPriId) : asyncMgProductStoreCli.searchSpuBizSummaryFromDb(flow, aid, tid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, spuBizSummaryDataFuture);
        }

        // 目前提供的接口查询结果，只有这三个字段，Info.SKU_CODE, Info.PD_ID, Info.SKU_ID
        if (MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.getSearchTableName().equals(tableName)) {
            Log.logStd("begin to send async request to get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;getAllData=%d;", flow, aid, unionPriId, addDataToLocalCache);
            DefaultFuture skuCodeDataFuture = addDataToLocalCache ? asyncMgProductSpecCli.getSkuCodeAllData(flow, aid, unionPriId) : asyncMgProductSpecCli.searchSkuCodeFromDb(flow, aid, unionPriId, searchArg);
            tableNameMappingRemoteGetDataFuture.put(tableName, skuCodeDataFuture);
        }
    }

    public FaiList<Param> getSearchResult(String tableName, ParamMatcher searchMatcher,
                                          FaiList<Param> localCacheDataList,
                                          Map<String, Map<Integer, Param>> tableNameMappingPdIdParam) {

        // 用搜索条件过滤后的结果
        FaiList<Param> searchResult = new FaiList<>();
        prepareData(tableName, searchMatcher, localCacheDataList, searchResult, tableNameMappingPdIdParam);
        return searchResult;
    }

    public void prepareData(String tableName, ParamMatcher searchMatcher,
                            FaiList<Param> dataList,
                            FaiList<Param> recvSearchResult,
                            Map<String, Map<Integer, Param>> tableNameMappingPdIdParam) {
        // 目前pdId 和 排序字段都是一对一的场景
        Map<Integer, Param> pdIdMappingParam = new HashMap<>(dataList.size());

        for (Param info : dataList) {
            // 使用数据量小做全量缓存的时候才用搜索条件过滤
            if (searchMatcher != null && !searchMatcher.isEmpty() && !searchMatcher.match(info)) {
                continue;
            }

            Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
            if (!pdIdMappingParam.containsKey(pdId)) {
                pdIdMappingParam.put(pdId, info);
            }

            if (recvSearchResult != null) {
                recvSearchResult.add(info);
            }
        }

        // 主要用于排序和取交集
        tableNameMappingPdIdParam.put(tableName, pdIdMappingParam);
    }


    public void asyncGetData(int flow, int aid, int tid, int unionPriId,
                             final Map<String, Param> searchKeyWordTable_searchInfo,
                             Map<String, Map<Integer, Param>> searchKeywordTableMappingPdIdParam,
                             final Map<String, Param> table_searchInfo,
                             Map<String, Map<Integer, Param>> tableMappingPdIdParam,
                             final Map<String, Param> comparatorTable_searchInfo,
                             Map<String, Map<Integer, Param>> comparatorTableMappingPdIdParam,
                             MgProductBasicCli asyncMgProductBasicCli,
                             MgProductStoreCli asyncMgProductStoreCli,
                             MgProductSpecCli asyncMgProductSpecCli) {

        // key：searchKeyword（关键词）表名  value:“从远程获取数据的异步任务”
        Map<String, CompletableFuture> searchKeywordTableMappingRemoteGetDataFuture = new HashMap<>(16);
        // key:searchKeyword(关键词)表名   value:“从本地缓存获取数据异步任务”
        Map<String, CompletableFuture> searchKeywordTableMappingLocalGetDataFuture = new HashMap<>(16);
        // 异步获取searchKeyWord所在的表的数据
        searchKeyWordTable_searchInfo.forEach((searchTable, searchInfo) -> {
            asyncGetDataFromLocalOrRemote(flow, aid, tid, unionPriId, searchTable, searchInfo,
                searchKeywordTableMappingRemoteGetDataFuture,
                searchKeywordTableMappingLocalGetDataFuture,
                searchKeywordTableMappingPdIdParam,
                asyncMgProductBasicCli,
                asyncMgProductStoreCli,
                asyncMgProductSpecCli);
        });

        // 除searchKeyWord之外的查询条件(包含排序)所在的表 映射“从本地缓存获取数据异步任务”
        Map<String, CompletableFuture> tableMappingRemoteGetDataFuture = new HashMap<>(16);
        // 除searchKeyWord之外的查询条件(包含排序)所在的表 映射“从远程获取数据的异步任务”
        Map<String, CompletableFuture> tableMappingLocalGetDataFuture = new HashMap<>(16);
        // 异步获取除了searchKeyWord之外的查询条件所在的表的数据
        table_searchInfo.forEach((searchTable, searchInfo) -> {
            asyncGetDataFromLocalOrRemote(flow, aid, tid, unionPriId, searchTable, searchInfo,
                tableMappingRemoteGetDataFuture,
                tableMappingLocalGetDataFuture,
                tableMappingPdIdParam,
                asyncMgProductBasicCli,
                asyncMgProductStoreCli,
                asyncMgProductSpecCli);
        });

        // 排序字段所在的表 映射“从本地缓存获取数据异步任务”
        Map<String, CompletableFuture> comparatorTableMappingRemoteGetDataFuture = new HashMap<>(16);
        // 排序字段所在的表 映射“从远程获取数据的异步任务”
        Map<String, CompletableFuture> comparatorTableMappingLocalGetDataFuture = new HashMap<>(16);
        // 异步获取排序字段所在的表的数据
        comparatorTable_searchInfo.forEach((searchTable, searchInfo) -> {
            asyncGetDataFromLocalOrRemote(flow, aid, tid, unionPriId, searchTable, searchInfo,
                comparatorTableMappingRemoteGetDataFuture,
                comparatorTableMappingLocalGetDataFuture,
                comparatorTableMappingPdIdParam,
                asyncMgProductBasicCli,
                asyncMgProductStoreCli,
                asyncMgProductSpecCli);
        });

        int callbackTaskTotal = searchKeywordTableMappingRemoteGetDataFuture.size() + searchKeywordTableMappingLocalGetDataFuture.size()
            + tableMappingRemoteGetDataFuture.size() + tableMappingLocalGetDataFuture.size()
            + comparatorTableMappingRemoteGetDataFuture.size() + comparatorTableMappingLocalGetDataFuture.size();
        CountDownLatch countDownLatch = new CountDownLatch(callbackTaskTotal);
        // 回调获取结果
        // 关键词搜索回调获取结果
        callbackGetResultList(flow, aid, unionPriId, searchKeywordTableMappingLocalGetDataFuture, searchKeywordTableMappingRemoteGetDataFuture,
            searchKeyWordTable_searchInfo, searchKeywordTableMappingPdIdParam, countDownLatch);

        // 其他搜索条件回调获取结果
        callbackGetResultList(flow, aid, unionPriId, tableMappingLocalGetDataFuture, tableMappingRemoteGetDataFuture,
            table_searchInfo, tableMappingPdIdParam, countDownLatch);

        // 搜索排序表回调获取的结果
        callbackGetResultList(flow, aid, unionPriId, comparatorTableMappingLocalGetDataFuture, comparatorTableMappingRemoteGetDataFuture,
            comparatorTable_searchInfo, comparatorTableMappingPdIdParam, countDownLatch);

        //  阻塞获取搜索结果完成
        try {
            countDownLatch.await();
            Log.logStd("finish  each table search data;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        } catch (InterruptedException e) {
            throw new MgException(Errno.ERROR, "waiting for each table search data time out;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }
    }

    /**
     * 异步获取搜索结果
     * @param searchInfo 保存搜索和排序等信息
     * @param tableNameMappingRemoteGetDataFuture 表名映射“从远程获取数据的异步任务”
     * @param tableNameMappingLocalGetDataFuture 表名映射“从本地缓存获取数据异步任务”
     * @param tableNameMappingPdIdParamList 表名 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
     */
    public void asyncGetDataFromLocalOrRemote(int flow, int aid, int tid, int unionPriId,
                                              String tableName, Param searchInfo,
                                              Map<String, CompletableFuture> tableNameMappingRemoteGetDataFuture,
                                              Map<String, CompletableFuture> tableNameMappingLocalGetDataFuture,
                                              Map<String, Map<Integer, Param>> tableNameMappingPdIdParamList,
                                              MgProductBasicCli asyncMgProductBasicCli,
                                              MgProductStoreCli asyncMgProductStoreCli,
                                              MgProductSpecCli asyncMgProductSpecCli) {

        // 获取查询条件
        ParamMatcher searchMatcher = (ParamMatcher) searchInfo.getObject(DbSearchSorterInfo.SEARCH_MATCHER);
        int dataCount = searchInfo.getInt(DbSearchSorterInfo.DATA_COUNT);
        // 全量缓存的阈值
        int dataLoadFromDbThreshold = getLoadFromDbThreshold(tableName);
        // 添加数据到本地缓存
        boolean addDataToLocalCache = dataCount <= dataLoadFromDbThreshold;
        searchInfo.setBoolean(DbSearchSorterInfo.ADD_DATA_TO_LOCAL_CACHE, addDataToLocalCache);
        // 是否需要从远端获取数据
        boolean needGetDataFromRemote = searchInfo.getBoolean(DbSearchSorterInfo.NEED_GET_DATA_FROM_REMOTE);
        // 直接从缓存拿
        if (!needGetDataFromRemote) {
            // 从一级缓存获取数据
            ParamListCache1 localMgProductSearchData = MgProductSearchCache.getLocalMgProductSearchDataCache(unionPriId);
            String cacheKey = MgProductSearchCache.getLocalMgProductSearchDataCacheKey(aid, tableName);
            if (localMgProductSearchData.containsKey(cacheKey)) {
                Log.logStd("get result from localCacheDataList;flow=%d,aid=%d,unionPriId=%d,ParamMatcher=%s;", flow, aid, unionPriId, searchMatcher);
                FaiList<Param> localCacheDataList = localMgProductSearchData.get(cacheKey);
                // 保存缓存的结果（未经过搜索条件过滤）到searchInfo
                searchInfo.setList(DbSearchSorterInfo.CACHE_DATA_LIST, localCacheDataList);
                // 从缓存拿的也异步执行
                CompletableFuture<FaiList<Param>> localGetDataFuture = CompletableFuture.supplyAsync(() -> getSearchResult(tableName, searchMatcher, localCacheDataList, tableNameMappingPdIdParamList));
                // 添加到表名映射“从本地缓存获取数据异步任务”，用于回调
                tableNameMappingLocalGetDataFuture.put(tableName, localGetDataFuture);
                return;
            } else {
                // 可能缓存被回收，重新获取数据
                Log.logStd("cache1==null; flow=%s;aid=%d;unionPriId=%d;searchSorterInfo=%s;", flow, aid, unionPriId, searchInfo);
            }
        }

        SearchArg searchArg = new SearchArg();
        searchArg.matcher = searchMatcher;
        // 从远端异步获取数据
        asyncGetDataFromRemote(flow, aid, tid, unionPriId, tableName, searchArg, addDataToLocalCache, tableNameMappingRemoteGetDataFuture, asyncMgProductBasicCli, asyncMgProductStoreCli, asyncMgProductSpecCli);
    }



    /**
     * 回调获取搜索结果
     * @param tableNameMappingLocalGetDataFuture 表名映射“从本地缓存获取数据异步任务”
     * @param tableNameMappingRemoteGetDataFuture 表名映射“从远程获取数据的异步任务”
     * @param tableMappingSearchInfo  表名映射“包含搜索和排序等信息的Param”
     * @param tableNameMappingPdIdParam  表名 -> pdId -> Param 主要用于排序，目前排序的场景都是 pdId 与排序字段是一对一
     */
    public void callbackGetResultList(int flow, int aid, int unionPriId,
                                      Map<String, CompletableFuture> tableNameMappingLocalGetDataFuture,
                                      Map<String, CompletableFuture> tableNameMappingRemoteGetDataFuture,
                                      final Map<String, Param> tableMappingSearchInfo,
                                      Map<String, Map<Integer, Param>> tableNameMappingPdIdParam,
                                      CountDownLatch countDownLatch) {
        // “从本地缓存获取数据的异步任务”回调获取结果
        tableNameMappingLocalGetDataFuture.forEach((tableName, localGetDataFuture) -> {
            localGetDataFuture.whenComplete((BiConsumer<FaiList<Param>, Throwable>) (searchResultList, throwable) -> {
                Param searchInfo = tableMappingSearchInfo.get(tableName);
                // 搜索结果
                searchInfo.setList(SEARCH_RESULT_LIST, searchResultList);
                // 搜索结果的条数
                searchInfo.setInt(SEARCH_RESULT_SIZE, searchResultList.size());
                Log.logStd("finish from localCache Data get searchResult;flow=%d,aid=%d;unionPriId=%d;tableName=%s;searchResultList=%s", flow, aid, unionPriId, tableName, searchResultList);
                countDownLatch.countDown();
            });
        });

        // “从远程获取数据的异步任务”回调获取结果
        tableNameMappingRemoteGetDataFuture.forEach((tableName, remoteGetDataFuture) -> {
            remoteGetDataFuture.whenComplete((BiConsumer<RemoteStandResult, Throwable>)(remoteStandResult, ex) -> {
                if (remoteStandResult.isSuccess()) {
                    // TODO 注意Errno.NOT_FOUND的情况是否会返回null，目前涉及到的服务的接口搜索不到，不会返回null
                    FaiList<Param> searchResultList = remoteStandResult.getObject(ParseData.TABLE_NAME_MAPPING_PARSE_DATA_KEY.get(tableName), FaiList.class);
                    Param searchInfo = tableMappingSearchInfo.get(tableName);
                    Boolean addDataToLocalCache = searchInfo.getBoolean(ADD_DATA_TO_LOCAL_CACHE);
                    if (addDataToLocalCache) {
                        // 说明是aid + unionPriId 全量搜索

                        // 设置本地缓存和数据状态缓存
                        searchInfo.setList(CACHE_DATA_LIST, searchResultList);
                        // 设置各个表的全量数据到本地缓存
                        ParamListCache1 localMgProductSearchData = MgProductSearchCache.getLocalMgProductSearchDataCache(unionPriId);
                        // 线程安全类
                        localMgProductSearchData.put(MgProductSearchCache.getLocalMgProductSearchDataCacheKey(aid, tableName), searchResultList);

                        // 设置各个表的本地缓存时间
                        Param dataStatusInfo = new Param();
                        dataStatusInfo.assign(searchInfo, DataStatus.Info.MANAGE_LAST_UPDATE_TIME);
                        dataStatusInfo.assign(searchInfo, DataStatus.Info.VISITOR_LAST_UPDATE_TIME);
                        dataStatusInfo.assign(searchInfo, DataStatus.Info.TOTAL_SIZE);
                        // 添加数据状态缓存
                        String cacheKey = MgProductSearchCache.LocalDataStatusCache.getDataStatusCacheKey(aid, unionPriId, tableName);
                        // TODO:待考虑是否为线程安全，目前来看是线程安全，不用加锁
                        MgProductSearchCache.LocalDataStatusCache.addLocalDataStatusCache(cacheKey, dataStatusInfo);

                        // 此时的searchResultList还没有使用搜索条件过滤，使用搜索条件过滤
                        ParamMatcher searchMatcher = (ParamMatcher) searchInfo.getObject(SEARCH_MATCHER);
                        // 使用搜索条件过滤后的最终结果
                        searchResultList = getSearchResult(tableName, searchMatcher, searchResultList, tableNameMappingPdIdParam);
                    }  else {
                        // 说明不是全量搜索
                        Log.logStd(tableName + " table data(aid+unionPriId) total size is more than LoadFromDbThreshold;Don't need addData to localCache;flow=%d;aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                        // 这里再调用prepareData是为了将数据放到tableNameMappingPdIdParam中，方便取交集和排序
                        prepareData(tableName, null, searchResultList, null, tableNameMappingPdIdParam);
                    }

                    // 保存搜索结果到searchInfo
                    searchInfo.setList(SEARCH_RESULT_LIST, searchResultList);
                    searchInfo.setInt(SEARCH_RESULT_SIZE, searchResultList.size());
                    Log.logStd("finish remote get " + tableName + " table data;flow=%d,aid=%d,unionPriId=%d;searchResultList=%s", flow, aid, unionPriId, searchResultList);
                    countDownLatch.countDown();
                } else {
                    int rt = remoteStandResult.getRt();
                    throw new MgException(rt, "remote get " + tableName + " table data error;flow=%d,aid=%d,unionPriId=%d;", flow, aid, unionPriId);
                }
            });
        });
    }

    /**
     * 各表从db全量load数据的阈值
     */
    public static int getLoadFromDbThreshold(String tableName){
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Str.isEmpty(conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey))){
            return defaultThreshold;
        }

        return conf.getParam(MgProductSearchSvr.SvrConfigGlobalConf.loadFromDbThresholdKey)
            .getInt(tableName, defaultThreshold);
    }

    /**
     * 使用es中获取到PdId使用In Sql的阈值。
     */
    public int getInSqlThreshold() {
        Param conf = ConfPool.getConf(MgProductSearchSvr.SvrConfigGlobalConf.svrConfigGlobalConfKey);
        int defaultThreshold = 1000;
        if(Str.isEmpty(conf) || Objects.isNull(conf.getInt(MgProductSearchSvr.SvrConfigGlobalConf.useIdFromEsAsInSqlThresholdKey))){
            return defaultThreshold;
        }
        return conf.getInt(MgProductSearchSvr.SvrConfigGlobalConf.useIdFromEsAsInSqlThresholdKey, defaultThreshold);
    }

    public static final class DbSearchSorterInfo {
        public static final String SEARCH_TABLE = "searchTable";
        public static final String SEARCH_MATCHER = "searchMatcher";
        public static final String NEED_GET_DATA_FROM_REMOTE = "needGetDataFromRemote";
        public static final String DATA_COUNT = "dataCount";
        public static final String CACHE_DATA_LIST = "cacheDataList";

        public static final String SEARCH_RESULT_LIST = "searchResultList";
        public static final String SEARCH_RESULT_SIZE = "searchResultSize";
        public static final String ADD_DATA_TO_LOCAL_CACHE = "addDataToLocalCache";
    }

    public static final class EsSearchSorterInfo {
        public static final String ES_SEARCH_RESULT_TOTAL = "esSearchResultTotal";
        public static final String ES_SEARCH_RESULT = "esSearchResult";
        public static final String PDIDLIST_FROME_ES_SEARCH_RESULT = "pdIdListFromEsSearchResult";
    }
}
