package fai.MgProductInfSvr.interfaces.utils;

import fai.MgProductInfSvr.interfaces.entity.*;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;

import java.util.Calendar;

/**
 * @author Lu
 * @date 2021-08-17 15:17
 *
 * Db 专用的搜索类
 * 【注】set方法，业务方已经没有使用的必要了，先保留着
 */
public class MgProductDbSearch extends BaseMgProductSearch {

    private FaiList<Integer> rlGroupIdList;  // 业务商品分类 IdList
    private FaiList<Integer> rlTagIdList;   //  业务商品标签搜索
    private FaiList<Integer> rlPdIdList;    // 业务商品 IdList
    private FaiList<Integer> typeList;     // 商品类型：实物、卡密、酒店
    private Calendar addTimeBegin;
    private Calendar addTimeEnd;            // 如果搜索的 创建时间 开始 和 创建时间 结束是相同的，则走 EQ 的逻辑
    private FaiList<Integer> rlLibIdList;   //  在哪些库搜索，默认是全部库

    // search keyword 相关
    private boolean enableSearchSkuCode = false; // 关键词用作条形码搜索
    private boolean enableSearchProductProp = false;   // 是否允许搜索商品参数, 默认是 false
    private FaiList<Integer> keyWordSearchInPropIdList;    //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
    private boolean enableSearchProductRemark = false;   // 是否允许搜索商品详情, 默认是 false
    private FaiList<String> searchProductRemarkKeyList;  //  在哪些富文本详情下搜索, 与 searchKeyWord 匹配使用

    private FaiList<Integer> rlPropValIdList;   //  根据 参数值 搜索

    private Long priceBegin;   //  最小交易价格
    private Long priceEnd;     //  最大交易价格，如果最小交易价格和最大交易价格一样, 则是等于比较

    private Integer salesBegin;  //  最小的销量
    private Integer salesEnd;    //  最大的销量，如果最小销量和销量一样, 则是等于比较

    private Integer remainCountBegin;  //  最小的库存
    private Integer remainCountEnd;    //  最大的库存，如果最小库存和库存一样, 则是等于比较

    private Integer modeType; // 服务模型
    private Integer sysType; // 商品中台系统类型（商品/服务）

    private String firstComparatorTable;  // 第一排序字段的table
    private String secondComparatorTable = SearchTableNameEnum.MG_PRODUCT_REL.searchTableName;   // 第二排序字段的table,默认是MG_PRODUCT_REL

    /**
     * 自定义的排序字段，优先级：自定义排序字段 > 第一排序字段 > 第二排序字段
     * 暂时只支持int类型的字段做自定义的排序
     */
    private String customComparatorKey;

    /**
     * 自定义排序字段所在的表
     */
    private String customComparatorTable;

    /**
     * 自定义的排序字段的顺序，且FaiList保存的内容为同一类型
     *
     * 例如customComparatorKey为id
     * 增加key对应的顺序，例如id的order是1,3,2,4，比较时将采用此order进行比较排序
     */
    private FaiList<?> customComparatorList;

    @Override
    public Param getSearchParam() {
        // 先获取公共查询条件的Param
        Param param = getBaseSearchParam();
        param.setList(DbSearchInfo.RL_GROUP_ID_LIST, rlGroupIdList);   // 业务商品分类
        param.setList(DbSearchInfo.RL_TAG_ID_LIST, rlTagIdList);   // 业务商品标签
        param.setList(DbSearchInfo.RL_PD_ID_LIST, rlPdIdList);          // rlPdIdList
        param.setList(DbSearchInfo.TYPE_LIST, typeList);                // 商品类型：实物、卡密、酒店， 对应 ProductEntity.DbSearchInfo.PD_TYPE 字段
        param.setList(DbSearchInfo.RL_LIB_ID_LIST, rlLibIdList);           //  在哪些库搜索，默认是全部库

        // flag相关
        param.setObject(DbSearchInfo.RLFLAG_PARAM_MATCHER, rlFlagMatcher);
        param.setObject(DbSearchInfo.FLAG_PARAM_MATCHER, flagMatcher);

        // 关键词searchKeyword相关
        param.setBoolean(DbSearchInfo.ENABLE_SEARCH_SKU_CODE, enableSearchSkuCode); // 关键词用作条形码搜索
        param.setBoolean(DbSearchInfo.ENABLE_SEARCH_PRODUCT_PROP, enableSearchProductProp);   // 是否允许搜索商品参数, 默认是 false
        param.setList(DbSearchInfo.KEY_WORD_SEARCH_IN_PROP_ID_LIST, keyWordSearchInPropIdList);     //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
        param.setBoolean(DbSearchInfo.ENABLE_SEARCH_PRODUCT_REMARK, enableSearchProductRemark);  // 是否允许搜索商品详情, 默认是 false
        param.setList(DbSearchInfo.SEARCH_PRODUCT_REMARK_KEY_LIST, searchProductRemarkKeyList); // 关键词用作商品详情搜索

        param.setList(DbSearchInfo.RL_PROP_VAL_ID_LIST, rlPropValIdList);  //  根据 参数值 搜索
        param.setCalendar(DbSearchInfo.ADD_TIME_BEGIN, addTimeBegin);  // 搜索商品  开始录入时间
        param.setCalendar(DbSearchInfo.ADD_TIME_END, addTimeEnd);      // 搜索商品  结束录入时间
        param.setLong(DbSearchInfo.PRICE_BEGIN, priceBegin);     // 搜索商品 开始价格
        param.setLong(DbSearchInfo.PRICE_END, priceEnd);         // 搜索商品  结束价格
        param.setInt(DbSearchInfo.SALES_BEGIN, salesBegin);      // 搜索商品 开始 销量
        param.setInt(DbSearchInfo.SALES_END, salesEnd);          // 搜索商品 结束 销量
        param.setInt(DbSearchInfo.REMAIN_COUNT_BEGIN, remainCountBegin);      // 搜索商品 开始 库存
        param.setInt(DbSearchInfo.REMAIN_COUNT_END, remainCountEnd);          // 搜索商品 结束 库存
        param.setInt(DbSearchInfo.MODE_TYPE, modeType); // 服务模型
        param.setInt(DbSearchInfo.SYS_TYPE, sysType); // 商品中台系统类型（商品/服务）

        // 排序相关
        param.setString(DbSearchInfo.CUSTOM_COMPARATOR_KEY, customComparatorKey); //自定义排序的key
        param.setList(DbSearchInfo.CUSTOM_COMPARATOR_LIST, customComparatorList); // 自定义排序的key对应的List
        param.setString(DbSearchInfo.CUSTOM_COMPARATOR_TABLE, customComparatorTable); // 自定义排序字段所在的表
        param.setString(DbSearchInfo.FIRST_COMPARATOR_TABLE, firstComparatorTable); // 第一排序table
        param.setString(DbSearchInfo.SECOND_COMPARATOR_TABLE, secondComparatorTable); // 第二排序table

        return param;
    }

    @Override
    public void initSearchParam(Param dbSearchParam) {
        // 初始化公共的查找字段
        initBaseSearchParam(dbSearchParam);

        this.rlGroupIdList = dbSearchParam.getList(DbSearchInfo.RL_GROUP_ID_LIST); // 业务商品分类
        this.rlTagIdList = dbSearchParam.getList(DbSearchInfo.RL_TAG_ID_LIST); // 业务商品标签
        this.rlPdIdList = dbSearchParam.getList(DbSearchInfo.RL_PD_ID_LIST);        // 业务商品idList
        this.typeList = dbSearchParam.getList(DbSearchInfo.TYPE_LIST);               // 商品类型：实物、卡密、酒店

        this.rlLibIdList = dbSearchParam.getList(DbSearchInfo.RL_LIB_ID_LIST);                       //  在哪些库搜索，默认是全部库

        // 解析flag和rlFlag
        parseParamMatcher(dbSearchParam);

        // 关键词相关
        this.enableSearchSkuCode = dbSearchParam.getBoolean(DbSearchInfo.ENABLE_SEARCH_SKU_CODE, false); // 关键词用作条形码搜索
        this.enableSearchProductProp = dbSearchParam.getBoolean(DbSearchInfo.ENABLE_SEARCH_PRODUCT_PROP, false);  // 是否允许搜索商品参数, 默认是 false
        this.keyWordSearchInPropIdList = dbSearchParam.getList(DbSearchInfo.KEY_WORD_SEARCH_IN_PROP_ID_LIST);  //  在哪些参数下筛选
        this.enableSearchProductRemark = dbSearchParam.getBoolean(DbSearchInfo.ENABLE_SEARCH_PRODUCT_REMARK, false); // 是否允许搜索商品详情, 默认是 false
        this.searchProductRemarkKeyList = dbSearchParam.getList(DbSearchInfo.SEARCH_PRODUCT_REMARK_KEY_LIST);

        this.rlPropValIdList = dbSearchParam.getList(DbSearchInfo.RL_PROP_VAL_ID_LIST);   // 根据 参数值 搜索

        this.addTimeBegin = dbSearchParam.getCalendar(DbSearchInfo.ADD_TIME_BEGIN);   // 搜索商品开始 录入时间
        this.addTimeEnd = dbSearchParam.getCalendar(DbSearchInfo.ADD_TIME_END);       // 搜索商品结束 录入时间
        this.priceBegin = dbSearchParam.getLong(DbSearchInfo.PRICE_BEGIN);          // 搜索商品开始 价格
        this.priceEnd = dbSearchParam.getLong(DbSearchInfo.PRICE_END);              // 搜索商品结束 价格
        this.salesBegin = dbSearchParam.getInt(DbSearchInfo.SALES_BEGIN);           // 搜索商品 开始 销量
        this.salesEnd = dbSearchParam.getInt(DbSearchInfo.SALES_END);               // 搜索商品 结束 销量
        this.remainCountBegin = dbSearchParam.getInt(DbSearchInfo.REMAIN_COUNT_BEGIN);  // 搜索商品 开始 库存
        this.remainCountEnd = dbSearchParam.getInt(DbSearchInfo.REMAIN_COUNT_END);      // 搜索商品 结束 库存
        this.modeType = dbSearchParam.getInt(DbSearchInfo.MODE_TYPE); // 服务模型
        this.sysType = dbSearchParam.getInt(DbSearchInfo.SYS_TYPE); // 商品中台系统类型（商品/服务）

        // 排序相关
        this.customComparatorKey = dbSearchParam.getString(DbSearchInfo.CUSTOM_COMPARATOR_KEY); // 自定义的排序
        this.customComparatorList = dbSearchParam.getList(DbSearchInfo.CUSTOM_COMPARATOR_LIST); // 自定义排序的List
        this.customComparatorTable = dbSearchParam.getString(DbSearchInfo.CUSTOM_COMPARATOR_TABLE); // 自定义排序字段所在的表
        this.firstComparatorTable = dbSearchParam.getString(DbSearchInfo.FIRST_COMPARATOR_TABLE); // 第一排序字段table
        // db查询条件里的第二排序字段默认是rlPdId
        this.secondComparatorKey = dbSearchParam.getString(BaseSearchInfo.SECOND_COMPARATOR_KEY, ProductBasicEntity.ProductInfo.RL_PD_ID);
        this.secondComparatorTable = dbSearchParam.getString(DbSearchInfo.SECOND_COMPARATOR_TABLE, SearchTableNameEnum.MG_PRODUCT_REL.searchTableName); // 第二排序字段table,默认是商品业务关系表
    }


    public static final class DbSearchInfo {
        public static final String RL_GROUP_ID_LIST = "rlGroupIdList"; //商品的分类idList
        public static final String RL_TAG_ID_LIST = "rlTagIdList"; //商品标签idList
        public static final String RL_PD_ID_LIST = "rlPdIdList"; //商品业务idList
        public static final String TYPE_LIST = "typeList"; //商品类型：实物、卡密、酒店

        public static final String RL_LIB_ID_LIST = "rlLibIdList"; //在哪些库搜索，默认是全部库

        public static final String ENABLE_SEARCH_SKU_CODE = "enableSearchSkuCode";
        public static final String ENABLE_SEARCH_PRODUCT_PROP = "enableSearchProductProp"; // 是否允许搜索商品参数, 默认是 false
        public static final String KEY_WORD_SEARCH_IN_PROP_ID_LIST = "keyWordSearchInPropIdList"; // 在哪些参数下搜索, 与 searchKeyWord 匹配使用
        public static final String ENABLE_SEARCH_PRODUCT_REMARK = "enableSearchProductRemark"; // 是否允许搜索商品详情, 默认是 false
        public static final String SEARCH_PRODUCT_REMARK_KEY_LIST = "searchProductRemarkKeyList"; // 搜索商品详情 keyList, 可能区分 mobi key、site key，合适由各个项目传入进来

        public static final String RL_PROP_VAL_ID_LIST = "rlPropValIdList"; // 根据 参数值 搜索进行 like 搜索

        public static final String ADD_TIME_BEGIN = "addTimeBegin"; // 搜索商品  开始录入时间, 需要传入 "xxxx-xx-xx xx:xx:xx" 的格式
        public static final String ADD_TIME_END = "addTimeEnd"; // 搜索商品 结束录入时间，需要传入 "xxxx-xx-xx xx:xx:xx" 的格式
        public static final String PRICE_BEGIN = "priceBegin"; // 搜索商品  开始价格
        public static final String PRICE_END = "priceEnd"; // 搜索商品 结束价格
        public static final String SALES_BEGIN = "salesBegin";// 搜索商品 开始 销量
        public static final String SALES_END = "salesEnd"; // 搜索商品 结束 销量
        public static final String REMAIN_COUNT_BEGIN = "remainCountBegin";// 搜索商品 开始 库存
        public static final String REMAIN_COUNT_END = "remainCountEnd"; // 搜索商品 结束 库存
        public static final String MODE_TYPE = "modeType"; // 服务模型
        public static final String SYS_TYPE = "sysType"; // 商品中台系统类型（商品/服务）

        public static final String CUSTOM_COMPARATOR_KEY = "customComparatorKey";// 自定义的排序，如果设置了该排序，其他的排序无效（包括es里的排序）
        public static final String CUSTOM_COMPARATOR_TABLE = "customComparatorTable"; // 自定义排序字段所在的表
        public static final String CUSTOM_COMPARATOR_LIST = "customComparatorList"; // 自定义排序的List

        public static final String FIRST_COMPARATOR_TABLE = "firstComparatorTable"; // 第一排序字段的table
        public static final String SECOND_COMPARATOR_TABLE = "secondComparatorTable"; // 第二排序字段的table

        public static final String RLFLAG_PARAM_MATCHER = "rlFlagParamMatcher";
        public static final String FLAG_PARAM_MATCHER = "flagParamMatcher";
    }

    /**
     * 排序指定 table, table 待完善, 需要作为缓存的key，为了节省内存，所以使用缩写
     */
    public enum SearchTableNameEnum{
        // 对应 mgProduct， 商品基础表
        MG_PRODUCT("pd"),
        // 对应 mgProductRel， 商品业务关系表
        MG_PRODUCT_REL("pdr"),
        // 对应 mgProductBindProp，参数值绑定商品关系表
        MG_PRODUCT_BIND_PROP("pbp"),
        // 对应 mgProductBindGroup，分类绑定商品关系表
        MG_PRODUCT_BIND_GROUP("pbg"),
        // 对应 mgProductBindTag，标签绑定商品关系表
        MG_PRODUCT_BIND_TAG("pbt"),
        // 对应 mgSpuBizSummary，商品 spu 销售汇总表
        MG_SPU_BIZ_SUMMARY("sbs"),
        // 对应mgProductSpecSkuCode，商品规格skuCode表
        MG_PRODUCT_SPEC_SKU_CODE("pssc");

        private final String searchTableName;

        SearchTableNameEnum(String searchTableName) {
            this.searchTableName = searchTableName;
        }

        public String getSearchTableName() {
            return searchTableName;
        }
    }

    
    /**
     * 判断是否搜索访客态的数据，即是否只查管理态的数据
     * @param tableName 查询的表名
     * @return true 表示只查管理态的字段
     */
    public boolean getIsOnlySearchManageData(String tableName){
        if(MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(tableName)){
            // priceBegin 、 priceEnd 都是管理态修改的数据
            // salesBegin 、salesEnd、remainCountBegin、remainCountEnd 是访客可能变动的数据
            // 访客态字段: SpuBizSummaryEntity.VISITOR_FIELDS
            if(salesBegin != null || salesEnd != null){
                return false;
            }
            if(remainCountBegin != null || remainCountEnd != null){
                return false;
            }

            // 判断排序字段是否包含访客字段
            ParamComparator paramComparator = getParamComparator();
            String comparatorTable = getFirstComparatorTable();
            String firstComparatorKey = getFirstComparatorKey();
            return paramComparator.isEmpty() ||
                !MgProductDbSearch.SearchTableNameEnum.MG_SPU_BIZ_SUMMARY.searchTableName.equals(comparatorTable) ||
                !ProductStoreEntity.SpuBizSummaryInfo.VISITOR_FIELDS.contains(firstComparatorKey);
        }

        /*
         * MG_PRODUCT: searchKeyWord 都是管理态修改的数据
         * MG_PRODUCT_REL: rlPdIdList、 typeList 、 rlLibIdList 、
         *                 upSalesStatus、 addTimeBegin、 addTimeEnd 都是管理态修改的数据
         * MG_PRODUCT_BIND_PROP: rlPropValIdList 都是管理态修改的数据
         * MG_PRODUCT_BIND_GROUP: rlGroupIdList 都是管理态修改的数据
         * MG_PRODUCT_BIND_TAG: rlTagId 都是管理态修改的数据
         * MG_PRODUCT_SPEC_SKU_CODE: skuCode 都是管理态修改的数据
         */
        return MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT.searchTableName.equals(tableName) ||
            MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_REL.searchTableName.equals(tableName) ||
            MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_PROP.searchTableName.equals(tableName) ||
            MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_GROUP.searchTableName.equals(tableName) ||
            MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_BIND_TAG.searchTableName.equals(tableName) ||
            MgProductDbSearch.SearchTableNameEnum.MG_PRODUCT_SPEC_SKU_CODE.searchTableName.equals (tableName);
    }

    // 根据 matcher 判断是否有搜索条件
    public boolean isEmpty(){
        // searchKeyword的搜索条件
        boolean searchKeywordSearchMatcherIsEmpty = getProductRemarkSkwSearchMatcher(null).isEmpty() &&
            getProductPropValSkwSearchMatcher(null).isEmpty() &&
            getProductBasicSkwSearchMatcher(null).isEmpty() &&
            getProductSpecSkuCodeSkwSearchMatcher(null).isEmpty();

        // 除searchKeyword外的其他搜索条件
        boolean otherSearchMatcherIsEmpty = getProductBasicSearchMatcher(null).isEmpty() &&
            getProductRelSearchMatcher(null).isEmpty() &&
            getProductBindPropSearchMatcher(null).isEmpty() &&
            getProductBindGroupSearchMatcher(null).isEmpty() &&
            getProductBindTagSearchMatcher(null).isEmpty() &&
            getProductSpuBizSummarySearchMatcher(null).isEmpty();

        return  searchKeywordSearchMatcherIsEmpty && otherSearchMatcherIsEmpty;
    }


    // TODO: 在 "在商品 富文本 字段"  搜索,目前还没有支持
    public ParamMatcher getProductRemarkSkwSearchMatcher(ParamMatcher paramMatcher){
        ParamMatcher paramMatcherOr = new ParamMatcher();
        // 兼容门店，不用加上 && !searchProductRemarkKeyList.isEmpty()
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductName && searchProductRemarkKeyList != null){
            //  商品名称, 名称 like 查询
            for(String remarkKey : searchProductRemarkKeyList){
                paramMatcherOr.or(remarkKey, ParamMatcher.LK, searchKeyWord);
            }
        }
        if(paramMatcher == null){
            return paramMatcherOr;
        }else{
            paramMatcher.and(paramMatcherOr);
            return paramMatcher;
        }
    }

    // TODO: 在 "参数值表" mgProductPropVal_xxxx 搜索,目前还没有支持
    public ParamMatcher getProductPropValSkwSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        //兼容门店，不要加上 !keyWordSearchInPropIdList.isEmpty()
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductProp && keyWordSearchInPropIdList != null){
            if(keyWordSearchInPropIdList.size() == 1){
                paramMatcher.and(ProductPropEntity.PropValInfo.PROP_ID, ParamMatcher.EQ, keyWordSearchInPropIdList.get(0));
            }else{
                paramMatcher.and(ProductPropEntity.PropValInfo.PROP_ID, ParamMatcher.IN, keyWordSearchInPropIdList);
            }
            // 对参数值进行 like 搜索
            paramMatcher.and(ProductPropEntity.PropValInfo.VAL, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }

    /**
     * searchKeyWord 的搜索条件
     * skw:searchKeyWord
     */
    public ParamMatcher getProductBasicSkwSearchMatcher(ParamMatcher paramMatcher) {
        if (paramMatcher == null) {
            paramMatcher = new ParamMatcher();
        }
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductName){
            //  商品名称, 名称 like 查询
            paramMatcher.and(ProductBasicEntity.ProductInfo.NAME, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }

    /**
     * 关键词用作条形码在"商品规格sku表" mgProductSpecSkuCode_0xxx搜索，
     * skw:searchKeyWord
     */
    public ParamMatcher getProductSpecSkuCodeSkwSearchMatcher(ParamMatcher paramMatcher) {
        if (paramMatcher == null) {
            paramMatcher = new ParamMatcher();
        }
        if (!Str.isEmpty(searchKeyWord) && enableSearchSkuCode) {
            // 关键词用作条形码进行like搜索
            paramMatcher.and(ProductSpecEntity.SpecSkuInfo.SKU_CODE, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }

    /**
     * 在 "商品基础表" mgProduct_xxxx 搜索，除SearchKeyWord之外的搜索条件
     */
    public ParamMatcher getProductBasicSearchMatcher(ParamMatcher paramMatcher){
        if (paramMatcher == null) {
            if (flagMatcher != null) {
                // flag支持多个位进行操作
                paramMatcher = flagMatcher;
            } else {
                paramMatcher = new ParamMatcher();
            }
        }
        return paramMatcher;
    }


    // 在 "商品业务关系表" mgProductRel_xxxx 搜索
    public ParamMatcher getProductRelSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            if (rlFlagMatcher != null) {
                // rlFlag支持多个位进行操作
                paramMatcher = rlFlagMatcher;
            } else {
                paramMatcher = new ParamMatcher();
            }
        }

        if (sysType != null) {
            paramMatcher.and(ProductBasicEntity.ProductInfo.SYS_TYPE, ParamMatcher.EQ, sysType);
        }

        // 从 mgProduct_xxxx 冗余字段查询，商品类型。兼容门店逻辑，不要加上!typeList.isEmpty()
        if(typeList != null){
            if(typeList.size() == 1){
                paramMatcher.and(ProductBasicEntity.ProductInfo.PD_TYPE, ParamMatcher.EQ, typeList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.ProductInfo.PD_TYPE, ParamMatcher.IN, typeList);
            }
        }

        // 业务商品idList.兼容门店，不要加上!rlPdIdList.isEmpty()
        if(rlPdIdList != null){
            if(rlPdIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_PD_ID, ParamMatcher.EQ, rlPdIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_PD_ID, ParamMatcher.IN, rlPdIdList);
            }
        }

        //  商品库  兼容门店，不要加上!rlLibIdList.isEmpty()
        if(rlLibIdList != null){
            if(rlLibIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_LIB_ID, ParamMatcher.EQ, rlLibIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.ProductInfo.RL_LIB_ID, ParamMatcher.IN, rlLibIdList);
            }
        }

        // 商品的状态（商品业务表）
        if (upSalesStatus != null) {
            if (upSalesStatus == UpSalesStatusEnum.UP_AND_DOWN_SALES.getUpSalesStatus()) {
                // 上架或者下架的，或者两种都有
                FaiList<Integer> statusList = new FaiList<Integer>();
                statusList.add(ProductBasicValObj.ProductValObj.Status.UP);
                statusList.add(ProductBasicValObj.ProductValObj.Status.DOWN);
                paramMatcher.and(ProductBasicEntity.ProductInfo.STATUS, ParamMatcher.IN, statusList);
            } else if (upSalesStatus != UpSalesStatusEnum.ALL.getUpSalesStatus()) {
                // 非全部的，单独是某种状态
                paramMatcher.and(ProductBasicEntity.ProductInfo.STATUS, ParamMatcher.EQ, upSalesStatus);
            }
        }


        //  商品录入时间
        if(addTimeBegin != null || addTimeEnd != null){
            if(addTimeBegin != null && addTimeEnd != null && addTimeBegin.getTimeInMillis() == addTimeEnd.getTimeInMillis()){
                paramMatcher.and(ProductBasicEntity.ProductInfo.ADD_TIME, ParamMatcher.EQ, addTimeBegin);
            }else{
                //  创建时间大于
                if(addTimeBegin != null){
                    paramMatcher.and(ProductBasicEntity.ProductInfo.ADD_TIME, ParamMatcher.GE, addTimeBegin);
                }
                //  创建时间小于
                if(addTimeEnd != null){
                    paramMatcher.and(ProductBasicEntity.ProductInfo.ADD_TIME, ParamMatcher.LE, addTimeEnd);
                }
            }
        }
        return paramMatcher;
    }

    // 在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
    public ParamMatcher getProductBindPropSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 兼容门店，不要加上!rlPropValIdList.isEmpty()
        if(rlPropValIdList != null){
            if(rlPropValIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, ParamMatcher.EQ, rlPropValIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, ParamMatcher.IN, rlPropValIdList);
            }
        }
        return paramMatcher;
    }

    // 在 "商品与分类关联表" mgProductBindGroup_xxxx 搜索
    public ParamMatcher getProductBindGroupSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 业务商品分类。兼容门店，不要加上!rlGroupIdList.isEmpty()
        if(rlGroupIdList != null){
            if(rlGroupIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.BindGroupInfo.RL_GROUP_ID, ParamMatcher.EQ, rlGroupIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.BindGroupInfo.RL_GROUP_ID, ParamMatcher.IN, rlGroupIdList);
            }
        }
        return paramMatcher;
    }

    // 在 "商品与标签关联表" mgProductBindTag_xxxx 搜索
    public ParamMatcher getProductBindTagSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 业务商品标签。兼容门店，不要加上!rlTagIdList.isEmpty()
        if(rlTagIdList != null){
            if(rlTagIdList.size() == 1){
                paramMatcher.and(ProductBasicEntity.BindTagInfo.RL_TAG_ID, ParamMatcher.EQ, rlTagIdList.get(0));
            }else{
                paramMatcher.and(ProductBasicEntity.BindTagInfo.RL_TAG_ID, ParamMatcher.IN, rlTagIdList);
            }
        }
        return paramMatcher;
    }

    // 在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
    public ParamMatcher getProductSpuBizSummarySearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }

        if (modeType != null) {
            paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MODE_TYPE, ParamMatcher.EQ, modeType);
        }

        // 商品销量
        if(salesBegin != null || salesEnd != null){
            if(salesBegin != null && salesEnd != null && salesBegin.intValue() == salesEnd.intValue()){
                paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.SALES, ParamMatcher.EQ, salesBegin);
            }else{
                if(salesBegin != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.SALES, ParamMatcher.GE, salesBegin);
                }
                if(salesEnd != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.SALES, ParamMatcher.LE, salesEnd);
                }
            }
        }

        // 商品库存
        if(remainCountBegin != null || remainCountEnd != null){
            if(remainCountBegin != null && remainCountEnd != null && remainCountBegin.intValue() == remainCountEnd.intValue()){
                paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.REMAIN_COUNT, ParamMatcher.EQ, remainCountBegin);
            }else{
                if(remainCountBegin != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.REMAIN_COUNT, ParamMatcher.GE, remainCountBegin);
                }
                if(remainCountEnd != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.REMAIN_COUNT, ParamMatcher.LE, remainCountEnd);
                }
            }
        }

        // 商品价格
        if(priceBegin != null || priceEnd != null){
            if(priceBegin != null && priceEnd != null){
                if(priceBegin.longValue() == priceEnd.longValue()){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.EQ, priceBegin);
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.EQ, priceBegin);
                }else{
                    ParamMatcher priceMatcher = new ParamMatcher();
                    ParamMatcher minPriceMatcher = new ParamMatcher();
                    minPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.GE, priceBegin);
                    minPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.LE, priceEnd);
                    priceMatcher.or(minPriceMatcher);

                    ParamMatcher maxPriceMatcher = new ParamMatcher();
                    maxPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.GE, priceBegin);
                    maxPriceMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.LE, priceEnd);
                    priceMatcher.or(maxPriceMatcher);
                    paramMatcher.and(priceMatcher);
                }
            }else{
                if(priceBegin != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MIN_PRICE, ParamMatcher.GE, priceBegin);
                }
                if(priceEnd != null){
                    paramMatcher.and(ProductStoreEntity.SpuBizSummaryInfo.MAX_PRICE, ParamMatcher.LE, priceEnd);
                }
            }
        }
        return paramMatcher;
    }


    /**
     * 把排序转换为 ParamComparator
     */
    public ParamComparator getParamComparator(){
        ParamComparator paramComparator = new ParamComparator();
        // 按优先级设置 自定义的排序 > 第一排序 > 第二排序
        // 自定义的排序
        if (hasCustomComparator()) {
            paramComparator.addKey(customComparatorKey, customComparatorList);
        }
        // 第一排序
        if(hasFirstComparator()){
            paramComparator.addKey(this.firstComparatorKey, this.firstComparatorKeyOrderByDesc);
        }
        // 第二排序
        if(needSecondComparatorSorting){
            // secondComparatorKey 默认是rlPdId,升序
            paramComparator.addKey(secondComparatorKey, secondComparatorKeyOrderByDesc);
        }
        return paramComparator;
    }

    public FaiList<Integer> getRlTagIdList() {
        return rlTagIdList;
    }

    public MgProductDbSearch setRlTagIdList(FaiList<Integer> rlTagIdList) {
        this.rlTagIdList = rlTagIdList;
        return this;
    }

    public MgProductDbSearch setRlPdIdList(FaiList<Integer> rlPdIdList) {
        this.rlPdIdList = rlPdIdList;
        return this;
    }
    public FaiList<Integer> getRlPdIdList() {
        return rlPdIdList;
    }

    public FaiList<Integer> getTypeList() {
        return typeList;
    }

    public MgProductDbSearch setTypeList(FaiList<Integer> typeList) {
        this.typeList = typeList;
        return this;
    }
    public FaiList<Integer> getRlGroupIdList() {
        return rlGroupIdList;
    }

    public MgProductDbSearch setRlGroupIdList(FaiList<Integer> rlGroupIdList) {
        this.rlGroupIdList = rlGroupIdList;
        return this;
    }

    public FaiList<Integer> getRlLibIdList() {
        return rlLibIdList;
    }

    public MgProductDbSearch setRlLibIdList(FaiList<Integer> rlLibIdList) {
        this.rlLibIdList = rlLibIdList;
        return this;
    }

    public MgProductDbSearch setSearchKeyword(String searchKeyword) {
        this.searchKeyWord = searchKeyword;
        return this;
    }

    // 如果商品参数的搜索rlPropIdList没有设置的话，那就只是代表商品名称like搜索
    public MgProductDbSearch setSearchKeyWord(String searchKeyWord,
                                              boolean enableSearchProductName,
                                              boolean enableSearchProductProp,
                                              FaiList<Integer> keyWordSearchInPropIdList,
                                              boolean enableSearchProductRemark,
                                              FaiList<String> searchProductRemarkKeyList) {
        this.searchKeyWord = searchKeyWord;
        this.enableSearchProductName = enableSearchProductName;
        this.enableSearchProductProp = enableSearchProductProp;
        this.keyWordSearchInPropIdList = keyWordSearchInPropIdList;   // 在指定 propId 进行 like  搜索
        this.enableSearchProductRemark = enableSearchProductRemark;
        this.searchProductRemarkKeyList = searchProductRemarkKeyList;
        return this;
    }

    public boolean getEnableSearchProductName() {
        return enableSearchProductName;
    }


    public boolean getEnableSearchProductProp() {
        return enableSearchProductProp;
    }

    public MgProductDbSearch setEnableSearchProductProp(boolean enableSearchProductProp) {
        this.enableSearchProductProp = enableSearchProductProp;
        return this;
    }

    public boolean getEnableSearchProductRemark() {
        return enableSearchProductRemark;
    }

    public MgProductDbSearch setEnableSearchProductRemark(boolean enableSearchProductRemark) {
        this.enableSearchProductRemark = enableSearchProductRemark;
        return this;
    }

    public FaiList<String> getSearchProductRemarkKeyList() {
        return searchProductRemarkKeyList;
    }

    public MgProductDbSearch setSearchProductRemarkKeyList(FaiList<String> searchProductRemarkKeyList) {
        this.searchProductRemarkKeyList = searchProductRemarkKeyList;
        return this;
    }

    public MgProductDbSearch setKeyWordSearchInPropIdList(FaiList<Integer> keyWordSearchInPropIdList) {
        this.keyWordSearchInPropIdList = keyWordSearchInPropIdList;
        return this;
    }

    public FaiList<Integer> getKeyWordSearchInPropIdList() {
        return keyWordSearchInPropIdList;
    }

    public FaiList<Integer> getRlPropValIdList() {
        return rlPropValIdList;
    }

    public MgProductDbSearch setRlPropValIdList(FaiList<Integer> rlPropValIdList) {
        this.rlPropValIdList = rlPropValIdList;
        return this;
    }

    // 商品创建时间范围
    public MgProductDbSearch setAddTime(Calendar addTimeBegin, Calendar addTimeEnd) {
        this.addTimeBegin = addTimeBegin;
        this.addTimeEnd = addTimeEnd;
        return this;
    }

    public Calendar getAddTimeBegin() {
        return addTimeBegin;
    }

    public Calendar getAddTimeEnd() {
        return addTimeEnd;
    }

    // 价格范围
    public MgProductDbSearch setPrice(Long priceBegin, Long priceEnd) {
        this.priceBegin = priceBegin;
        this.priceEnd = priceEnd;
        return this;
    }

    public Long getPriceBegin() {
        return priceBegin;
    }

    public Long getPriceEnd() {
        return priceEnd;
    }

    // 销量范围
    public MgProductDbSearch setSales(Integer salesBegin, Integer salesEnd) {
        this.salesBegin = salesBegin;
        this.salesEnd = salesEnd;
        return this;
    }

    public Integer getSalesBegin() {
        return salesBegin;
    }

    public Integer getSalesEnd() {
        return salesEnd;
    }

    // 库存范围
    public MgProductDbSearch setRemainCount(Integer remainCountBegin, Integer remainCountEnd) {
        this.remainCountBegin = remainCountBegin;
        this.remainCountEnd = remainCountEnd;
        return this;
    }

    public Integer getRemainCountBegin() {
        return remainCountBegin;
    }

    public Integer getRemainCountEnd() {
        return remainCountEnd;
    }

    public Integer getModeType() {
        return modeType;
    }

    public MgProductDbSearch setModeType(Integer modeType) {
        this.modeType = modeType;
        return this;
    }

    public Integer getSysType() {
        return sysType;
    }

    public MgProductDbSearch setSysType(Integer sysType) {
        this.sysType = sysType;
        return this;
    }

    public boolean getFirstComparatorKeyOrderByDesc(){
        return this.firstComparatorKeyOrderByDesc;
    }

    public String getSecondComparatorTable() {
        return secondComparatorTable;
    }

    public boolean getSecondComparatorKeyOrderByDesc(){
        return this.secondComparatorKeyOrderByDesc;
    }

    public String getCustomComparatorKey() {
        return customComparatorKey;
    }

    public FaiList<?> getCustomComparatorList() {
        return customComparatorList;
    }

    public String getCustomComparatorTable() {
        return customComparatorTable;
    }


    public MgProductDbSearch setCustomComparator(String customComparatorKey, String customComparatorTable, FaiList<?> customComparatorList) {
        this.customComparatorKey = customComparatorKey;
        this.customComparatorTable = customComparatorTable;
        this.customComparatorList = customComparatorList;
        return this;
    }

    public MgProductDbSearch setFirstComparator(String firstComparatorKey, String firstComparatorTable, boolean firstComparatorKeyOrderByDesc) {
        this.firstComparatorKey = firstComparatorKey;
        this.firstComparatorTable = firstComparatorTable;
        this.firstComparatorKeyOrderByDesc = firstComparatorKeyOrderByDesc;
        return this;
    }

    public MgProductDbSearch setSecondComparator(boolean needSecondComparatorSorting, String secondComparatorKey, String secondComparatorTable, boolean secondComparatorKeyOrderByDesc) {
        this.needSecondComparatorSorting = needSecondComparatorSorting;
        this.secondComparatorKey = secondComparatorKey;
        this.secondComparatorTable = secondComparatorTable;
        this.secondComparatorKeyOrderByDesc = secondComparatorKeyOrderByDesc;
        return this;
    }

    public MgProductDbSearch setComparator(String customComparatorKey, String customComparatorTable, FaiList<?> customComparatorList,
                                           String firstComparatorKey, String firstComparatorTable, boolean firstComparatorKeyOrderByDesc,
                                           boolean needSecondComparatorSorting, String secondComparatorKey, String secondComparatorTable, boolean secondComparatorKeyOrderByDesc) {
        return setCustomComparator(customComparatorKey, customComparatorTable, customComparatorList)
            .setFirstComparator(firstComparatorKey, firstComparatorTable, firstComparatorKeyOrderByDesc)
            .setSecondComparator(needSecondComparatorSorting, secondComparatorKey, secondComparatorTable, secondComparatorKeyOrderByDesc);
    }

    // 保存rlFlag
    private ParamMatcher rlFlagMatcher;

    // 保存flag
    private ParamMatcher flagMatcher;

    // rlFlag
    public MgProductDbSearch setPdRlFlag(boolean and, String op, int mask, int value) {
        String rlFlag = ProductBasicEntity.ProductInfo.RL_FLAG;
        if (rlFlagMatcher == null) {
            rlFlagMatcher = new ParamMatcher();
        }

        if (and) {
            rlFlagMatcher.and(rlFlag, op, mask, value);
        } else {
            rlFlagMatcher.or(rlFlag, op, mask, value);
        }
        return this;
    }

    // flag
    public MgProductDbSearch setPdFlag(boolean and, String op, int mask, int value) {
        String rlFlag = ProductBasicEntity.ProductInfo.FLAG;
        if (flagMatcher == null) {
            flagMatcher = new ParamMatcher();
        }

        if (and) {
            flagMatcher.and(rlFlag, op, mask, value);
        } else {
            flagMatcher.or(rlFlag, op, mask, value);
        }

        return this;
    }

    private void parseParamMatcher(Param dbSearchParam) {
        FaiList<Param> flags = (FaiList<Param>) dbSearchParam.getObject(DbSearchInfo.FLAG_PARAM_MATCHER);
        FaiList<Param> rlFlags = (FaiList<Param>) dbSearchParam.getObject(DbSearchInfo.RLFLAG_PARAM_MATCHER);
        if (!Util.isEmptyList(rlFlags)) {
            // 避免NPE
            this.rlFlagMatcher = ParamMatcher.parseParamMatcher(rlFlags);
        }
        if (!Util.isEmptyList(flags)) {
            // 避免NPE
            this.flagMatcher = ParamMatcher.parseParamMatcher(flags);
        }
    }

    /**
     * 是否有自定义的排序
     */
    public boolean hasCustomComparator() {
        return !Str.isEmpty(customComparatorKey) && !Str.isEmpty(customComparatorTable) && !Util.isEmptyList(customComparatorList);
    }

    /**
     * 是否有第一排序
     * @return true 表示有
     */
    public boolean hasFirstComparator() {
        return !Str.isEmpty(firstComparatorKey) && !Str.isEmpty(firstComparatorTable);
    }

    public String getFirstComparatorTable() {
        return firstComparatorTable;
    }

    public MgProductDbSearch setFirstComparatorTable(String firstComparatorTable) {
        this.firstComparatorTable = firstComparatorTable;
        return this;
    }

    public MgProductDbSearch setSecondComparatorTable(String secondComparatorTable) {
        this.secondComparatorTable = secondComparatorTable;
        return this;
    }

    public MgProductDbSearch setCustomComparatorTable(String customComparatorTable) {
        this.customComparatorTable = customComparatorTable;
        return this;
    }

    public MgProductDbSearch setEnableSearchProductName(boolean enableSearchProductName) {
        this.enableSearchProductName = enableSearchProductName;
        return this;
    }

    public MgProductDbSearch setEnableSearchSkuCode(boolean enableSearchSkuCode) {
        this.enableSearchSkuCode = enableSearchSkuCode;
        return this;
    }

    public MgProductDbSearch setUpSalesStatus(Integer upSalesStatus) {
        this.upSalesStatus = upSalesStatus;
        return this;
    }

    public MgProductDbSearch setFirstComparatorKey(String firstComparatorKey) {
        this.firstComparatorKey = firstComparatorKey;
        return this;
    }

    public MgProductDbSearch setFirstComparatorKeyOrderByDesc(boolean firstComparatorKeyOrderByDesc) {
        this.firstComparatorKeyOrderByDesc = firstComparatorKeyOrderByDesc;
        return this;
    }

    public MgProductDbSearch setNeedSecondComparatorSorting(boolean needSecondComparatorSorting) {
        this.needSecondComparatorSorting = needSecondComparatorSorting;
        return this;
    }

    public MgProductDbSearch setSecondComparatorKey(String secondComparatorKey) {
        this.secondComparatorKey = secondComparatorKey;
        return this;
    }

    public MgProductDbSearch setSecondComparatorKeyOrderByDesc(boolean secondComparatorKeyOrderByDesc) {
        this.secondComparatorKeyOrderByDesc = secondComparatorKeyOrderByDesc;
        return this;
    }

    public MgProductDbSearch setAddTimeBegin(Calendar addTimeBegin) {
        this.addTimeBegin = addTimeBegin;
        return this;
    }

    public MgProductDbSearch setAddTimeEnd(Calendar addTimeEnd) {
        this.addTimeEnd = addTimeEnd;
        return this;
    }


    public MgProductDbSearch setPriceBegin(Long priceBegin) {
        this.priceBegin = priceBegin;
        return this;
    }

    public MgProductDbSearch setPriceEnd(Long priceEnd) {
        this.priceEnd = priceEnd;
        return this;
    }

    public MgProductDbSearch setSalesBegin(Integer salesBegin) {
        this.salesBegin = salesBegin;
        return this;
    }

    public MgProductDbSearch setSalesEnd(Integer salesEnd) {
        this.salesEnd = salesEnd;
        return this;
    }

    public MgProductDbSearch setRemainCountBegin(Integer remainCountBegin) {
        this.remainCountBegin = remainCountBegin;
        return this;
    }

    public MgProductDbSearch setRemainCountEnd(Integer remainCountEnd) {
        this.remainCountEnd = remainCountEnd;
        return this;
    }

    public MgProductDbSearch setCustomComparatorKey(String customComparatorKey) {
        this.customComparatorKey = customComparatorKey;
        return this;
    }

    public MgProductDbSearch setCustomComparatorList(FaiList<?> customComparatorList) {
        this.customComparatorList = customComparatorList;
        return this;
    }

    @Override
    public String toString() {
        return getSearchParam().toJson();
    }
}
