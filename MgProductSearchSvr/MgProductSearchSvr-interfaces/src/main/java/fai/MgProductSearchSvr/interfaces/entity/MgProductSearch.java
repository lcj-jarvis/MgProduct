package fai.MgProductSearchSvr.interfaces.entity;

import fai.MgProductPropSvr.interfaces.entity.ProductPropValEntity;
import fai.comm.util.*;
import fai.MgProductBasicSvr.interfaces.entity.*;
import fai.MgProductStoreSvr.interfaces.entity.*;

public class MgProductSearch {
    public static final class Info {
        public static final String UP_SALES_STATUS = "upSalesStatus";  // 上下架状态，默认是获取上架的状态
        public static final String RL_GROUP_ID_LIST = "rlGroupIdList";     // 商品的分类idList
        public static final String RL_LABLE_ID_LIST = "rlLableIdList";     //  商品标签idList
        public static final String RL_PD_ID_LIST = "rlPdIdList";      // 商品业务idList
        public static final String TYPE_LIST = "typeList";            // 商品类型：实物、卡密、酒店
        public static final String RL_LIB_ID_LIST = "rlLibIdList";               //  在哪些库搜索，默认是全部库

        public static final String SEARCH_KEY_WORD = "searchKeyWord";        // 搜索关键词
        public static final String ENABLE_SEARCH_PRODUCT_NAME = "enableSearchProductName";  // 是否允许搜索商品名称, 默认是 true
        public static final String ENABLE_SEARCH_PRODUCT_PROP = "enableSearchProductProp";  // 是否允许搜索商品参数, 默认是 true
        public static final String PROP_ID_LIST = "propIdList";   //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
        public static final String ENABLE_SEARCH_PRODUCT_REMARK = "enableSearchProductRemark";  // 是否允许搜索商品详情, 默认是 true
        public static final String SEARCH_PRODUCT_REMARK_KEY_LIST = "searchProductRemarkKeyList";   // 搜索商品详情 keyList, 可能区分 mobi key、site key，合适由各个项目传入进来

        public static final String RL_PROP_VAL_ID_LIST = "rlPropValIdList";  //  根据 参数值 搜索
        public static final String ADD_TIME_BEGIN = "addTimeBegin";   // 搜索商品  开始录入时间, 需要传入 "xxxx-xx-xx xx:xx:xx" 的格式
        public static final String ADD_TIME_END = "addTimeEnd";        // 搜索商品 结束录入时间，需要传入 "xxxx-xx-xx xx:xx:xx" 的格式
        public static final String PRICE_BEGIN = "priceBegin";   // 搜索商品  开始价格
        public static final String PRICE_END = "priceEnd";        // 搜索商品 结束价格

        public static final String START = "start";  //  分页位置
        public static final String LIMIT = "limit";  //  分页条数

        public static final String RL_PD_ID_COMPARATOR_LIST  = "rlPdIdComparatorList";  // 商品idList 排序，设置了这个排序，其他设置的排序就无效了

        public static final String FIRST_COMPARATOR_TABLE = "firstComparatorTable";    // 第一排序字段的table
        public static final String FIRST_COMPARATOR_KEY = "firstComparatorKey";     // 第一排序字段
        public static final String FIRST_COMPARATOR_KEY_ORDER_BY_DESC = "firstComparatorKeyOrderByDesc";  // 顺序 还是 倒序，默认是顺序

        public static final String SECOND_COMPARATOR_TABLE = "secondComparatorTable";  // 第二排序字段的table
        public static final String SECOND_COMPARATOR_KEY = "secondComparatorKey";  // 第二排序字段必须是能够确定唯一的排序字段的，如果不能，会重写为 id 倒序排序
        public static final String SECOND_COMPARATOR_KEY_ORDER_BY_DESC = "secondComparatorKeyOrderByDesc";  // 顺序 还是 倒序，默认是顺序
    }

    //  上下架搜索参数封装
    public enum UpSalesStatusEnum{
        ALL(0), // 全部
        UP_SALES(1),  // 上架
        DOWN_SALES(2), // 下架
        DELETE(3), // 删除
        UP_AND_DOWN_SALES(4); // 上架和下架的
        public int upSalesStatus;
        private UpSalesStatusEnum(int upSalesStatus){
            this.upSalesStatus = upSalesStatus;
        }
        public int getUpSalesStatus(){
            return upSalesStatus;
        }
    }

    // 排序指定 table, table 待完善
    public enum SearchTableNameEnum{
        MG_PRODUCT("mgProduct"),
        MG_PRODUCT_REL("mgProductRel"),
        MG_PRODUCT_BIND_PROP("mgProductBindProp"),
        MG_PRODUCT_GROUP_REL("mgProductGroupRel"),
        MG_PRODUCT_LABLE_REL("mgProductLableRel"),
        MG_SPU_BIZ_SUMMARY("mgSpuBizSummary");

        public String searchTableName;
        private SearchTableNameEnum(String searchTableName) {
            this.searchTableName = searchTableName;
        }
        public String getSortTableName(){
            return searchTableName;
        }
    }

    // 上下架开始
    private int upSalesStatus = 1;           //  默认是上架的, 对应 ProductRelValObj.Status
    private FaiList<Integer> rlGroupIdList;  // 业务商品分类 IdList
    private FaiList<Integer> rlLableIdList;   //  业务商品标签搜索
    private FaiList<Integer> rlPdIdList;      // 业务商品 IdList
    private FaiList<Integer> typeList;       // 商品类型：实物、卡密、酒店
    private String addTimeBegin;
    private String addTimeEnd;           // 如果搜索的 创建时间 开始 和 创建时间 结束是相同的，则走 EQ 的逻辑
    private FaiList<Integer> rlLibIdList;        //  在哪些库搜索，默认是全部库
    private String searchKeyWord;            //  搜索的关键字，会关联商品名称、商品对应的参数
    private boolean enableSearchProductName = false;   // 是否允许搜索商品名称, 默认是 false
    private boolean enableSearchProductProp = false;   // 是否允许搜索商品参数, 默认是 false
    private FaiList<Integer> propIdList;    //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
    private boolean enableSearchProductRemark = false;   // 是否允许搜索商品详情, 默认是 false
    private FaiList<String> searchProductRemarkKeyList;  //  在哪些富文本详情下搜索, 与 searchKeyWord 匹配使用
    private FaiList<Integer> rlPropValIdList;   //  根据 参数值 搜索

    private Long priceBegin;   //  最小交易价格
    private Long priceEnd;     //  最大交易价格，如果最小交易价格和最大交易价格一样，


    private FaiList<Integer> rlPdIdComparatorList;  // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
    private String firstComparatorTable;  // 第一排序字段的table
    private String firstComparatorKey;   // 第一排序字段
    private boolean firstComparatorKeyOrderByDesc = false;   // 第一排序字段的顺序, 默认顺序
    private String secondComparatorTable;                // 第二排序字段
    private String secondComparatorKey;                  // 第二排序字段
    private boolean secondComparatorKeyOrderByDesc = false;   // 第二排序字段的顺序, 默认顺序

    private int start = 0; // 搜索的开始位置
    private int limit = 100;  // 分页限制条数开始，默认最大是 100


    // 把查询条件转换为 Param
    public Param getSearchParam(){
        Param param = new Param();
        param.setInt(Info.UP_SALES_STATUS, upSalesStatus);     // 上下架
        param.setList(Info.RL_GROUP_ID_LIST, rlGroupIdList);   // 业务商品分类
        param.setList(Info.RL_LABLE_ID_LIST, rlLableIdList);   // 业务商品标签
        param.setList(Info.RL_PD_ID_LIST, rlPdIdList);          // priceEnd
        param.setList(Info.TYPE_LIST, typeList);                // 商品类型：实物、卡密、酒店， 对应 ProductEntity.Info.PD_TYPE 字段
        param.setList(Info.RL_LIB_ID_LIST, rlLibIdList);           //  在哪些库搜索，默认是全部库
        param.setString(Info.SEARCH_KEY_WORD, searchKeyWord);  // 商品搜索关键词
        param.setBoolean(Info.ENABLE_SEARCH_PRODUCT_NAME, enableSearchProductName);   // 是否允许搜索商品名称, 默认是 false
        param.setBoolean(Info.ENABLE_SEARCH_PRODUCT_PROP, enableSearchProductProp);   // 是否允许搜索商品参数, 默认是 false
        param.setList(Info.PROP_ID_LIST, propIdList);     //  在哪些参数下搜索, 与 searchKeyWord 匹配使用
        param.setBoolean(Info.ENABLE_SEARCH_PRODUCT_REMARK, enableSearchProductRemark);  // 是否允许搜索商品详情, 默认是 false
        param.setList(Info.SEARCH_PRODUCT_REMARK_KEY_LIST, searchProductRemarkKeyList);

        param.setList(Info.RL_PROP_VAL_ID_LIST, rlPropValIdList);  //  根据 参数值 搜索
        param.setString(Info.ADD_TIME_BEGIN, addTimeBegin);  // 搜索商品  开始录入时间
        param.setString(Info.ADD_TIME_END, addTimeEnd);      // 搜索商品  结束录入时间
        param.setLong(Info.PRICE_BEGIN, priceBegin);     // 搜索商品 开始价格
        param.setLong(Info.PRICE_END, priceEnd);         // 搜索商品  结束价格

        param.setList(Info.RL_PD_ID_COMPARATOR_LIST, rlPdIdComparatorList);   // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        param.setString(Info.FIRST_COMPARATOR_TABLE, firstComparatorTable); // 第一排序table
        param.setString(Info.FIRST_COMPARATOR_KEY, firstComparatorKey);  // 第一排序字段
        param.setBoolean(Info.FIRST_COMPARATOR_KEY_ORDER_BY_DESC, firstComparatorKeyOrderByDesc);  // 第一排序字段的顺序
        param.setString(Info.SECOND_COMPARATOR_TABLE, secondComparatorTable);         // 第二排序table
        param.setString(Info.SECOND_COMPARATOR_KEY, secondComparatorKey);             // 第二排序字段
        param.setBoolean(Info.SECOND_COMPARATOR_KEY_ORDER_BY_DESC, secondComparatorKeyOrderByDesc); // 第二排序字段的顺序

        param.setInt(Info.START, start);   // 分页获取的起始位置
        param.setInt(Info.LIMIT, limit);   // 分页获取的起始位置
        return param;
    }

    public void initProductSearch(Param searchParam) {
        this.upSalesStatus = searchParam.getInt(Info.UP_SALES_STATUS);   // 上下架
        this.rlGroupIdList = searchParam.getList(Info.RL_GROUP_ID_LIST); // 业务商品分类
        this.rlLableIdList = searchParam.getList(Info.RL_LABLE_ID_LIST); // 业务商品标签
        this.rlPdIdList = searchParam.getList(Info.RL_PD_ID_LIST);        // 业务商品idList
        this.typeList = searchParam.getList(Info.TYPE_LIST);               // 商品类型：实物、卡密、酒店

        this.rlLibIdList = searchParam.getList(Info.RL_LIB_ID_LIST);                       //  在哪些库搜索，默认是全部库
        this.searchKeyWord = searchParam.getString(Info.SEARCH_KEY_WORD);   // 商品搜索关键词
        this.enableSearchProductName = searchParam.getBoolean(Info.ENABLE_SEARCH_PRODUCT_NAME);  // 是否允许搜索商品名称, 默认是 true
        this.enableSearchProductProp = searchParam.getBoolean(Info.ENABLE_SEARCH_PRODUCT_PROP);  // 是否允许搜索商品参数, 默认是 true
        this.propIdList = searchParam.getList(Info.PROP_ID_LIST);  //  在哪些参数下筛选
        this.enableSearchProductRemark = searchParam.getBoolean(Info.ENABLE_SEARCH_PRODUCT_REMARK); // 是否允许搜索商品详情, 默认是 true
        this.searchProductRemarkKeyList = searchParam.getList(Info.SEARCH_PRODUCT_REMARK_KEY_LIST);
        this.rlPropValIdList = searchParam.getList(Info.RL_PROP_VAL_ID_LIST);   // 根据 参数值 搜索
        this.addTimeBegin = searchParam.getString(Info.ADD_TIME_BEGIN);   // 搜索商品开始 录入时间
        this.addTimeEnd = searchParam.getString(Info.ADD_TIME_END);       // 搜索商品结束 录入时间
        this.priceBegin = searchParam.getLong(Info.PRICE_BEGIN);          // 搜索商品结束 价格
        this.priceEnd = searchParam.getLong(Info.PRICE_END);              // 搜索商品结束 价格

        this.rlPdIdComparatorList = searchParam.getList(Info.RL_PD_ID_COMPARATOR_LIST);   // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        this.firstComparatorTable = searchParam.getString(Info.FIRST_COMPARATOR_TABLE);   // 第一排序字段table
        this.firstComparatorKey = searchParam.getString(Info.FIRST_COMPARATOR_KEY);   // 第一排序字段
        this.firstComparatorKeyOrderByDesc = searchParam.getBoolean(Info.FIRST_COMPARATOR_KEY_ORDER_BY_DESC);   // 第一排序字段的顺序
        this.secondComparatorTable = searchParam.getString(Info.SECOND_COMPARATOR_TABLE);   // 第二排序字段table
        this.secondComparatorKey = searchParam.getString(Info.SECOND_COMPARATOR_KEY);    // 第二排序字段
        this.secondComparatorKeyOrderByDesc = searchParam.getBoolean(Info.SECOND_COMPARATOR_KEY_ORDER_BY_DESC);   // 第二排序字段的顺序

        this.start = searchParam.getInt(Info.START);    // 分页获取的起始位置
        this.limit = searchParam.getInt(Info.LIMIT);    // 分页获取的起始位置
    }

    // 在 "商品基础表" mgProduct_xxxx 搜索
    public ParamMatcher getProductBasicSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductName){
            //  商品名称, 名称 like 查询
            paramMatcher.and(ProductEntity.Info.NAME, ParamMatcher.LK, searchKeyWord);
        }
        // 商品类型
        if(typeList != null && !typeList.isEmpty()){
            paramMatcher.and(ProductEntity.Info.PD_TYPE, ParamMatcher.IN, typeList);
        }
        return paramMatcher;
    }

    // 在 "商品基础表" mgProduct_xxxx 搜索
    public ParamMatcher getProductBasicSearchOrMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductRemark){
            //  商品名称, 名称 like 查询
            paramMatcher.and(ProductEntity.Info.NAME, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }


    // 在 "在商品 富文本 字段"  搜索
    public ParamMatcher getProductRemarkSearchOrMatcher(ParamMatcher paramMatcher){
        ParamMatcher paramMatcherOr = new ParamMatcher();
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductName && searchProductRemarkKeyList != null && !searchProductRemarkKeyList.isEmpty()){
            //  商品名称, 名称 like 查询
            for(String remarkKey : searchProductRemarkKeyList){
                paramMatcherOr.or(remarkKey, ParamMatcher.LK, searchKeyWord);
            }
        }
        if(paramMatcher == null || paramMatcherOr.isEmpty()){
            return paramMatcherOr;
        }else{
            paramMatcher.and(paramMatcherOr);
            return paramMatcher;
        }
    }

    // 在 "参数值表" mgProductPropVal_xxxx 搜索
    public ParamMatcher getProductPropValSearchOrMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        if(!Str.isEmpty(searchKeyWord) && enableSearchProductProp && propIdList != null && !propIdList.isEmpty()){
            if(propIdList.size() == 1){
                paramMatcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.EQ, propIdList.get(0));
            }else{
                paramMatcher.and(ProductPropValEntity.Info.PROP_ID, ParamMatcher.IN, propIdList);
            }
            // 对参数值进行 like 搜索
            paramMatcher.and(ProductPropValEntity.Info.VAL, ParamMatcher.LK, searchKeyWord);
        }
        return paramMatcher;
    }


    // 在 "商品业务关系表" mgProductRel_xxxx 搜索
    public ParamMatcher getProductRelSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }

        // 业务商品idList
        if(rlPdIdList != null && !rlPdIdList.isEmpty()){
            paramMatcher.and(ProductRelEntity.Info.RL_PD_ID, ParamMatcher.IN, rlPdIdList);
        }

        //  商品库
        if(rlLibIdList != null && !rlLibIdList.isEmpty()){
            if(rlLibIdList.size() == 1){
                paramMatcher.and(ProductRelEntity.Info.RL_LIB_ID, ParamMatcher.EQ, rlLibIdList.get(0));
            }else{
                paramMatcher.and(ProductRelEntity.Info.RL_LIB_ID, ParamMatcher.IN, rlLibIdList);
            }
        }

        // 上架或者下架的
        if (upSalesStatus == UpSalesStatusEnum.UP_AND_DOWN_SALES.upSalesStatus){
            FaiList<Integer> statusList = new FaiList<Integer>();
            statusList.add(ProductRelValObj.Status.UP);
            statusList.add(ProductRelValObj.Status.DOWN);
            paramMatcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.IN, statusList);
        }else if(upSalesStatus != UpSalesStatusEnum.ALL.upSalesStatus){   //  非全部的
            paramMatcher.and(ProductRelEntity.Info.STATUS, ParamMatcher.EQ, upSalesStatus);
        }

        //  商品录入时间
        if(!Str.isEmpty(addTimeBegin) || !Str.isEmpty(addTimeEnd)){
            if(Str.equals(addTimeBegin, addTimeEnd)){
                paramMatcher.and(ProductRelEntity.Info.ADD_TIME, ParamMatcher.EQ, addTimeBegin);
            }else{
                //  创建时间大于
                if(!Str.isEmpty(addTimeBegin)){
                    paramMatcher.and(ProductRelEntity.Info.ADD_TIME, ParamMatcher.GE, addTimeBegin);
                }
                //  创建时间小于
                if(!Str.isEmpty(addTimeEnd)){
                    paramMatcher.and(ProductRelEntity.Info.ADD_TIME, ParamMatcher.LE, addTimeEnd);
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
        if(rlPropValIdList != null && !rlPropValIdList.isEmpty()){
            if(rlPropValIdList.size() == 1){
                paramMatcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.EQ, rlPropValIdList.get(0));
            }else{
                paramMatcher.and(ProductBindPropEntity.Info.PROP_VAL_ID, ParamMatcher.IN, rlPropValIdList);
            }
        }
        return paramMatcher;
    }


    // 在 "分类业务关系表" mgProductGroupRel_xxxx 搜索
    public ParamMatcher getProductGroupRelSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 业务商品分类
        if(rlGroupIdList != null && !rlGroupIdList.isEmpty()){
            if(rlGroupIdList.size() == 1){
                paramMatcher.and("rlGroupId", ParamMatcher.EQ, rlGroupIdList.get(0));
            }else{
                paramMatcher.and("rlGroupId", ParamMatcher.IN, rlGroupIdList);
            }
        }
        return paramMatcher;
    }

    // 在 "标签业务关系表" mgProductLableRel_xxxx 搜索
    public ParamMatcher getProductLableRelSearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 业务商品标签
        if(rlLableIdList != null && !rlLableIdList.isEmpty()){
            if(rlLableIdList.size() == 1){
                paramMatcher.and("rlLableId", ParamMatcher.EQ, rlLableIdList.get(0));
            }else{
                paramMatcher.and("rlLableId", ParamMatcher.IN, rlLableIdList);
            }
        }
        return paramMatcher;
    }


    // 在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索
    public ParamMatcher getProductSpuBizSummarySearchMatcher(ParamMatcher paramMatcher){
        if(paramMatcher == null){
            paramMatcher = new ParamMatcher();
        }
        // 商品价格
        if(priceBegin != null || priceEnd != null){
            if(priceBegin != null && priceEnd != null && priceBegin.longValue() == priceEnd.longValue()){
                paramMatcher.and(SpuBizSummaryEntity.Info.MIN_PRICE, ParamMatcher.EQ, priceBegin);
                paramMatcher.and(SpuBizSummaryEntity.Info.MAX_PRICE, ParamMatcher.EQ, priceBegin);
            }else{
                if(priceBegin != null){
                    paramMatcher.and(SpuBizSummaryEntity.Info.MIN_PRICE, ParamMatcher.GE, priceBegin);
                }
                if(priceEnd != null){
                    paramMatcher.and(SpuBizSummaryEntity.Info.MIN_PRICE, ParamMatcher.LE, priceEnd);
                }
            }
        }
        return paramMatcher;
    }

    //  把排序转换为 ParamComparator
    public ParamComparator getFirstParamComparator(){
        ParamComparator paramComparator = new ParamComparator();
        // 商品idList 排序，设置了这个排序，其他设置的排序就无效了
        if(rlPdIdComparatorList != null && !rlPdIdComparatorList.isEmpty()){
            paramComparator.addKey(ProductRelEntity.Info.RL_PD_ID, rlPdIdComparatorList);
            paramComparator.addKey(ProductRelEntity.Info.RL_PD_ID, true);   // 如果 rlPdIdComparatorList 排不了序的数据，按业务商品 id 重新排序
        }else{
            if(!Str.isEmpty(this.firstComparatorTable) && !Str.isEmpty(this.firstComparatorKey)){
                paramComparator.addKey(this.firstComparatorKey, this.firstComparatorKeyOrderByDesc);
            }
            // 第二排序也是在同一张表
            if(!Str.isEmpty(this.secondComparatorTable) && !Str.isEmpty(this.secondComparatorKey) && this.secondComparatorTable.equals(this.firstComparatorTable)){
                paramComparator.addKey(this.secondComparatorKey, this.secondComparatorKeyOrderByDesc);
            }
        }
        return paramComparator;
    }

    public ParamComparator getSecondParamComparator(){
        ParamComparator paramComparator = new ParamComparator();
        if(rlPdIdComparatorList != null && !rlPdIdComparatorList.isEmpty()){
            // 已经有第一排序
            return paramComparator;
        }
        if(Str.isEmpty(this.firstComparatorTable) || Str.isEmpty(this.firstComparatorKey)){
            // 如果没有第一排序，不允许第二排序
            return paramComparator;
        }
        if(!Str.isEmpty(this.secondComparatorTable) && !Str.isEmpty(this.secondComparatorKey)){
            paramComparator.addKey(this.secondComparatorKey, this.secondComparatorKeyOrderByDesc);
        }
        return paramComparator;
    }

    //  把把查询条件转换为 SearchArg
    public void setSearArgStartAndLimit(SearchArg searchArg){
        if(searchArg == null){
            return;
        }
        searchArg.start = this.start;
        searchArg.limit = this.limit;
    }

    public FaiList<Integer> getRlLableIdList() {
        return rlLableIdList;
    }
    public MgProductSearch setRlLableIdList(FaiList<Integer> rlLableIdList) {
        this.rlLableIdList = rlLableIdList;
        return this;
    }
    public MgProductSearch setRlPdIdList(FaiList<Integer> rlPdIdList) {
        this.rlPdIdList = rlPdIdList;
        return this;
    }
    public FaiList<Integer> getRlPdIdList() {
        return rlPdIdList;
    }

    public MgProductSearch setUpSalesStatus(UpSalesStatusEnum upSalesStatusEnum){
        this.upSalesStatus = upSalesStatusEnum.upSalesStatus;
        return this;
    }
    public int getUpSalesStatus(){
        return this.upSalesStatus;
    }
    public FaiList<Integer> getTypeList() {
        return typeList;
    }
    public MgProductSearch setTypeList(FaiList<Integer> typeList) {
        this.typeList = typeList;
        return this;
    }
    public FaiList<Integer> getRlGroupIdList() {
        return rlGroupIdList;
    }

    public MgProductSearch setRlGroupIdList(FaiList<Integer> rlGroupIdList) {
        this.rlGroupIdList = rlGroupIdList;
        return this;
    }

    public FaiList<Integer> getRlLibIdList() {
        return rlLibIdList;
    }
    public MgProductSearch setRlLibIdList(FaiList<Integer> rlLibIdList) {
        this.rlLibIdList = rlLibIdList;
        return this;
    }

    public String getSearchKeyWord() {
        return searchKeyWord;
    }
    // 如果商品参数的搜索rlPropIdList没有设置的话，那就只是代表商品名称like搜索
    public MgProductSearch setSearchKeyWord(String searchKeyWord, boolean enableSearchProductName, boolean enableSearchProductProp, FaiList<Integer> propIdList, boolean enableSearchProductRemark, FaiList<String> searchProductRemarkKeyList) {
        this.searchKeyWord = searchKeyWord;
        this.enableSearchProductName = enableSearchProductName;
        this.enableSearchProductProp = enableSearchProductProp;
        this.propIdList = propIdList;
        this.enableSearchProductRemark = enableSearchProductRemark;
        this.searchProductRemarkKeyList = searchProductRemarkKeyList;
        return this;
    }

    public boolean getEnableSearchProductName() {
        return enableSearchProductName;
    }
    public MgProductSearch setEnableSearchProductName(boolean enableSearchProductName) {
        this.enableSearchProductName = enableSearchProductName;
        return this;
    }
    public boolean getEnableSearchProductProp() {
        return enableSearchProductProp;
    }
    public MgProductSearch setEnableSearchProductProp(boolean enableSearchProductProp) {
        this.enableSearchProductProp = enableSearchProductProp;
        return this;
    }
    public boolean getEnableSearchProductRemark() {
        return enableSearchProductRemark;
    }
    public MgProductSearch setEnableSearchProductRemark(boolean enableSearchProductRemark) {
        this.enableSearchProductRemark = enableSearchProductRemark;
        return this;
    }
    public FaiList<String> getSearchProductRemarkKeyList() {
        return searchProductRemarkKeyList;
    }
    public MgProductSearch setSearchProductRemarkKeyList(FaiList<String> searchProductRemarkKeyList) {
        this.searchProductRemarkKeyList = searchProductRemarkKeyList;
        return this;
    }
    public MgProductSearch setPropIdList(FaiList<Integer> propIdList) {
        this.propIdList = propIdList;
        return this;
    }

    public FaiList<Integer> getPropIdList() {
        return propIdList;
    }
    public FaiList<Integer> getRlPropValIdList() {
        return rlPropValIdList;
    }
    public MgProductSearch setRlPropValIdList(FaiList<Integer> rlPropValIdList) {
        this.rlPropValIdList = rlPropValIdList;
        return this;
    }

    public MgProductSearch setAddTime(String addTimeBegin, String addTimeEnd) {
        this.addTimeBegin = addTimeBegin;
        this.addTimeEnd = addTimeEnd;
        return this;
    }
    public String getAddTimeBegin() {
        return addTimeBegin;
    }
    public String getAddTimeEnd() {
        return addTimeEnd;
    }

    public MgProductSearch setPrice(Long priceBegin, Long priceEnd) {
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

    public FaiList<Integer> getRlPdIdComparatorList() {
        return rlPdIdComparatorList;
    }
    public void setRlPdIdComparatorList(FaiList<Integer> rlPdIdComparatorList) {
        this.rlPdIdComparatorList = rlPdIdComparatorList;
    }

    public MgProductSearch setStart(int start){
        this.start = start >= 0 ? start : this.start;
        return this;
    }
    public int getStart(){
        return this.start;
    }

    public MgProductSearch setLimit(int limit){
        this.limit = limit >= 0 ? limit : this.limit;
        return this;
    }
    public int getLimit(){
        return this.limit;
    }

    public MgProductSearch setFirstComparator(Pair<SearchTableNameEnum, String> firstComparatorTableAndKey, boolean desc){
        if(Str.isEmpty(firstComparatorTableAndKey.first.searchTableName) || Str.isEmpty(firstComparatorTableAndKey.second)){
            return this;
        }
        this.firstComparatorTable = firstComparatorTableAndKey.first.searchTableName;
        this.firstComparatorKey = firstComparatorTableAndKey.second;
        this.firstComparatorKeyOrderByDesc = desc;
        return this;
    }
    public String getFirstComparatorTable() {
        return firstComparatorTable;
    }
    public String getFirstComparatorKey(){
        return this.firstComparatorKey;
    }
    public boolean getFirstComparatorKeyOrderByDesc(){
        return this.firstComparatorKeyOrderByDesc;
    }
    public MgProductSearch setSecondComparator(Pair<SearchTableNameEnum, String> secondComparatorTableAndKey, boolean desc){
        if(Str.isEmpty(secondComparatorTableAndKey.first.searchTableName) || Str.isEmpty(secondComparatorTableAndKey.second)){
            return this;
        }
        this.secondComparatorTable = secondComparatorTableAndKey.first.searchTableName;
        this.secondComparatorKey = secondComparatorTableAndKey.second;
        this.secondComparatorKeyOrderByDesc = desc;
        return this;
    }
    public String getSecondComparatorTable() {
        return secondComparatorTable;
    }
    public String getSecondComparatorKey(){
        return this.secondComparatorKey;
    }
    public boolean getSecondComparatorKeyOrderByDesc(){
        return this.secondComparatorKeyOrderByDesc;
    }
}
