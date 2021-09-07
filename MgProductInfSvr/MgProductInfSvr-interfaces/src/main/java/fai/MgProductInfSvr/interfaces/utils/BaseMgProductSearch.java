package fai.MgProductInfSvr.interfaces.utils;

import fai.comm.util.Param;

/**
 * @author Lu
 * @date 2021-08-16 18:01
 *
 * 抽象的查询父类
 * 包含db和es的公共查询字段
 */
public abstract class BaseMgProductSearch {

    /**
     * db中的搜索的关键字，会关联商品名称、商品对应的参数
     * es中的目前只是name
     */
    protected String searchKeyWord;

    /**
     * 关键词用做商品名称搜索，默认是false
     */
    protected boolean enableSearchProductName = false;

    /**
     * 上下架开始
     * 默认是上下架都搜索, 对应 ProductRelValObj.Status字段
     */
    protected int upSalesStatus = UpSalesStatusEnum.ALL.upSalesStatus;

    /**
     * 第一排序字段
     */
    protected String firstComparatorKey;

    /**
     * 第一排序字段的顺序, 默认顺序
     */
    protected boolean firstComparatorKeyOrderByDesc = false;

    /**
     * 选择是否使用第二的排序。默认不开启
     */
    protected boolean needSecondComparatorSorting = false;

    /**
     * 第二排序字段
     * 目前db里的第二排序字段默认是rlPdId，通常db里的第二排序字段也只有这个。
     */
    protected String secondComparatorKey;

    /**
     * 第二排序字段的顺序，默认顺序
     */
    protected boolean secondComparatorKeyOrderByDesc = false;

    /**
     * 搜索的开始位置
     */
    protected int start = 0;

    /**
     * 分页限制条数开始，默认最大是 200
     */
    protected int limit = MAX_LIMIT;

    public static final Integer MAX_LIMIT = 200;

    /**
     * 上下架搜索参数封装
     */
    public enum UpSalesStatusEnum{
        // 全部的状态
        ALL(0),
        // 上架
        UP_SALES(1),
        // 下架
        DOWN_SALES(2),
        // 删除
        DELETE(3),
        // 上架和下架的
        UP_AND_DOWN_SALES(4);
        public int upSalesStatus;
        private UpSalesStatusEnum(int upSalesStatus){
            this.upSalesStatus = upSalesStatus;
        }
        public int getUpSalesStatus(){
            return upSalesStatus;
        }
    }

    /**
     * 把查询条件转换为 Param
     * @return 包含查询条件的 Param
     */
    public abstract Param getSearchParam();

    /**
     * 初始化查询条件
     * @param searchParam 查询条件
     */
    public abstract void initSearchParam(Param searchParam);

    public static final class BaseSearchInfo {

        /**
         * 搜索关键词
         */
        public static final String SEARCH_KEYWORD = "searchKeyWord";

        /**
         * 上下架状态，默认是获取上架的状态
         */
        public static final String UP_SALES_STATUS = "upSalesStatus";

        /**
         * 允许关键词用于搜索商品名称
         */
        public static final String ENABLE_SEARCH_PRODUCT_NAME = "enableSearchProductName";

        /**
         * 第一排序字段
         */
        public static final String FIRST_COMPARATOR_KEY = "firstComparatorKey";

        /**
         * 升序还是倒序，默认是升序
         */
        public static final String FIRST_COMPARATOR_KEY_ORDER_BY_DESC = "firstComparatorKeyOrderByDesc";

        /**
         * 是否需要第二排序
         */
        public static final String NEED_SECOND_COMPARATOR_SORTING = "needSecondComparatorSorting";

        /**
         * 第二排序字段必须是能够确定唯一的排序字段的，如果不能，会重写为 id 倒序排序
         */
        public static final String SECOND_COMPARATOR_KEY = "secondComparatorKey";

        /**
         * 升序还是倒序，默认是升序
         */
        public static final String SECOND_COMPARATOR_KEY_ORDER_BY_DESC = "secondComparatorKeyOrderByDesc";

        /**
         * 分页位置
         */
        public static final String START = "start";

        /**
         * 分页条数
         */
        public static final String LIMIT = "limit";
    }

    /**
     * 初始化公共的查询条件
     * @param baseSearchParam
     */
    protected void initBaseSearchParam(Param baseSearchParam) {
        this.searchKeyWord = baseSearchParam.getString(BaseSearchInfo.SEARCH_KEYWORD);
        this.enableSearchProductName = baseSearchParam.getBoolean(BaseSearchInfo.ENABLE_SEARCH_PRODUCT_NAME, false);
        this.upSalesStatus = baseSearchParam.getInt(BaseSearchInfo.UP_SALES_STATUS, UpSalesStatusEnum.ALL.upSalesStatus);

        this.firstComparatorKey = baseSearchParam.getString(BaseSearchInfo.FIRST_COMPARATOR_KEY);
        this.firstComparatorKeyOrderByDesc = baseSearchParam.getBoolean(BaseSearchInfo.FIRST_COMPARATOR_KEY_ORDER_BY_DESC, false);
        this.needSecondComparatorSorting = baseSearchParam.getBoolean(BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING, false);
        this.secondComparatorKey = baseSearchParam.getString(BaseSearchInfo.SECOND_COMPARATOR_KEY);
        this.secondComparatorKeyOrderByDesc = baseSearchParam.getBoolean(BaseSearchInfo.SECOND_COMPARATOR_KEY_ORDER_BY_DESC, false);

        this.start = baseSearchParam.getInt(BaseSearchInfo.START, 0);
        // 最大分页数设置为200
        this.limit = baseSearchParam.getInt(BaseSearchInfo.LIMIT, MAX_LIMIT);
    }

    /**
     * 将公共的查询条件转换为Param
     */
    protected Param getBaseSearchParam() {
        Param baseParam = new Param();

        baseParam.setString(BaseSearchInfo.SEARCH_KEYWORD, searchKeyWord);
        baseParam.setBoolean(BaseSearchInfo.ENABLE_SEARCH_PRODUCT_NAME, enableSearchProductName);
        baseParam.setInt(BaseSearchInfo.UP_SALES_STATUS, upSalesStatus);
        baseParam.setString(BaseSearchInfo.FIRST_COMPARATOR_KEY, firstComparatorKey);
        baseParam.setBoolean(BaseSearchInfo.FIRST_COMPARATOR_KEY_ORDER_BY_DESC, firstComparatorKeyOrderByDesc);
        baseParam.setBoolean(BaseSearchInfo.NEED_SECOND_COMPARATOR_SORTING, needSecondComparatorSorting);
        baseParam.setString(BaseSearchInfo.SECOND_COMPARATOR_KEY, secondComparatorKey);
        baseParam.setBoolean(BaseSearchInfo.SECOND_COMPARATOR_KEY_ORDER_BY_DESC, secondComparatorKeyOrderByDesc);
        baseParam.setInt(BaseSearchInfo.START, start);
        baseParam.setInt(BaseSearchInfo.LIMIT, limit);

        return baseParam;
    }

    public String getSearchKeyWord() {
        return searchKeyWord;
    }

    public int getUpSalesStatus() {
        return upSalesStatus;
    }

    public String getFirstComparatorKey() {
        return firstComparatorKey;
    }

    public boolean isFirstComparatorKeyOrderByDesc() {
        return firstComparatorKeyOrderByDesc;
    }

    public boolean isNeedSecondComparatorSorting() {
        return needSecondComparatorSorting;
    }

    public String getSecondComparatorKey() {
        return secondComparatorKey;
    }

    public boolean isSecondComparatorKeyOrderByDesc() {
        return secondComparatorKeyOrderByDesc;
    }

    public BaseMgProductSearch setStart(int start){
        this.start = start >= 0 ? start : this.start;
        return this;
    }

    public int getStart() {
        return start;
    }

    public BaseMgProductSearch setLimit(int limit){
        this.limit = limit;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public boolean isEnableSearchProductName() {
        return enableSearchProductName;
    }
}
