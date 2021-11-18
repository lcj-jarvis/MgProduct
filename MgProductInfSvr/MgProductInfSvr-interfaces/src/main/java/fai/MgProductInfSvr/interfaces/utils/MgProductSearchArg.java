package fai.MgProductInfSvr.interfaces.utils;

import com.google.common.base.Objects;
import fai.comm.util.Param;
import fai.comm.util.Str;

/**
 * @author Lu
 * @date 2021-08-18 16:05
 *
 * 对外的搜索类，内置了MgProductDbSearch搜索类和MgProductEsSearch搜索类
 */
public class MgProductSearchArg {
    /**
     * 保存es的搜索条件
     */
    private MgProductEsSearch mgProductEsSearch;

    /**
     * 保存db的搜索条件
     */
    private MgProductDbSearch mgProductDbSearch;

    /**
     * 分页的开始位置
     */
    private Integer start = 0;

    /**
     * 分页限制条数，默认最大是 200
     */
    private Integer limit = MAX_LIMIT;

    public static final Integer MAX_LIMIT = 200;

    /**
     * 接收搜索结果的总条数
     */
    private Integer total;

    public MgProductSearchArg() {
    }

    public MgProductSearchArg(MgProductEsSearch mgProductEsSearch, MgProductDbSearch mgProductDbSearch) {
        this.mgProductEsSearch = mgProductEsSearch;
        this.mgProductDbSearch = mgProductDbSearch;
    }

    public Param getDbSearchParam() {
        if (mgProductDbSearch == null) {
            return new Param();
        }
        return mgProductDbSearch.getSearchParam();
    }

    public Param getEsSearchParam() {
        if (mgProductEsSearch == null) {
            return new Param();
        }
        return mgProductEsSearch.getSearchParam();
    }


    public Param getPageParam() {
        return new Param().setInt(PageInfo.START, start).setInt(PageInfo.LIMIT, limit);
    }

    public MgProductDbSearch getMgProductDbSearch() {
        return mgProductDbSearch;
    }


    public MgProductSearchArg setMgProductDbSearch(MgProductDbSearch mgProductDbSearch) {
        this.mgProductDbSearch = mgProductDbSearch;
        return this;
    }

    public MgProductEsSearch getMgProductEsSearch() {
        return mgProductEsSearch;
    }

    public MgProductSearchArg setMgProductEsSearch(MgProductEsSearch mgProductEsSearch) {
        this.mgProductEsSearch = mgProductEsSearch;
        return this;
    }

    public Integer getStart() {
        return start;
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getTotal() {
        return total;
    }

    public MgProductSearchArg setStart(Integer start) {
        this.start = start >= 0 ? start : this.start;
        return this;
    }


    public MgProductSearchArg setLimit(Integer limit) {
        this.limit = limit >= 0 ? limit : this.limit;
        return this;
    }

    public MgProductSearchArg setTotal(Integer total) {
        this.total = total;
        return this;
    }

    /**
     * es和db的查询条件都为空
     * @return true 表示es和db的查询条件都为空
     */
    public boolean isEmpty() {
        return (mgProductEsSearch == null && mgProductDbSearch == null)
            || (mgProductEsSearch != null && mgProductEsSearch.isEmpty() && mgProductDbSearch == null)
            // 要保留它设置的排序
            || (mgProductEsSearch != null && mgProductEsSearch.isEmpty() && mgProductDbSearch != null && mgProductDbSearch.isEmpty() && mgProductDbSearch.getParamComparator().isEmpty())
            || (mgProductEsSearch == null && mgProductDbSearch != null && mgProductDbSearch.isEmpty() && mgProductDbSearch.getParamComparator().isEmpty());
    }

    public static final class PageInfo {
        public static final String START = "start";
        public static final String LIMIT = "limit";
    }
}
