package fai.MgProductInfSvr.interfaces.utils;

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
     * 初始化MgProductSearchArg。
     * @param esSearchParam 包含es搜索条件，Param里的key，可以参考MgProductEsSearch的initSearchParam方法进行设置
     * @param dbSearchParam 包含db搜索条件，Param里的key，可以参考MgProductDbSearch的initSearchParam方法进行设置
     */
    public MgProductSearchArg initSearchParam(Param esSearchParam, Param dbSearchParam) {
        // 按需加载
        if (!Str.isEmpty(esSearchParam)) {
            mgProductEsSearch = new MgProductEsSearch();
            mgProductEsSearch.initSearchParam(esSearchParam);
        }
        if (!Str.isEmpty(dbSearchParam)) {
            mgProductDbSearch = new MgProductDbSearch();
            mgProductDbSearch.initSearchParam(dbSearchParam);
        }
        return this;
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

    /**
     * es和db的查询条件都为空
     * @return true 表示es和db的查询条件都为空
     */
    public boolean isEmpty() {
        return mgProductEsSearch == null && mgProductDbSearch == null;
    }
}
