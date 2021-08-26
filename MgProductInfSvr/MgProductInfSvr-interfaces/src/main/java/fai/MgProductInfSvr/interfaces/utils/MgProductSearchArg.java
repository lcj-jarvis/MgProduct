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

    private MgProductEsSearch mgProductEsSearch;
    private MgProductDbSearch mgProductDbSearch;

    public void initSearchParam(Param esSearchParam, Param dbSearchParam) {
        // 按需加载
        if (!Str.isEmpty(esSearchParam)) {
            mgProductEsSearch = new MgProductEsSearch();
            mgProductEsSearch.initSearchParam(esSearchParam);
        }
        if (!Str.isEmpty(dbSearchParam)) {
            mgProductDbSearch = new MgProductDbSearch();
            mgProductDbSearch.initSearchParam(dbSearchParam);
        }
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
}
