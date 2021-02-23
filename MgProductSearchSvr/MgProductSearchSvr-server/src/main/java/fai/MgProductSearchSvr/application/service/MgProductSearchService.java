package fai.MgProductSearchSvr.application.service;

import fai.MgProductSearchSvr.interfaces.entity.MgProductSearch;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class MgProductSearchService {

    public int searchList(FaiSession session, int flow, int aid, int unionPriId, int tid, int productCount, String searchParamString) throws IOException {
        int rt = Errno.ERROR;
        Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;searchParamString=%s;", aid, unionPriId, tid, productCount, flow, searchParamString);

        Param searchParam = Param.parseParam(searchParamString);
        if(Str.isEmpty(searchParam)){
            rt = Errno.ARGS_ERROR;
            return rt;
        }
        MgProductSearch mgProductSearch = new MgProductSearch();
        mgProductSearch.initProductSearch(searchParam);    // 初始化 ProductSearch
        Log.logDbg("searchParam=%s;", searchParam.toJson());


        // 需要搜索 商品业务关系表
        ParamMatcher productRelSearchMatcher = mgProductSearch.getProductRelSearchMatcher(null);
        if(!productRelSearchMatcher.isEmpty()){
            // 获取数据的数量
        }


        Log.logDbg("aid=%d;unionPriId=%d;tid=%d;productCount=%d;flow=%s;", aid, unionPriId, tid, productCount, flow);
        rt = Errno.OK;
        session.write(rt);
        return rt;
    }


    // 1、在 "商品基础表" mgProduct_xxxx 搜索
    // 2、在 "商品业务关系表" mgProductRel_xxxx 搜索
    // 3、在 "参数值表" mgProductPropVal_xxxx搜索
    // 4、在 "商品与参数值关联表" mgProductBindProp_xxxx 搜索
    // 5、在 "分类业务关系表" mgProductGroupRel_xxxx 搜索
    // 6、在 "标签业务关系表" mgProductLableRel_xxxx 搜索
    // 7、在 "商品业务销售总表" mgSpuBizSummary_xxxx 搜索

    //  ConcurrentHashMap<Integer, ParamCache1>  eg: <unionPriId, ParamCache1>
    private ConcurrentHashMap<Integer, ParamCache1> mgProductCache = new ConcurrentHashMap<Integer, ParamCache1>();

    // 数据的更新时间和总条数的缓存
    private ParamCache1 dataStatusCache = new ParamCache1();

    // 结果集的缓存，后面可以优化为codis缓存
    private ParamCache1 resultCache = new ParamCache1();


}
