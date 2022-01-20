package fai.MgProductStoreSvr.domain.repository.dao;

import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.domain.entity.SpuBizSummaryValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.DaoCtrl;

/**
 * 库存销售原先不支持软删除数据
 * 现在要做支持，为保持原来的各逻辑不变，相关表不查软删数据出来
 * 所有查询接口加上 status != -1条件
 * 如果以后需要查软删数据，再另外加接口
 */
public abstract class DaoCtrlWithoutDel extends DaoCtrl {
    public DaoCtrlWithoutDel(int flow, int aid) {
        super(flow, aid);
    }

    @Override
    public int selectFirst(SearchArg searchArg, Ref<Param> ref) {
        if(searchArg != null && searchArg.matcher != null) {
            searchArg.matcher.and(SpuBizSummaryEntity.Info.STATUS, ParamMatcher.NE, SpuBizSummaryValObj.Status.DEL);
        }
        return super.selectFirst(searchArg, ref, null);
    }
    @Override
    public int selectFirst(SearchArg searchArg, Ref<Param> ref, String ... onlyNeedFields) {
        if(searchArg != null && searchArg.matcher != null) {
            searchArg.matcher.and(SpuBizSummaryEntity.Info.STATUS, ParamMatcher.NE, SpuBizSummaryValObj.Status.DEL);
        }
        return super.selectFirst(searchArg, ref, onlyNeedFields);
    }

    @Override
    public int select(SearchArg searchArg, Ref<FaiList<Param>> listRef) {
        if(searchArg != null && searchArg.matcher != null) {
            searchArg.matcher.and(SpuBizSummaryEntity.Info.STATUS, ParamMatcher.NE, SpuBizSummaryValObj.Status.DEL);
        }
        return super.select(searchArg, listRef);
    }

    @Override
    public int select(SearchArg searchArg, Ref<FaiList<Param>> listRef, String ... onlyNeedFields){
        if(searchArg != null && searchArg.matcher != null) {
            searchArg.matcher.and(SpuBizSummaryEntity.Info.STATUS, ParamMatcher.NE, SpuBizSummaryValObj.Status.DEL);
        }
        return super.select(searchArg, listRef, onlyNeedFields);
    }

    @Override
    public int select(SearchArg searchArg, Ref<FaiList<Param>> listRef, FaiList<String> onlyNeedFields){
        if(searchArg != null && searchArg.matcher != null) {
            searchArg.matcher.and(SpuBizSummaryEntity.Info.STATUS, ParamMatcher.NE, SpuBizSummaryValObj.Status.DEL);
        }
        return super.select(searchArg, listRef, onlyNeedFields);
    }

    @Override
    public int select(Dao.SelectArg sltArg, Ref<FaiList<Param>> listRef){
        if(sltArg != null && sltArg.searchArg != null && sltArg.searchArg.matcher != null) {
            sltArg.searchArg.matcher.and(SpuBizSummaryEntity.Info.STATUS, ParamMatcher.NE, SpuBizSummaryValObj.Status.DEL);
        }
        return super.select(sltArg, listRef);
    }
}
