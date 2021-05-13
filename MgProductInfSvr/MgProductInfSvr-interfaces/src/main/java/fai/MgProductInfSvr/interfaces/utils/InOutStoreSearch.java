package fai.MgProductInfSvr.interfaces.utils;

import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.comm.util.*;

import java.util.Calendar;

public class InOutStoreSearch {
    public static enum SearchType {
        InOutStoreRecord, // 搜索出入库记录详情
        InOutStoreSum // 搜索出入库记录汇总数据(按照 单号+主键id 汇总)
    }
    public int start = 0;
    public int limit = -1;
    public ParamComparator cmpor = null;
    public Ref<Integer> totalSize = null;
    private ParamMatcher matcher;

    private SearchArg searchArg;

    private SearchType searchType;
    private int aid;
    private int tid;
    private int siteId;
    private int lgId;
    private int keepPriId1;
    private FaiList<Param> unionPriIds; // tid+siteId+lgId+keepPriId1 的集合

    public int getAid() {
        return aid;
    }

    public int getTid() {
        return tid;
    }

    public int getSiteId() {
        return siteId;
    }

    public int getLgId() {
        return lgId;
    }

    public int getKeepPriId1() {
        return keepPriId1;
    }

    public FaiList<Param> getUnionPriIds() {
        return unionPriIds;
    }

    public SearchType getSearchType() {
        return searchType;
    }

    // 搜索单个指定业务维度 aid + tid+siteId+lgId+keepPriId
    public InOutStoreSearch(int aid, int tid, int siteId, int lgId, int keepPriId1, SearchType searchType) {
        this.aid = aid;
        this.tid = tid;
        this.siteId = siteId;
        this.lgId = lgId;
        this.keepPriId1 = keepPriId1;
        this.searchType = searchType;
        createMatcher();
    }

    // 搜索多个指定业务维度 aid + unionPriIds(tid+siteId+lgId+keepPriId1 的集合)
    public InOutStoreSearch(int aid, FaiList<Param> unionPriIds, SearchType searchType) {
        this.aid = aid;
        this.unionPriIds = unionPriIds;
        this.searchType = searchType;
        createMatcher();
    }

    // 操作时间
    public InOutStoreSearch setOptTime(Calendar minOptTime, Calendar maxOptTime) {
        String key = SearchType.InOutStoreSum.equals(searchType) ? ProductStoreEntity.InOutStoreSumInfo.OPT_TIME : ProductStoreEntity.InOutStoreRecordInfo.OPT_TIME;
        if(minOptTime != null) {
            matcher.and(key, ParamMatcher.GE, minOptTime);
        }
        if(maxOptTime != null) {
            matcher.and(key, ParamMatcher.LE, maxOptTime);
        }
        return this;
    }

    // 出入库类型
    public InOutStoreSearch setCtype(FaiList<Integer> cTypes) {
        String key = SearchType.InOutStoreSum.equals(searchType) ? ProductStoreEntity.InOutStoreSumInfo.C_TYPE : ProductStoreEntity.InOutStoreRecordInfo.C_TYPE;
        if(cTypes.size() == 1) {
            matcher.and(key, ParamMatcher.EQ, cTypes.get(0));
            return this;
        }
        matcher.and(key, ParamMatcher.IN, cTypes);
        return this;
    }

    // 单号
    public InOutStoreSearch setNumber(String number) {
        String key = SearchType.InOutStoreSum.equals(searchType) ? ProductStoreEntity.InOutStoreSumInfo.NUMBER : ProductStoreEntity.InOutStoreRecordInfo.NUMBER;
        matcher.and(key, ParamMatcher.EQ, number);
        return this;
    }

    // 出入库记录id，相当于一个int值的单号
    public InOutStoreSearch setIntNumber(int ioStoreRecId) {
        String key = SearchType.InOutStoreSum.equals(searchType) ? ProductStoreEntity.InOutStoreSumInfo.IN_OUT_STORE_REC_ID : ProductStoreEntity.InOutStoreRecordInfo.IN_OUT_STORE_REC_ID;
        matcher.and(key, ParamMatcher.EQ, ioStoreRecId);
        return this;
    }

    // 创建时间
    public InOutStoreSearch setCreatTime(Calendar minCreateTime, Calendar maxCreateTime) {
        String key = SearchType.InOutStoreSum.equals(searchType) ? ProductStoreEntity.InOutStoreSumInfo.SYS_CREATE_TIME : ProductStoreEntity.InOutStoreRecordInfo.SYS_CREATE_TIME;
        if(minCreateTime != null) {
            matcher.and(key, ParamMatcher.GE, minCreateTime);
        }
        if(maxCreateTime != null) {
            matcher.and(key, ParamMatcher.LE, maxCreateTime);
        }
        return this;
    }

    // 商品业务id
    public InOutStoreSearch setRlPdIds(FaiList<Integer> rlPdIds) {
        if(rlPdIds == null || rlPdIds.isEmpty()) {
            Log.logErr("rlPdIds is null;aid=%d;tid=%d;siteId=%d;lgId=%d;keepPriId1=%d;", aid, tid, siteId, lgId, keepPriId1);
            return null;
        }
        if(SearchType.InOutStoreSum.equals(searchType)) {
            Log.logErr("search arg error;searchType is InOutStoreSum;aid=%d;tid=%d;siteId=%d;lgId=%d;keepPriId1=%d;", aid, tid, siteId, lgId, keepPriId1);
            return null;
        }
        if(rlPdIds.size() == 1) {
            matcher.and(ProductStoreEntity.InOutStoreRecordInfo.RL_PD_ID, ParamMatcher.EQ, rlPdIds.get(0));
        }else {
            matcher.and(ProductStoreEntity.InOutStoreRecordInfo.RL_PD_ID, ParamMatcher.IN, rlPdIds);
        }
        return this;
    }

    public SearchArg getSearchArg() {
        if(this.searchArg == null) {
            this.searchArg = new SearchArg();
            this.searchArg.start = this.start;
            this.searchArg.limit = this.limit;
            this.searchArg.cmpor = this.cmpor;
            this.searchArg.totalSize = this.totalSize;
            this.searchArg.matcher = this.matcher;
        }
        return searchArg;
    }

    private void createMatcher() {
        switch (searchType) {
            case InOutStoreSum:
                createSumMatcher();
                break;
            default:
                createRecordMatcher();
                break;
        }
    }
    private void createRecordMatcher() {
        matcher = new ParamMatcher(ProductStoreEntity.InOutStoreRecordInfo.AID, ParamMatcher.EQ, aid);
    }
    private void createSumMatcher() {
        matcher = new ParamMatcher(ProductStoreEntity.InOutStoreSumInfo.AID, ParamMatcher.EQ, aid);
    }
}
