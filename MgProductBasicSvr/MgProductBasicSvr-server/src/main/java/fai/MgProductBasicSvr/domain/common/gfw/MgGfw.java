package fai.MgProductBasicSvr.domain.common.gfw;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.comm.util.*;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public abstract class MgGfw {
    protected int type;
    protected static ThreadLocal<HashSet<Param>> preLogList = new ThreadLocal<>();
    protected static ThreadLocal<Map<String, Integer>> pdId_rlPdId = new ThreadLocal<>();

    public static void clear() {
        preLogList.remove();
        pdId_rlPdId.remove();
    }

    protected static HashSet<Param> getPreList() {
        HashSet<Param> list = preLogList.get();
        if(list == null) {
            list = new HashSet<>();
            preLogList.set(list);
        }
        return list;
    }

    protected static Map<String, Integer> getCacheRelMap() {
        Map<String, Integer> relMap = pdId_rlPdId.get();
        if(relMap == null) {
            relMap = new HashMap<>();
            pdId_rlPdId.set(relMap);
        }
        return relMap;
    }

    public void commitPre(ProductRelProc relProc) {
        if(preLogList.get() == null) {
            return;
        }
        FaiList<Param> list = new FaiList<>(preLogList.get());
        // 之前统一设置的都是pdId(因为操作基础表的时候，可能还没有rlPdId)，提交时统一替换为rlPdId
        FaiList<Param> writeList = new FaiList<>();
        for(Param info : list) {
            Integer rlPdId = getRlPdId(relProc, info);
            if(rlPdId == null) {
                continue;
            }
            info.setInt(Gfw.Info.ID, rlPdId);
            writeList.add(info);
        }
        Gfw.writeGfwLog(this.type, list);
        preLogList.remove();
        pdId_rlPdId.remove();
    }

    private Integer getRlPdId(ProductRelProc relProc, Param info) {
        int aid = info.getInt(Gfw.Info.AID);
        int unionPriId = (int) info.remove(ProductRelEntity.Info.UNION_PRI_ID);
        int pdId = (int) info.remove(Gfw.Info.ID);
        Map<String, Integer> relMap = getCacheRelMap();
        Integer rlPdId = relMap.get(aid + "-" + unionPriId + "-" + pdId);
        if(rlPdId != null) {
            return rlPdId;
        }
        Param relInfo = relProc.getProductRel(aid, unionPriId, pdId);
        rlPdId = relInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
        relMap.put(aid + "-" + unionPriId + "-" + pdId, rlPdId);
        return rlPdId;
    }

    public void preWriteGfwLog(int aid, int siteId, Param info) {
        FaiList<Param> gfwDatas = buildLogData(aid, siteId, info);
        if(Utils.isEmptyList(gfwDatas)) {
            return;
        }
        HashSet<Param> preList = getPreList();
        preList.addAll(gfwDatas);
    }

    public void preWriteGfwLog(int aid, int siteId, FaiList<Param> list) {
        for(Param info : list) {
            preWriteGfwLog(aid, siteId, info);
        }
    }

    // 子类可重写该方法实现记录不同字段
    protected abstract FaiList<Param> buildLogData(int aid, int siteId, Param info);
}
