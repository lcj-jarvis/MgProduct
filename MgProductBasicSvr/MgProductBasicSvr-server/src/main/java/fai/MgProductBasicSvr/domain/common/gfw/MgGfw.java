package fai.MgProductBasicSvr.domain.common.gfw;

import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.comm.util.*;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashSet;

public abstract class MgGfw {
    protected int type;
    protected static ThreadLocal<HashSet<Param>> preLogList = new ThreadLocal<>();

    public static void clear() {
        preLogList.remove();
    }

    protected static HashSet<Param> getPreList() {
        HashSet<Param> list = preLogList.get();
        if(list == null) {
            list = new HashSet<>();
            preLogList.set(list);
        }
        return list;
    }

    public void commitPre() {
        if(preLogList.get() == null) {
            return;
        }
        FaiList<Param> list = new FaiList<>(preLogList.get());
        Gfw.writeGfwLog(this.type, list);
        preLogList.remove();
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
    protected FaiList<Param> buildLogData(int aid, int siteId, Param info) {
        if(Str.isEmpty(info) || siteId < 0) {
            return null;
        }
        String name = info.getString(ProductEntity.Info.NAME);
        Integer pdId = info.getInt(ProductEntity.Info.PD_ID);
        if(name == null || pdId == null) {
            Log.logErr("build gfw data error;aid=%s;siteId=%s;info=%s;", aid, siteId, info);
            return null;
        }

        Param gfwData = new Param();
        gfwData.setInt(Gfw.Info.AID, aid);
        gfwData.setInt(Gfw.Info.ID, pdId);
        gfwData.setInt(Gfw.Info.FOLDER_ID, siteId);
        gfwData.setString(Gfw.Info.KEY, ProductEntity.Info.NAME);
        gfwData.setString(Gfw.Info.VALUE, name);

        return Utils.asFaiList(gfwData);
    }
}
