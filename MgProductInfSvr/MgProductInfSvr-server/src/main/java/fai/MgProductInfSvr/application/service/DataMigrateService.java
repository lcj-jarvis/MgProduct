package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.dto.MigrateDef;
import fai.MgProductInfSvr.application.MgProductInfSvr;
import fai.MgProductInfSvr.domain.entity.RichTextValObj;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.domain.serviceproc.RichTextProc;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextValObj;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据迁移用，迁移完成后废弃
 */
public class DataMigrateService extends MgProductInfService {
    private static DaoPool daoPool;
    public static void init(MgProductInfSvr.SvrOption svrOption) {
        int dbType = svrOption.getDebug() ? FaiDbUtil.Type.DEV_MASTER : FaiDbUtil.Type.PRO_MASTER;
        daoPool = getDaoPool("MgProductInfSvr", dbType, 10, "ykProduct");
    }

    private static DaoPool getDaoPool(String svrName, int type, int maxSize, String instance) {
        // connect database config
        Param dbInfo = FaiDbUtil.getDbInfo(type, instance);
        if (dbInfo == null || dbInfo.isEmpty()) {
            Log.logErr("get daoInfo err");
            return null;
        }
        String ip = dbInfo.getString(FaiDbUtil.DbInfo.IP);
        int port = dbInfo.getInt(FaiDbUtil.DbInfo.PORT);
        String user = dbInfo.getString(FaiDbUtil.DbInfo.USER);
        String pwd = dbInfo.getString(FaiDbUtil.DbInfo.PASSWORD);
        String db = dbInfo.getString(FaiDbUtil.DbInfo.DATABASE);
        DaoPool daoPool = new DaoPool(svrName, maxSize, ip, port, db, user, pwd);

        return daoPool;
    }

    private static String getYKPdTableName(int aid) {
        return "yk_product_" + String.format("%03d", aid%100);
    }

    @SuccessRt(Errno.OK)
    public int migrateYK(FaiSession session, int flow, int aid) throws IOException {
        int rt;

        FaiList<Param> ykPdList = null;
        FaiList<Param> ykPdBindGroupList = null;
        Dao ykDao = null;
        try {
            ykDao = daoPool.getDao();
            if (ykDao == null) {
                rt = Errno.DAO_CONN_ERROR;
                throw new MgException(rt, "conn db err;aid=%d", aid);
            }
            Dao.SelectArg sltArg = new Dao.SelectArg();
            sltArg.table = getYKPdTableName(aid);
            sltArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            sltArg.searchArg.cmpor = new ParamComparator("storeId", false);
            ykPdList = ykDao.select(sltArg);

            sltArg = new Dao.SelectArg();
            sltArg.table = "yk_item_ref_category";
            sltArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            ykPdBindGroupList = ykDao.select(sltArg);
        }finally {
            if(ykDao != null) {
                ykDao.close();
            }
        }
        if(ykPdList == null || ykPdBindGroupList == null) {
            rt = Errno.DAO_ERROR;
            throw new MgException(rt, "select from yk err;aid=%s;", aid);
        }
        if(ykPdList.isEmpty()) {
            Log.logStd("migrate ok;aid=%d;", aid);
            return Errno.OK;
        }

        Map<String, FaiList<Integer>> bindGroupMap = new HashMap<>();
        for(Param bindGroup : ykPdBindGroupList) {
            int yid = bindGroup.getInt("yid");
            int rlPdId = bindGroup.getInt("itemId");
            FaiList<Integer> bindGroupIds = bindGroupMap.get(yid + "-" + rlPdId);
            if(bindGroupIds == null) {
                bindGroupIds = new FaiList<>();
                bindGroupMap.put(yid + "-" + rlPdId, bindGroupIds);
            }
            int rlGroupId = bindGroup.getInt("categoryId");
            bindGroupIds.add(rlGroupId);
        }

        int tid = FaiValObj.TermId.YK;
        int lgId = 0;

        Map<String, Param>  unionPriIdRlPdId_info = new HashMap<>();
        FaiList<Param> spuList = new FaiList<>();
        Map<Integer, FaiList<Param>> remarkMap = new HashMap<>();
        for(Param ykPd : ykPdList) {
            int siteId = ykPd.getInt("yid");
            int keepPriId1 = ykPd.getInt("storeId");
            int rlPdId = ykPd.getInt("productId");
            String name = ykPd.getString("name");
            String unitType = ykPd.getString("unitType");
            String remark = ykPd.getString("description");
            int ykFlag = ykPd.getInt("flag");
            int ykStatus = ykPd.getInt("status");
            int optSid = ykPd.getInt("optSid");
            String imgs = ykPd.getString("imgs");
            long createTime = ykPd.getLong("createTime");
            long updateTime = ykPd.getLong("updateTime");
            Calendar sysCreateTime = Calendar.getInstance();
            sysCreateTime.setTimeInMillis(createTime);
            Calendar sysUpdateTime = Calendar.getInstance();
            sysUpdateTime.setTimeInMillis(updateTime);
            int sort = ykPd.getInt("sort");
            String distributeTypes = ykPd.getString("distributeTypes");
            int priceType = ykPd.getInt("priceType");

            int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
            int ownUnionPriId = unionPriId;
            if(keepPriId1 != 0) {
                ownUnionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, 0);
            }

            // 富文本数据, 只处理总部的数据
            //if(keepPriId1 == 0) {
                FaiList<Param> remarkList = remarkMap.get(unionPriId);
                if(remarkList == null) {
                    remarkList = new FaiList<>();
                    remarkMap.put(unionPriId, remarkList);
                }
                Param remarkInfo = new Param();
                // 先用rlPdId占位，后面商品数据添加完后，要换成pdId
                remarkInfo.setInt(MgRichTextEntity.Info.RLID, rlPdId); //业务功能Id（必填）
                remarkInfo.setInt(MgRichTextEntity.Info.BIZ, MgRichTextValObj.Biz.PRODUCT);
                remarkInfo.setInt(MgRichTextEntity.Info.TYPE, RichTextValObj.Type.REMARK); //内容类型（必填）
                remarkInfo.setString(MgRichTextEntity.Info.CONTENT, remark); // 富文本内容（选填）
                remarkList.add(remarkInfo);
            //}

            // spu数据
            Param spuInfo = new Param();
            spuInfo.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            spuInfo.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);
            spuInfo.setString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, distributeTypes);
            spuInfo.setInt(SpuBizSummaryEntity.Info.PRICE_TYPE, priceType);
            spuList.add(spuInfo);

            // 拆开原本flag，0x1和0x2是各门店独自维护的，其余是共享的
            int rlFlag = 0;
            if((ykFlag & 0x1) == 0x1) {
                rlFlag |= 0x1;
                ykFlag &= ~0x1;
            }
            if((ykFlag & 0x2) == 0x2) {
                rlFlag |= 0x2;
                ykFlag &= ~0x2;
            }

            // 组装商品基础数据, 只处理总部的数据
            if(keepPriId1 == 0) {
                Param basicInfo = new Param();
                basicInfo.setInt(ProductBasicEntity.ProductInfo.AID, aid);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.RL_PD_ID, rlPdId);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.SOURCE_TID, tid);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.SOURCE_UNIONPRIID, unionPriId);
                basicInfo.setString(ProductBasicEntity.ProductInfo.NAME, name);
                basicInfo.setString(ProductBasicEntity.ProductInfo.IMG_LIST, imgs);
                basicInfo.setString(ProductBasicEntity.ProductInfo.UNIT, unitType);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.FLAG, ykFlag);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.STATUS, ykStatus);
                basicInfo.setCalendar(ProductBasicEntity.ProductInfo.CREATE_TIME, sysCreateTime);
                basicInfo.setCalendar(ProductBasicEntity.ProductInfo.UPDATE_TIME, sysUpdateTime);

                FaiList<Integer> bindGroupIds = bindGroupMap.get(siteId + "-" + rlPdId);
                Param info = new Param();
                info.setParam(MigrateDef.Info.ADD_PD, basicInfo);
                if(bindGroupIds != null) {
                    info.setList(MigrateDef.Info.BIND_RL_GROUP, bindGroupIds);
                }
                unionPriIdRlPdId_info.put(ownUnionPriId+"-"+rlPdId, info);
            }

            Param relInfo = new Param();
            relInfo.setInt(ProductBasicEntity.ProductInfo.AID, aid);
            relInfo.setInt(ProductBasicEntity.ProductInfo.RL_PD_ID, rlPdId);
            relInfo.setInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID, unionPriId);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SOURCE_TID, tid);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.ADD_TIME, sysCreateTime);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.LAST_UPDATE_TIME, sysUpdateTime);
            relInfo.setInt(ProductBasicEntity.ProductInfo.STATUS, ykStatus);
            relInfo.setInt(ProductBasicEntity.ProductInfo.RL_FLAG, rlFlag);
            relInfo.setInt(ProductBasicEntity.ProductInfo.LAST_SID, optSid);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SORT, sort);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.CREATE_TIME, sysCreateTime);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.UPDATE_TIME, sysUpdateTime);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SYS_TYPE, 0);


            Param info = unionPriIdRlPdId_info.get(ownUnionPriId+"-"+rlPdId);
            FaiList<Param> bindList = info.getList(MigrateDef.Info.BIND_PD_REL);
            if(bindList == null) {
                bindList = new FaiList<>();
                info.setList(MigrateDef.Info.BIND_PD_REL, bindList);
            }
            bindList.add(relInfo);
        }

        FaiList<Param> pdList = new FaiList<>();
        for(String key : unionPriIdRlPdId_info.keySet()) {
            Param info = unionPriIdRlPdId_info.get(key);
            pdList.add(info);
        }

        ProductBasicProc basicProc = new ProductBasicProc(flow);
        FaiList<Param> returnList = basicProc.dataMigrate(aid, tid, pdList);
        Map<String, Integer> unionPriIdRlPdId_pdId = new HashMap<>();

        for(Param returnInfo : returnList) {
            int unionPriId = returnInfo.getInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID);
            int rlPdId = returnInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID);
            int pdId = returnInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);

            unionPriIdRlPdId_pdId.put(unionPriId + "-" + rlPdId, pdId);
        }
        // 同步spu数据
        if(!spuList.isEmpty()) {
            // 替换rlPdId -> pdId
            for(Param info : spuList) {
                int unionPriId = info.getInt(SpuBizSummaryEntity.Info.UNION_PRI_ID);
                int rlPdId = info.getInt(SpuBizSummaryEntity.Info.RL_PD_ID);
                Integer pdId = unionPriIdRlPdId_pdId.get(unionPriId + "-" + rlPdId);
                if(pdId == null) {
                    rt = Errno.ERROR;
                    Log.logErr(rt, "spu get pdId err;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
                    return rt;
                }
                info.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
            }
            ProductStoreProc storeProc = new ProductStoreProc(flow);
            rt = storeProc.migrate(aid, spuList);
            if(rt != Errno.OK) {
                return rt;
            }
        }

        // 同步富文本数据
        if(!remarkMap.isEmpty()) {
            // 替换rlPdId -> pdId
            for(int unionPriId : remarkMap.keySet()) {
                FaiList<Param> remarkList = remarkMap.get(unionPriId);
                for(Param remark : remarkList) {
                    int rlPdId = remark.getInt(MgRichTextEntity.Info.RLID);
                    Integer pdId = unionPriIdRlPdId_pdId.get(unionPriId+ "-" + rlPdId);
                    if(pdId == null) {
                        rt = Errno.ERROR;
                        Log.logErr(rt, "remark get pdId err;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
                        return rt;
                    }
                    remark.setInt(MgRichTextEntity.Info.RLID, pdId);
                }
            }

            RichTextProc richProc = new RichTextProc(flow);
            for(int unionPriId : remarkMap.keySet()) {
                Param primary = getByUnionPriId(flow, aid, unionPriId);
                int siteId = primary.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
                int keepPriId1 = primary.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
                richProc.migrateDel(aid, tid, siteId, lgId, keepPriId1);
                richProc.batchAdd("", aid, tid, siteId, lgId, keepPriId1, remarkMap.get(unionPriId));
            }
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logDbg("migrate ok;flow=%d;aid=%d;", flow, aid);

        rt = Errno.OK;
        return rt;
    }
}

