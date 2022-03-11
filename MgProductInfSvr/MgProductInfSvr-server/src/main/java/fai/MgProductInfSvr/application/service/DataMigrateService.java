package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.dto.MigrateDef;
import fai.MgProductInfSvr.application.MgProductInfSvr;
import fai.MgProductInfSvr.domain.entity.RichTextValObj;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductSpecProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.domain.serviceproc.RichTextProc;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecValObj;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuValObj;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextValObj;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;

import java.io.IOException;
import java.util.*;

/**
 * 数据迁移用，迁移完成后废弃
 */
public class DataMigrateService extends MgProductInfService {
    private static DaoPool daoPool;
    private static DaoPool ykStoreDaoPool;
    private static DaoPool ykServiceDaoPool;
    public static void init(MgProductInfSvr.SvrOption svrOption) {
        int dbType = svrOption.getDebug() ? FaiDbUtil.Type.DEV_MASTER : FaiDbUtil.Type.PRO_MASTER;
        daoPool = getDaoPool("MgProductInfSvr", dbType, 10, "ykProduct");
        ykStoreDaoPool = getDaoPool("MgProductInfSvr", dbType, 10, "ykStore");
        ykServiceDaoPool = getDaoPool("MgProductInfSvr", dbType, 10, "ykService");
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

    private static String getYKStoreTableName(int aid) {
        return "yk_store_" + String.format("%04d", aid%1000);
    }

    private String getYKServiceTable(int aid) {
        return "ykService_" + String.format("%04d", aid % 1000);
    }

    private String getYKServiceSubTable(int aid) {
        return "ykServiceSub_" + String.format("%04d", aid % 1000);
    }

    @SuccessRt(Errno.OK)
    public int migrateYK(FaiSession session, int flow, int aid) throws IOException {
        int rt;

        FaiList<Param> ykPdList = null;
        FaiList<Param> ykPdBindGroupList = null;
        Dao ykDao = null;
        Dao ykStoreDao = null;

        Calendar defaultCal = Calendar.getInstance();
        defaultCal.setTimeInMillis(0L);

        Map<String, Param> storeInfoMap = new HashMap<>();
        try {
            ykDao = daoPool.getDao();
            if (ykDao == null) {
                rt = Errno.DAO_CONN_ERROR;
                throw new MgException(rt, "conn db err;aid=%d", aid);
            }
            ykStoreDao = ykStoreDaoPool.getDao();
            if (ykStoreDao == null) {
                rt = Errno.DAO_CONN_ERROR;
                throw new MgException(rt, "ykStoreDao conn db err;aid=%d", aid);
            }
            Dao.SelectArg sltArg = new Dao.SelectArg();
            sltArg = new Dao.SelectArg();
            sltArg.table = getYKStoreTableName(aid);
            sltArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            //sltArg.searchArg.matcher.and("flag2", ParamMatcher.LAND, 0x400, 0x400);
            FaiList<Param> stores = ykStoreDao.select(sltArg);
            if(stores == null) {
                rt = Errno.DAO_ERROR;
                throw new MgException(rt, "select storeIds err;aid=%s;", aid);
            }

            for(Param info : stores) {
                int storeId = info.getInt("storeId");
                int yid = info.getInt("yid");
                storeInfoMap.put(yid + "-" + storeId, info);
            }

            sltArg.table = getYKPdTableName(aid);
            sltArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            sltArg.searchArg.cmpor = new ParamComparator("storeId", false); // 按storeId升序
            ykPdList = ykDao.select(sltArg);

            sltArg = new Dao.SelectArg();
            sltArg.table = "yk_item_ref_category";
            sltArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            ykPdBindGroupList = ykDao.select(sltArg);
        }finally {
            if(ykStoreDao != null) {
                ykStoreDao.close();
            }
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

        // 存在上架门店的商品
        HashSet<String> onSalePd = new HashSet<>();

        // 总部已存在的商品id
        Map<Integer, FaiList<Integer>> yid_rlPdIds = new HashMap<>();
        // 各门店已存在的商品id
        Map<String, FaiList<Integer>> yidStoreId_rlPdIds = new HashMap<>();
        Iterator<Param> it = ykPdList.iterator();
        while (it.hasNext()) {
            Param ykPd = it.next();
            int yid = ykPd.getInt("yid");
            int storeId = ykPd.getInt("storeId");
            if(storeId != 0) {
                Param storeInfo = storeInfoMap.get(yid + "-" + storeId);
                int flag2 = storeInfo.getInt("flag2");
                // 不同步已删除门店的商品数据
                if(Misc.checkBit(flag2, 0x400)) {
                    it.remove();
                    continue;
                }
            }

            int rlPdId = ykPd.getInt("productId");
            // 总部
            if(storeId == 0) {
                FaiList<Integer> rlPdIds = yid_rlPdIds.get(yid);
                if(rlPdIds == null) {
                    rlPdIds = new FaiList<>();
                    yid_rlPdIds.put(yid, rlPdIds);
                }
                rlPdIds.add(rlPdId);
            }else {
                FaiList<Integer> rlPdIds = yidStoreId_rlPdIds.get(yid + "-" + storeId);
                if(rlPdIds == null) {
                    rlPdIds = new FaiList<>();
                    yidStoreId_rlPdIds.put(yid + "-" + storeId, rlPdIds);
                }
                rlPdIds.add(rlPdId);
                int status = ykPd.getInt("status");
                if(status == 1) {
                    onSalePd.add(yid + "-" + rlPdId);
                }
            }

        }

        // 有的门店没有商品数据(新门店在创建之前就存在的商品，可能不会出现在新门店中，需要同步这些缺失数据)
        Map<String, FaiList<Integer>> notExistPd = new HashMap<>();

        Set<Map.Entry<String, Param>> storeEntry = storeInfoMap.entrySet();
        for(Map.Entry<String, Param> entry : storeEntry) {
            Param storeInfo = entry.getValue();
            int flag2 = storeInfo.getInt("flag2");
            // 不同步已删除门店数据
            if(Misc.checkBit(flag2, 0x400)) {
                continue;
            }
            int storeId = storeInfo.getInt("storeId");
            if(storeId == 0) {
                continue;
            }
            int yid = storeInfo.getInt("yid");
            FaiList<Integer> headRlPdId = yid_rlPdIds.get(yid);
            if(headRlPdId == null || headRlPdId.isEmpty()) {
                continue;
            }
            FaiList<Integer> needSyncRlPdId = headRlPdId.clone();
            FaiList<Integer> storeRlPdId = yidStoreId_rlPdIds.get(yid+ "-" + storeId);
            if(storeRlPdId != null) {
                // 总部商品id数据 去掉门店已存在商品id 数据，剩下的就是缺失数据，需要同步
                needSyncRlPdId.removeAll(storeRlPdId);
            }
            for(int rlPdId : needSyncRlPdId) {
                FaiList<Integer> storeIds = notExistPd.get(yid + "-" + rlPdId);
                if(storeIds == null) {
                    storeIds = new FaiList<>();
                    notExistPd.put(yid+ "-" + rlPdId, storeIds);
                }
                storeIds.add(storeId);
            }
        }

        Map<String, Set<Integer>> bindGroupMap = new HashMap<>();
        for (Param bindGroup : ykPdBindGroupList) {
            int yid = bindGroup.getInt("yid");
            int rlPdId = bindGroup.getInt("itemId");
            Set<Integer> bindGroupIds = bindGroupMap.computeIfAbsent(yid + "-" + rlPdId, k -> new HashSet<>());
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
            if(keepPriId1 == 0) {
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
            }

            // spu数据, 中台软删数据没有spu数据，目前逻辑先不同步
            if(ykStatus != -1) {
                Param spuInfo = new Param();
                spuInfo.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
                spuInfo.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);
                spuInfo.setString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, distributeTypes);
                spuInfo.setInt(SpuBizSummaryEntity.Info.PRICE_TYPE, priceType);
                spuList.add(spuInfo);
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

                Set<Integer> bindGroupIds = bindGroupMap.get(siteId + "-" + rlPdId);
                Param info = new Param();
                info.setParam(MigrateDef.Info.ADD_PD, basicInfo);
                if(bindGroupIds != null) {
                    info.setList(MigrateDef.Info.BIND_RL_GROUP, new FaiList<>(bindGroupIds));
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
            relInfo.setInt(ProductBasicEntity.ProductInfo.RL_FLAG, ykFlag);
            relInfo.setInt(ProductBasicEntity.ProductInfo.LAST_SID, optSid);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SORT, sort);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.CREATE_TIME, sysCreateTime);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.UPDATE_TIME, sysUpdateTime);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SYS_TYPE, 0);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.TOP, defaultCal);
            if(Misc.checkBit(ykFlag, 0x2)) {
                relInfo.setCalendar(ProductBasicEntity.ProductInfo.TOP, Calendar.getInstance());
            }

            Param info = unionPriIdRlPdId_info.get(ownUnionPriId+"-"+rlPdId);
            FaiList<Param> bindList = info.getList(MigrateDef.Info.BIND_PD_REL);
            if(bindList == null) {
                bindList = new FaiList<>();
                info.setList(MigrateDef.Info.BIND_PD_REL, bindList);
            }
            bindList.add(relInfo);

            if(keepPriId1 == 0) {
                if(onSalePd.contains(siteId + "-" + rlPdId)) {
                    relInfo.setInt(ProductBasicEntity.ProductInfo.STATUS, 1);
                }else if(ykStatus != -1) {
                    relInfo.setInt(ProductBasicEntity.ProductInfo.STATUS, 0);
                }
                FaiList<Integer> storeIds = notExistPd.get(siteId + "-" + rlPdId);
                if(storeIds != null) {
                    for(int storeId : storeIds) {
                        Param newRelInfo = relInfo.clone();
                        int curUnionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, storeId);
                        newRelInfo.setInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID, curUnionPriId);
                        newRelInfo.setInt(ProductBasicEntity.ProductInfo.STATUS, -1);
                        bindList.add(newRelInfo);
                        // spu数据
                        if(ykStatus != -1) {
                            Param spuInfo = new Param();
                            spuInfo.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, curUnionPriId);
                            spuInfo.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);
                            spuInfo.setString(SpuBizSummaryEntity.Info.DISTRIBUTE_LIST, distributeTypes);
                            spuInfo.setInt(SpuBizSummaryEntity.Info.PRICE_TYPE, priceType);
                            spuList.add(spuInfo);
                        }
                    }
                }
            }
        }

        FaiList<Param> pdList = new FaiList<>();
        for(String key : unionPriIdRlPdId_info.keySet()) {
            Param info = unionPriIdRlPdId_info.get(key);
            pdList.add(info);
        }

        ProductBasicProc basicProc = new ProductBasicProc(flow);

        // 先找出是否是重复迁移数据的 pdId, 如果存在则清除基础信息中的数据
        FaiList<Integer> migratePdIds = basicProc.getMigratePdIds(aid, 0, false);
        Log.logDbg("needDelPdId=%s", migratePdIds);

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
                richProc.batchDel("", aid, tid, siteId, lgId, keepPriId1, migratePdIds);
                richProc.batchAdd("", aid, tid, siteId, lgId, keepPriId1, remarkMap.get(unionPriId));
            }
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logDbg("migrate ok;flow=%d;aid=%d;", flow, aid);

        rt = Errno.OK;
        return rt;
    }

    @SuccessRt(Errno.OK)
    public int migrateYKService(FaiSession session, int flow, int aid) throws IOException {
        int rt;

        FaiList<Param> ykServiceList;
        FaiList<Param> ykPdBindGroupList;
        FaiList<Param> ykServiceSubList;

        Calendar defaultCal = Calendar.getInstance();
        defaultCal.setTimeInMillis(0L);

        Dao ykDao = null;
        Dao ykStoreDao = null;

        Map<String, Param> storeInfoMap = new HashMap<>();
        try {
            ykDao = ykServiceDaoPool.getDao();
            if (ykDao == null) {
                rt = Errno.DAO_CONN_ERROR;
                throw new MgException(rt, "conn db err;aid=%d", aid);
            }
            ykStoreDao = ykStoreDaoPool.getDao();
            if (ykStoreDao == null) {
                rt = Errno.DAO_CONN_ERROR;
                throw new MgException(rt, "ykStoreDao conn db err;aid=%d", aid);
            }

            // 查询 所有门店
            Dao.SelectArg sltArg = new Dao.SelectArg();
            sltArg = new Dao.SelectArg();
            sltArg.table = getYKStoreTableName(aid);
            sltArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            // sltArg.searchArg.matcher.and("flag2", ParamMatcher.LAND, 0x400, 0x400);
            FaiList<Param> stores = ykStoreDao.select(sltArg);
            if(stores == null) {
                rt = Errno.DAO_ERROR;
                throw new MgException(rt, "select storeIds err;aid=%s;", aid);
            }

            for(Param info : stores) {
                int storeId = info.getInt("storeId");
                int yid = info.getInt("yid");
                storeInfoMap.put(yid + "-" + storeId, info);
            }

            // 查询 YK 服务信息
            Dao.SelectArg selectArg = new Dao.SelectArg();
            selectArg.table = getYKServiceTable(aid);
            selectArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            // 总店的数据放在前面
            selectArg.searchArg.cmpor = new ParamComparator("storeId", false);
            ykServiceList = ykDao.select(selectArg);

            // 查询 YK 服务分类绑定信息
            selectArg = new Dao.SelectArg();
            selectArg.table = "yk_service_category";
            selectArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            ykPdBindGroupList = ykDao.select(selectArg);

            // 查询 ykServiceSub 信息 (总店)
            selectArg = new Dao.SelectArg();
            selectArg.table = getYKServiceSubTable(aid);
            selectArg.searchArg.matcher = new ParamMatcher("aid", ParamMatcher.EQ, aid);
            selectArg.searchArg.matcher.and("storeId", ParamMatcher.EQ, 0);
            ykServiceSubList = ykDao.select(selectArg);
        } finally {
            if (ykDao != null) {
                ykDao.close();
            }
            if (ykStoreDao != null) {
                ykStoreDao.close();
            }
        }

        if(ykServiceList == null || ykPdBindGroupList == null) {
            rt = Errno.DAO_ERROR;
            throw new MgException(rt, "select from yk err;aid=%s;", aid);
        }

        if(ykServiceList.isEmpty()) {
            Log.logStd("migrate ok;aid=%d;", aid);
            return Errno.OK;
        }

        // 总部已存在的商品id
        Map<Integer, FaiList<Integer>> yid_rlPdIds = new HashMap<>();
        // 各门店已存在的商品id
        Map<String, FaiList<Integer>> yidStoreId_rlPdIds = new HashMap<>();
        Iterator<Param> it = ykServiceList.iterator();
        while (it.hasNext()) {
            Param ykPd = it.next();
            int yid = ykPd.getInt("yid");
            int storeId = ykPd.getInt("storeId");
            int rlPdId = ykPd.getInt("serviceId");
            if(storeId != 0) {
                Param storeInfo = storeInfoMap.get(yid + "-" + storeId);
                if (storeInfo == null) {
                    // 2022/1/26 线上存在 storeInfo 为 null 的情况，和门店确认后不迁移这些数据
                    it.remove();
                    continue;
                }
                int flag2 = storeInfo.getInt("flag2");
                // 不同步已删除门店的商品数据
                if(Misc.checkBit(flag2, 0x400)) {
                    it.remove();
                    continue;
                }
            }

            // 总部
            if(storeId == 0) {
                FaiList<Integer> rlPdIds = yid_rlPdIds.get(yid);
                if(rlPdIds == null) {
                    rlPdIds = new FaiList<>();
                    yid_rlPdIds.put(yid, rlPdIds);
                }
                rlPdIds.add(rlPdId);
            }else {
                FaiList<Integer> rlPdIds = yidStoreId_rlPdIds.get(yid + "-" + storeId);
                if(rlPdIds == null) {
                    rlPdIds = new FaiList<>();
                    yidStoreId_rlPdIds.put(yid + "-" + storeId, rlPdIds);
                }
                rlPdIds.add(rlPdId);
            }

        }

        // 有的门店没有商品数据(新门店在创建之前就存在的商品，可能不会出现在新门店中，需要同步这些缺失数据)
        Map<String, FaiList<Integer>> notExistPd = new HashMap<>();

        Set<Map.Entry<String, Param>> storeEntry = storeInfoMap.entrySet();
        for(Map.Entry<String, Param> entry : storeEntry) {
            Param storeInfo = entry.getValue();
            int flag2 = storeInfo.getInt("flag2");
            // 不同步已删除门店数据
            if(Misc.checkBit(flag2, 0x400)) {
                continue;
            }
            int storeId = storeInfo.getInt("storeId");
            if(storeId == 0) {
                continue;
            }
            int yid = storeInfo.getInt("yid");
            FaiList<Integer> headRlPdId = yid_rlPdIds.get(yid);
            if(headRlPdId == null || headRlPdId.isEmpty()) {
                continue;
            }
            FaiList<Integer> needSyncRlPdId = headRlPdId.clone();
            FaiList<Integer> storeRlPdId = yidStoreId_rlPdIds.get(yid+ "-" + storeId);
            if(storeRlPdId != null) {
                // 总部商品id数据 去掉门店已存在商品id 数据，剩下的就是缺失数据
                needSyncRlPdId.removeAll(storeRlPdId);
            }
            for(int rlPdId : needSyncRlPdId) {
                FaiList<Integer> storeIds = notExistPd.get(yid + "-" + rlPdId);
                if(storeIds == null) {
                    storeIds = new FaiList<>();
                    notExistPd.put(yid+ "-" + rlPdId, storeIds);
                }
                storeIds.add(storeId);
            }
        }

        Map<String, Set<Integer>> bindGroupMap = new HashMap<>();
        for (Param bindGroup : ykPdBindGroupList) {
            int yid = bindGroup.getInt("yid");
            int rlPdId = bindGroup.getInt("serviceId");
            Set<Integer> bindGroupIds = bindGroupMap.computeIfAbsent(yid + "-" + rlPdId, k -> new HashSet<>());
            int rlGroupId = bindGroup.getInt("categoryId");
            bindGroupIds.add(rlGroupId);
        }

        Map<String, Param> ykServiceSubMap = new HashMap<>();
        for (Param ykServiceSub : ykServiceSubList) {
            int storeId = ykServiceSub.getInt("storeId");
            int yid = ykServiceSub.getInt("yid");
            int serviceId = ykServiceSub.getInt("serviceId");
            ykServiceSubMap.put(yid + "-" + serviceId + "-" + storeId, ykServiceSub);
        }

        int tid = FaiValObj.TermId.YK;
        int lgId = 0;
        int sysType = 1;

        Map<String, Param>  unionPriIdRlPdId_info = new HashMap<>();
        FaiList<Param> spuList = new FaiList<>();
        FaiList<Param> storeSkuList = new FaiList<>();
        Map<Integer, FaiList<Param>> remarkMap = new HashMap<>();
        FaiList<Param> specList = new FaiList<>();

        HashMap<String, Integer> unionPriIdMap = new HashMap<>();
        // 记录总店服务的状态
        HashMap<String, Integer> storeStatusMap = new HashMap<>();
        for (Param ykService : ykServiceList) {
            int siteId = ykService.getInt("yid");
            int keepPriId1 = ykService.getInt("storeId");
            int rlPdId = ykService.getInt("serviceId");
            String name = ykService.getString("name");
            long price = ykService.getInt("price");
            String imgList = ykService.getString("poster");
            int ykFlag = ykService.getInt("flag");
            int ykStatus = ykService.getInt("status");
            long createTime = ykService.getLong("createTime");
            long updateTime = ykService.getLong("updateTime");
            Calendar sysCreateTime = Calendar.getInstance();
            sysCreateTime.setTimeInMillis(createTime);
            Calendar sysUpdateTime = Calendar.getInstance();
            sysUpdateTime.setTimeInMillis(updateTime);
            double duration = ykService.getDouble("duration");
            int priceType = ykService.getInt("priceType");
            int modeType = ykService.getInt("modeType");
            int sort = ykService.getInt("sort");
            int keepIntProp1 = ykService.getInt("serviceType");

            // 记录 tid siteId lgId keepPriId 和 uid 的关系, 减少调用主键服务
            Integer unionPriId = unionPriIdMap.get(tid + "_" + siteId + "_" + lgId + "_" + keepPriId1);
            if (unionPriId == null) {
                unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
                unionPriIdMap.put(tid + "_" + siteId + "_" + lgId + "_" + keepPriId1, unionPriId);
            }
            Integer ownUnionPriId = unionPriId;
            if (keepPriId1 != 0) {
                ownUnionPriId = unionPriIdMap.get(tid + "_" + siteId + "_" + lgId + "_" + 0);
                if (ownUnionPriId == null) {
                    ownUnionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, 0);
                    unionPriIdMap.put(tid + "_" + siteId + "_" + lgId + "_" + 0, ownUnionPriId);
                }
            }

            // 富文本数据，只处理总部的数据
            if (keepPriId1 == 0) {
                Param ykServiceSub = ykServiceSubMap.get(siteId + "-" + rlPdId + "-" + keepPriId1);
                if (Str.isEmpty(ykServiceSub)) {
                    rt = Errno.ERROR;
                    throw new MgException(rt, "data error,ykServiceSub is empty;aid=%d;yid=%d;serviceId=%d;storeId=%d", siteId, rlPdId, keepPriId1);
                }
                String remark = ykServiceSub.getString("context", "");
                String remark1 = ykServiceSub.getString("detail", "");

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
                Param remark1Info = new Param();
                remark1Info.setInt(MgRichTextEntity.Info.RLID, rlPdId); //业务功能Id（必填）
                remark1Info.setInt(MgRichTextEntity.Info.BIZ, MgRichTextValObj.Biz.PRODUCT);
                remark1Info.setInt(MgRichTextEntity.Info.TYPE, RichTextValObj.Type.REMARK1); //内容类型（必填）
                remark1Info.setString(MgRichTextEntity.Info.CONTENT, remark1); // 富文本内容（选填）
                remarkList.add(remarkInfo);
                remarkList.add(remark1Info);
            }

            // 将门店软删除状态 转换成 中台的软删除状态
            if (ykStatus == 2) {
                ykStatus = -1;
            }

            if (keepPriId1 == 0) {
                storeStatusMap.put(siteId + "-" + keepPriId1 + "-" + rlPdId, ykStatus);
            }

            // 规格数据，只处理总店的数据 (实际服务没有规格，由中台默认生成一个规格)
            if (keepPriId1 == 0) {
                Param spec = new Param();
                // 先填入 rlPdId 后面需要替换成 pdId
                spec.setInt("rlPdId", rlPdId);
                spec.setString(ProductSpecEntity.Info.NAME, "全部");
                spec.setInt(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
                spec.setInt(ProductSpecEntity.Info.SOURCE_TID, tid);
                spec.setCalendar(ProductSpecEntity.Info.SYS_CREATE_TIME, sysCreateTime);
                spec.setCalendar(ProductSpecEntity.Info.SYS_UPDATE_TIME, sysUpdateTime);
                // 规格只有软删除状态或者默认状态
                spec.setInt(ProductSpecSkuEntity.Info.STATUS, ykStatus == -1 ? -1 : 0);
                // 允许规格值为空
                spec.setInt(ProductSpecEntity.Info.FLAG, ProductSpecValObj.Spec.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY);
                spec.setList(ProductSpecEntity.Info.IN_PD_SC_VAL_LIST, new FaiList<>());
                specList.add(spec);
            }

            // spu 数据
            Param spuInfo = new Param();
            spuInfo.setInt(SpuBizSummaryEntity.Info.AID, aid);
            spuInfo.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
            spuInfo.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);
            spuInfo.setInt(SpuBizSummaryEntity.Info.PRICE_TYPE, priceType);
            spuInfo.setInt(SpuBizSummaryEntity.Info.MODE_TYPE, modeType);
            spuInfo.setInt(SpuBizSummaryEntity.Info.SYS_TYPE, sysType);
            spuInfo.setInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, ownUnionPriId);
            spuInfo.setCalendar(SpuBizSummaryEntity.Info.SYS_CREATE_TIME, sysCreateTime);
            spuInfo.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, sysUpdateTime);
            spuInfo.setLong(SpuBizSummaryEntity.Info.MIN_PRICE, price);
            spuInfo.setLong(SpuBizSummaryEntity.Info.MAX_PRICE, price);
            spuInfo.setInt(SpuBizSummaryEntity.Info.STATUS, ykStatus);
            if (keepPriId1 != 0 && ykStatus == -1) {
                Integer curStatus = storeStatusMap.get(siteId + "-" + 0 + "-" + rlPdId);
                if (curStatus == null) {
                    Log.logErr(Errno.ERROR, "获取总店状态失败;flow=%d;aid=%d;siteId=%d;keepPriId=%d;rlPdId=%d;", flow, aid, siteId, keepPriId1, rlPdId);
                    return Errno.ERROR;
                }
                spuInfo.setInt(SpuBizSummaryEntity.Info.STATUS, curStatus);
            }
            spuList.add(spuInfo);

            // 销售 sku 数据
            Param storeSku = new Param();
            storeSku.setInt(StoreSalesSkuEntity.Info.AID, aid);
            storeSku.setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId);
            storeSku.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
            storeSku.setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, ownUnionPriId);
            storeSku.setLong(StoreSalesSkuEntity.Info.PRICE, price);
            storeSku.setDouble(StoreSalesSkuEntity.Info.DURATION, duration);
            storeSku.setInt(StoreSalesSkuEntity.Info.FLAG, StoreSalesSkuValObj.FLag.SETED_PRICE);
            storeSku.setInt(StoreSalesSkuEntity.Info.SYS_TYPE, sysType);
            storeSku.setInt("status", ykStatus);
            if (keepPriId1 != 0 && ykStatus == -1) {
                Integer curStatus = storeStatusMap.get(siteId + "-" + 0 + "-" + rlPdId);
                if (curStatus == null) {
                    Log.logErr(Errno.ERROR, "获取总店状态失败;flow=%d;aid=%d;siteId=%d;keepPriId=%d;rlPdId=%d;", flow, aid, siteId, keepPriId1, rlPdId);
                    return Errno.ERROR;
                }
                storeSku.setInt("status", curStatus);
            }
            storeSku.setCalendar(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, sysCreateTime);
            storeSku.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, sysUpdateTime);
            storeSkuList.add(storeSku);

            // 商品基础数据
            if (keepPriId1 == 0) {
                Param basicInfo = new Param();
                basicInfo.setInt(ProductBasicEntity.ProductInfo.AID, aid);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.SOURCE_TID, tid);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.RL_PD_ID, rlPdId);
                basicInfo.setString(ProductBasicEntity.ProductInfo.NAME, name);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.SOURCE_UNIONPRIID, unionPriId);
                basicInfo.setString(ProductBasicEntity.ProductInfo.IMG_LIST, imgList);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.STATUS, ykStatus);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.FLAG, ykFlag);
                basicInfo.setInt(ProductBasicEntity.ProductInfo.KEEP_INT_PROP1, keepIntProp1);
                basicInfo.setCalendar(ProductBasicEntity.ProductInfo.CREATE_TIME, sysCreateTime);
                basicInfo.setCalendar(ProductBasicEntity.ProductInfo.UPDATE_TIME, sysUpdateTime);

                Set<Integer> bindGroupIds = bindGroupMap.get(siteId + "-" + rlPdId);
                Param info = new Param();
                info.setParam(MigrateDef.Info.ADD_PD, basicInfo);
                if(bindGroupIds != null) {
                    info.setList(MigrateDef.Info.BIND_RL_GROUP, new FaiList<>(bindGroupIds));
                }
                unionPriIdRlPdId_info.put(ownUnionPriId + "-" + rlPdId, info);
            }

            Param relInfo = new Param();
            relInfo.setInt(ProductBasicEntity.ProductInfo.AID, aid);
            relInfo.setInt(ProductBasicEntity.ProductInfo.RL_PD_ID, rlPdId);
            relInfo.setInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID, unionPriId);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SOURCE_TID, tid);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.ADD_TIME, sysCreateTime);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.LAST_UPDATE_TIME, sysUpdateTime);
            relInfo.setInt(ProductBasicEntity.ProductInfo.STATUS, ykStatus);
            relInfo.setInt(ProductBasicEntity.ProductInfo.RL_FLAG, ykFlag);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SORT, sort);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.CREATE_TIME, sysCreateTime);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.UPDATE_TIME, sysUpdateTime);
            relInfo.setInt(ProductBasicEntity.ProductInfo.SYS_TYPE, 1);
            relInfo.setCalendar(ProductBasicEntity.ProductInfo.TOP, defaultCal);
            // 0x200 门店通的service 用这个位标识 置顶
            if(Misc.checkBit(ykFlag, 0x200)) {
                relInfo.setCalendar(ProductBasicEntity.ProductInfo.TOP, Calendar.getInstance());
            }

            Param info = unionPriIdRlPdId_info.get(ownUnionPriId + "-" + rlPdId);
            if (info == null) {
                Log.logErr("get ownUnionPriId info is null;aid=%d;ownUnionPriId=%d;rlPdId=%d", aid, ownUnionPriId, rlPdId);
                return Errno.ERROR;
            }
            FaiList<Param> bindList = info.getList(MigrateDef.Info.BIND_PD_REL);
            if(bindList == null) {
                bindList = new FaiList<>();
                info.setList(MigrateDef.Info.BIND_PD_REL, bindList);
            }
            bindList.add(relInfo);

            if(keepPriId1 == 0) {
                FaiList<Integer> storeIds = notExistPd.get(siteId + "-" + rlPdId);
                if(storeIds != null) {
                    for(int storeId : storeIds) {
                        Param newRelInfo = relInfo.clone();
                        Integer curUnionPriId = unionPriIdMap.get(tid + "_" + siteId + "_" + lgId + "_" + storeId);
                        if (curUnionPriId == null) {
                            curUnionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, storeId);
                            unionPriIdMap.put(tid + "_" + siteId + "_" + lgId + "_" + storeId, curUnionPriId);
                        }
                        newRelInfo.setInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID, curUnionPriId);
                        bindList.add(newRelInfo);
                        // spu数据
                        Param curSpuInfo = new Param();
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.AID, aid);
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, curUnionPriId);
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.PRICE_TYPE, priceType);
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.MODE_TYPE, modeType);
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.SYS_TYPE, sysType);
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, ownUnionPriId);
                        curSpuInfo.setCalendar(SpuBizSummaryEntity.Info.SYS_CREATE_TIME, sysCreateTime);
                        curSpuInfo.setCalendar(SpuBizSummaryEntity.Info.SYS_UPDATE_TIME, sysUpdateTime);
                        curSpuInfo.setLong(SpuBizSummaryEntity.Info.MIN_PRICE, price);
                        curSpuInfo.setLong(SpuBizSummaryEntity.Info.MAX_PRICE, price);
                        curSpuInfo.setInt(SpuBizSummaryEntity.Info.STATUS, ykStatus);
                        if (storeId != 0 && ykStatus == -1) {
                            Integer curStatus = storeStatusMap.get(siteId + "-" + 0 + "-" + rlPdId);
                            if (curStatus == null) {
                                Log.logErr(Errno.ERROR, "获取总店状态失败;flow=%d;aid=%d;siteId=%d;keepPriId=%d;rlPdId=%d;", flow, aid, siteId, keepPriId1, rlPdId);
                                return Errno.ERROR;
                            }
                            curSpuInfo.setInt("status", curStatus);
                        }
                        spuList.add(curSpuInfo);
                        // storeSku 数据
                        Param curStoreSku = new Param();
                        curStoreSku.setInt(StoreSalesSkuEntity.Info.AID, aid);
                        curStoreSku.setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId);
                        curStoreSku.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, curUnionPriId);
                        curStoreSku.setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, ownUnionPriId);
                        curStoreSku.setLong(StoreSalesSkuEntity.Info.PRICE, price);
                        curStoreSku.setDouble(StoreSalesSkuEntity.Info.DURATION, duration);
                        curStoreSku.setInt(StoreSalesSkuEntity.Info.FLAG, StoreSalesSkuValObj.FLag.SETED_PRICE);
                        curStoreSku.setInt(StoreSalesSkuEntity.Info.SYS_TYPE, sysType);
                        curStoreSku.setInt("status", ykStatus);
                        if (storeId != 0 && ykStatus == -1) {
                            Integer curStatus = storeStatusMap.get(siteId + "-" + 0 + "-" + rlPdId);
                            if (curStatus == null) {
                                Log.logErr(Errno.ERROR, "获取总店状态失败;flow=%d;aid=%d;siteId=%d;keepPriId=%d;rlPdId=%d;", flow, aid, siteId, keepPriId1, rlPdId);
                                return Errno.ERROR;
                            }
                            storeSku.setInt("status", curStatus);
                        }
                        curStoreSku.setCalendar(StoreSalesSkuEntity.Info.SYS_CREATE_TIME, sysCreateTime);
                        curStoreSku.setCalendar(StoreSalesSkuEntity.Info.SYS_UPDATE_TIME, sysUpdateTime);
                        storeSkuList.add(curStoreSku);
                    }
                }
            }

        }

        FaiList<Param> serviceList = new FaiList<>();
        for (String key : unionPriIdRlPdId_info.keySet()) {
            Param info = unionPriIdRlPdId_info.get(key);
            serviceList.add(info);
        }

        ProductBasicProc basicProc = new ProductBasicProc(flow);
        // 先找出是否是重复迁移数据的 pdId, 如果存在则清除基础信息中的数据
        FaiList<Integer> migratePdIds = basicProc.getMigratePdIds(aid, sysType, true);
        Log.logDbg("needDelPdId=%s", migratePdIds);

        // 同步基础信息
        FaiList<Param> returnList = basicProc.dataMigrate(aid, tid, serviceList, sysType);
        Map<String, Integer> unionPriIdRlPdId_pdId = new HashMap<>();

        FaiList<Integer> pdIdList = new FaiList<>();
        for(Param returnInfo : returnList) {
            int unionPriId = returnInfo.getInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID);
            int rlPdId = returnInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID);
            int pdId = returnInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);
            pdIdList.add(pdId);
            unionPriIdRlPdId_pdId.put(unionPriId + "-" + rlPdId, pdId);
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
                if (!migratePdIds.isEmpty()) {
                    richProc.batchDel("", aid, tid, siteId, lgId, keepPriId1, migratePdIds);
                }
                richProc.batchAdd("", aid, tid, siteId, lgId, keepPriId1, remarkMap.get(unionPriId));
            }
        }

        // 同步spu数据
        if (!spuList.isEmpty()) {
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
            // 支持重复迁移
            if (!migratePdIds.isEmpty()) {
                storeProc.migrateYKServiceDel(aid, migratePdIds);
            }
            storeProc.migrateYKService(aid, spuList);
        }

        // 同步 spec 数据
        for (Param specInfo : specList) {
            int rlPdId = specInfo.getInt("rlPdId");
            Integer unionPriId = specInfo.getInt(ProductSpecEntity.Info.SOURCE_UNION_PRI_ID);
            Integer pdId = unionPriIdRlPdId_pdId.get(unionPriId + "-" + rlPdId);
            if (pdId == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "spec get pdId err;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
                return rt;
            }
            specInfo.setInt(ProductSpecEntity.Info.PD_ID, pdId);
        }
        if (!specList.isEmpty()) {
            ProductSpecProc specProc = new ProductSpecProc(flow);
            // 支持重复迁移
            if (!migratePdIds.isEmpty()) {
                specProc.batchDelPdAllSc(aid, tid, migratePdIds, "", false);
            }
            returnList = specProc.migrateYKService(aid, specList);
        }

        // pdId 与 skuId 的映射
        Map<Integer, Long> pdId_skuIdMap = new HashMap<>();
        for (Param info : returnList) {
            int pdId = info.getInt(ProductSpecSkuEntity.Info.PD_ID);
            long skuId = info.getLong(ProductSpecSkuEntity.Info.SKU_ID);
            pdId_skuIdMap.put(pdId, skuId);
        }

        // 同步库存销售
        for (Param storeSku : storeSkuList) {
            Integer rlPdId = storeSku.getInt(StoreSalesSkuEntity.Info.RL_PD_ID);
            Integer unionPriId = storeSku.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
            Integer pdId = unionPriIdRlPdId_pdId.get(unionPriId + "-" + rlPdId);
            if (pdId == null) {
                rt = Errno.ERROR;
                Log.logErr("storeSku get pdId err;aid=%d;uid=%d;rlPdId=%d;", aid, unionPriId, rlPdId);
                return rt;
            }
            storeSku.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
            Long skuId = pdId_skuIdMap.get(pdId);
            if (skuId == null) {
                rt = Errno.ERROR;
                Log.logErr("storeSku get skuId err;aid=%d;uid=%d;pdId=%d;", aid, unionPriId, pdId);
                return rt;
            }
            storeSku.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
        }
        if (!storeSkuList.isEmpty()) {
            ProductStoreProc storeProc = new ProductStoreProc(flow);
            storeProc.migrateYKStoreSku(aid, storeSkuList);
        }

        Log.logStd("migrate yk service ok;pdIdList=%s;", pdIdList);

        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logDbg("migrateYKService ok;flow=%d;aid=%d;", flow, aid);
        rt = Errno.OK;
        return rt;
    }
}

