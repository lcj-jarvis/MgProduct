package fai.MgProductInfSvr.application.service;

import fai.MgBackupSvr.interfaces.cli.MgBackupCli;
import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductGroupSvr.interfaces.cli.MgProductGroupCli;
import fai.MgProductInfSvr.application.MgProductInfSvr;
import fai.MgProductInfSvr.domain.comm.BizPriKey;
import fai.MgProductInfSvr.domain.comm.ProductSpecCheck;
import fai.MgProductInfSvr.domain.serviceproc.*;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.entity.*;
import fai.MgProductPropSvr.interfaces.cli.MgProductPropCli;
import fai.MgProductSpecSvr.interfaces.cli.MgProductSpecCli;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuEntity;
import fai.MgProductSpecSvr.interfaces.entity.ProductSpecSkuValObj;
import fai.MgProductStoreSvr.interfaces.cli.MgProductStoreCli;
import fai.MgProductStoreSvr.interfaces.entity.SkuSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.SpuBizSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.SpuSummaryEntity;
import fai.MgProductStoreSvr.interfaces.entity.StoreSalesSkuEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;
import fai.mgproduct.comm.Util;
import fai.comm.middleground.app.CloneDef;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 维护接口服务各个service共用的方法
 */
public class MgProductInfService extends ServicePub {

    /**
     * 获取备份服务数据
     */
    protected Param getBackupInfo(int flow, int aid, int tid, int rlBackupId) {
        int rt = Errno.ERROR;
        MgBackupCli cli = new MgBackupCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgBackupCli error");
        }

        Param info = new Param();
        rt = cli.getBackupInfo(aid, tid, rlBackupId, info);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getBackupInfo error;flow=%d;aid=%d;tid=%d;rlBackupId=%d;", flow, aid, tid, rlBackupId);
        }
        return info;
    }

    /**
     * 获取unionPriId，返回rt
     */
    protected int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Ref<Integer> idRef) {
        int rt = Errno.OK;
        try {
            int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
            idRef.value = unionPriId;
        }catch (MgException e) {
            e.log();
            rt = e.getRt();
        }
        return rt;
    }

    /**
     * 获取unionPriId, 返回值为unionPriId, 出错直接抛异常
     */
    protected int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgPrimaryKeyCli error");
        }

        Ref<Integer> idRef = new Ref<>();
        rt = cli.getUnionPriId(aid, tid, siteId, lgId, keepPriId1, idRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getUnionPriId error;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
        }
        return idRef.value;
    }

    /**
     * 获取unionPriId
     * @return unionPriId
     */
    protected int getUnionPriIdWithoutAdd(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgPrimaryKeyCli error");
        }

        Ref<Integer> idRef = new Ref<>();
        rt = cli.getUnionPriId(aid, tid, siteId, lgId, keepPriId1, false, idRef);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getUnionPriId error;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
        }
        return idRef.value;
    }

    /**
     * 根据unionPriIdList 获取主键信息
     * @param tid
     * @param unionPriIds
     * @param list
     */
    protected int getPrimaryKeyListByUnionPriIds(int flow, int aid, int tid, FaiList<Integer> unionPriIds, FaiList<Param> list) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error");
            return rt;
        }

        rt = cli.getListByUnionPriIds(unionPriIds, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getListByUnionPriIds error;flow=%d;aid=%d;tid=%d;unionPriIds=%s;", flow, aid, tid, unionPriIds);
            return rt;
        }
        return rt;
    }

    protected int getPrimaryKeyList(int flow, int aid, FaiList<Param> searchArgList, FaiList<Param> list) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        rt = cli.getPrimaryKeyList(aid, searchArgList, list);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getPrimaryKeyList error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        return rt;
    }

    protected FaiList<Param> getPrimaryKeyListWithOutAdd(int flow, int aid, FaiList<Param> searchArgList) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgPrimaryKeyCli error;flow=%d;aid=%d;", flow, aid);
        }

        FaiList<Param> list = new FaiList<>();
        rt = cli.getPrimaryKeyList(aid, searchArgList, false, list);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getPrimaryKeyList error;flow=%d;aid=%d;", flow, aid);
        }
        return list;
    }

    protected int getPdIdWithAdd(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, unionPriId, rlPdId, idRef, true);
    }
    /**
     * 获取PdId
     */
    protected int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, unionPriId, rlPdId, idRef, false);
    }

    protected int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef, boolean withAdd) {
        int rt = Errno.ERROR;
        MgProductBasicCli mgProductBasicCli = new MgProductBasicCli(flow);
        if(!mgProductBasicCli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgProductBasicCli error");
            return rt;
        }

        Param pdRelInfo = new Param();
        rt = mgProductBasicCli.getRelInfoByRlId(aid, unionPriId, rlPdId, pdRelInfo);
        if(rt != Errno.OK) {
            if(withAdd && (rt == Errno.NOT_FOUND)){
                rt = mgProductBasicCli.addProductAndRel(aid, tid, unionPriId, new Param()
                                .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                        , idRef, new Ref<>());
                if(rt != Errno.OK) {
                    Log.logErr(rt, "addProductAndRel error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
                    return rt;
                }
            }
            Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
            return rt;
        }
        idRef.value = pdRelInfo.getInt(ProductRelEntity.Info.PD_ID);
        return rt;
    }

    /**
     * 克隆
     */
    @SuccessRt(Errno.OK)
    public int cloneData(FaiSession session, int flow, int aid, Param primaryKey, int fromAid, FaiList<Param> clonePrimaryKeys, Param cloneOption) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || clonePrimaryKeys == null || clonePrimaryKeys.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;key=%s;clonePrimaryKeys=%s;cloneOption=%s;", flow, aid, primaryKey, clonePrimaryKeys, cloneOption);
            return rt;
        }

        FaiList<Param> primaryKeys = new FaiList<>();
        for(Param info : clonePrimaryKeys) {
            Param toPrimary = info.getParam(CloneDef.Info.TO_PRIMARY_KEY);
            Param fromPrimary = info.getParam(CloneDef.Info.FROM_PRIMARY_KEY);
            primaryKeys.add(toPrimary);
            primaryKeys.add(fromPrimary);
        }

        // 查所有克隆相关的主键信息，查不到 不执行自动添加
        FaiList<Param> list = getPrimaryKeyListWithOutAdd(flow, aid, primaryKeys);
        Map<BizPriKey, Integer> bizPriKeyUnionPriIdMap = toBizPriKeyUnionPriIdMap(list);

        FaiList<Param> cloneUnionPriIds = new FaiList<>();
        for(Param info : clonePrimaryKeys) {
            Param fromPrimary = info.getParam(CloneDef.Info.FROM_PRIMARY_KEY);
            int fromTid = fromPrimary.getInt(MgPrimaryKeyEntity.Info.TID);
            int fromSiteId = fromPrimary.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            int fromLgId = fromPrimary.getInt(MgPrimaryKeyEntity.Info.LGID);
            int fromKeepPriId1 = fromPrimary.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
            Integer fromUnionPriId = bizPriKeyUnionPriIdMap.get(new BizPriKey(fromTid, fromSiteId, fromLgId, fromKeepPriId1));
            // 如果fromUnionPriId不存在，则不会有相关数据的，跳过
            if(fromUnionPriId == null) {
                continue;
            }
            Param toPrimary = info.getParam(CloneDef.Info.TO_PRIMARY_KEY);
            int toTid = toPrimary.getInt(MgPrimaryKeyEntity.Info.TID);
            int toSiteId = toPrimary.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            int toLgId = toPrimary.getInt(MgPrimaryKeyEntity.Info.LGID);
            int toKeepPriId1 = toPrimary.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
            // 直接调用cli去拿，因为之前已经查过一次，如果查到了，则cli会有缓存，如果没有，则说明需要生成
            Integer toUnionPriId = getUnionPriId(flow, aid, toTid, toSiteId, toLgId, toKeepPriId1);

            Param cloneUnionPriId = new Param();
            cloneUnionPriId.setInt(CloneDef.Info.TO_PRIMARY_KEY, toUnionPriId);
            cloneUnionPriId.setInt(CloneDef.Info.FROM_PRIMARY_KEY, fromUnionPriId);
            cloneUnionPriIds.add(cloneUnionPriId);
        }
        if(cloneUnionPriIds.isEmpty()) {
            rt = Errno.ARGS_ERROR;
            Log.logErr(rt, "cloneUnionPriIds is empty;flow=%d;aid=%d;key=%s;clonePrimaryKeys=%s;cloneOption=%s;", flow, aid, primaryKey, clonePrimaryKeys, cloneOption);
            return rt;
        }

        // 默认都克隆
        boolean cloneAll = Str.isEmpty(cloneOption);

        // 克隆分类数据
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.GROUP, false)) {
            ProductGroupProc groupProc = new ProductGroupProc(flow);
            groupProc.cloneData(aid, fromAid, cloneUnionPriIds);
        }

        //TODO 克隆基础数据
        //TODO 克隆库数据
        //TODO 克隆标签数据
        //TODO 克隆参数数据

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("clone ok;flow=%d;aid=%d;key=%s;clonePrimaryKeys=%s;cloneOption=%s;", flow, aid, primaryKey, clonePrimaryKeys, cloneOption);
        return rt;
    }

    /**
     * 增量克隆
     */
    @SuccessRt(Errno.OK)
    public int incrementalClone(FaiSession session, int flow, int aid, Param primaryKey, int fromAid, Param fromPrimaryKey, Param cloneOption) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || Str.isEmpty(fromPrimaryKey)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, primaryKeys is empty;flow=%d;aid=%d;primaryKey=%s;fromPrimaryKey=%s;cloneOption=%s;", flow, aid, primaryKey, fromPrimaryKey, cloneOption);
            return rt;
        }

        int fromTid = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        int fromSiteId = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
        int fromLgId = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
        int fromKeepPriId1 = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
        int fromUnionPriId = getUnionPriIdWithoutAdd(flow, aid, fromTid, fromSiteId, fromLgId, fromKeepPriId1);

        int tid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        int siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
        int lgId = primaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
        int keepPriId1 = primaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
        int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);


        // 默认都克隆
        boolean cloneAll = Str.isEmpty(cloneOption);

        // 克隆分类数据
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.GROUP, false)) {
            ProductGroupProc groupProc = new ProductGroupProc(flow);
            groupProc.incrementalClone(aid, unionPriId, fromAid, fromUnionPriId);
        }

        //TODO 克隆基础数据
        //TODO 克隆库数据
        //TODO 克隆标签数据
        //TODO 克隆参数数据

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd(rt, "incrementalClone ok;flow=%d;aid=%d;primaryKey=%s;fromPrimaryKey=%s;cloneOption=%s;", flow, aid, primaryKey, fromPrimaryKey, cloneOption);
        return rt;
    }

    /**
     * 备份
     */
    @SuccessRt(Errno.OK)
    public int backupData(FaiSession session, int flow, int aid, Param primaryKey, FaiList<Param> backupPrimaryKeys, int rlBackupId) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || Util.isEmptyList(backupPrimaryKeys) || rlBackupId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;key=%s;backupPrimaryKeys=%s;rlBackupId=%s;", flow, aid, primaryKey, backupPrimaryKeys, rlBackupId);
            return rt;
        }
        int tid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);

        // 获取unionPriId
        FaiList<Param> list = getPrimaryKeyListWithOutAdd(flow, aid, backupPrimaryKeys);
        FaiList<Integer> unionPriIds = new FaiList<>();
        for(Param info : list) {
            unionPriIds.add(info.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID));
        }

        // 获取备份服务数据
        Param backupInfo = getBackupInfo(flow, aid, tid, rlBackupId);

        // 备份分类数据
        ProductGroupProc groupProc = new ProductGroupProc(flow);
        groupProc.backupData(aid, unionPriIds, backupInfo);

        //TODO 备份基础数据
        //TODO 备份库数据
        //TODO 备份标签数据
        //TODO 备份参数数据

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("backup ok;flow=%d;aid=%d;key=%s;backupPrimaryKeys=%s;rlBackupId=%s;", flow, aid, primaryKey, backupPrimaryKeys, rlBackupId);
        return rt;
    }

    /**
     * 还原备份
     */
    @SuccessRt(Errno.OK)
    public int restoreBackupData(FaiSession session, int flow, int aid, Param primaryKey, FaiList<Param> restorePrimaryKeys, int rlBackupId, Param restoreOption) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || Util.isEmptyList(restorePrimaryKeys) || rlBackupId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;key=%s;restorePrimaryKeys=%s;rlBackupId=%s;", flow, aid, primaryKey, restorePrimaryKeys, rlBackupId);
            return rt;
        }
        int tid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);

        // 获取unionPriId
        FaiList<Param> list = getPrimaryKeyListWithOutAdd(flow, aid, restorePrimaryKeys);
        FaiList<Integer> unionPriIds = new FaiList<>();
        for(Param info : list) {
            unionPriIds.add(info.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID));
        }

        // 获取备份服务数据
        Param backupInfo = getBackupInfo(flow, aid, tid, rlBackupId);

        // 默认操作所有数据
        boolean restoreAll = Str.isEmpty(restoreOption);

        // 还原分类数据
        if(restoreAll || restoreOption.getBoolean(MgProductEntity.Option.GROUP, false)) {
            ProductGroupProc groupProc = new ProductGroupProc(flow);
            groupProc.restoreBackup(aid, unionPriIds, backupInfo);
        }

        //TODO 还原基础数据
        //TODO 还原库数据
        //TODO 还原标签数据
        //TODO 还原参数数据

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("restore ok;flow=%d;aid=%d;key=%s;restoreKeys=%s;rlBackupId=%s;", flow, aid, primaryKey, restorePrimaryKeys, rlBackupId);
        return rt;
    }

    /**
     * 删除备份
     */
    @SuccessRt(Errno.OK)
    public int delBackup(FaiSession session, int flow, int aid, Param primaryKey, int rlBackupId) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || rlBackupId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;key=%s;rlBackupId=%s;", flow, aid, primaryKey, rlBackupId);
            return rt;
        }
        int tid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);

        // 获取备份服务数据
        Param backupInfo = getBackupInfo(flow, aid, tid, rlBackupId);

        // 删除分类数据备份
        ProductGroupProc groupProc = new ProductGroupProc(flow);
        groupProc.delBackup(aid, backupInfo);

        //TODO 删除基础数据备份
        //TODO 删除库数据备份
        //TODO 删除标签数据备份
        //TODO 删除参数数据备份

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("del backup ok;flow=%d;aid=%d;key=%s;rlBackupId=%s;", flow, aid, primaryKey, rlBackupId);
        return rt;
    }

    public int clearCache(FaiSession session, int flow, int aid) throws IOException {
        int returnRt = Errno.OK;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            MgProductBasicCli mgProductBasicCli = FaiCliFactory.createCli(MgProductBasicCli.class, flow);
            int rt = mgProductBasicCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("basic clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }
            MgProductGroupCli mgProductGroupCli = FaiCliFactory.createCli(MgProductGroupCli.class, flow);
            rt = mgProductGroupCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("group clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            MgProductPropCli mgProductPropCli = FaiCliFactory.createCli(MgProductPropCli.class, flow);
            rt = mgProductPropCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("prop clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            MgProductSpecCli mgProductSpecCli = FaiCliFactory.createCli(MgProductSpecCli.class, flow);
            rt = mgProductSpecCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("spec clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            MgProductStoreCli mgProductStoreCli = FaiCliFactory.createCli(MgProductStoreCli.class, flow);
            rt = mgProductStoreCli.nkClearCache(aid);
            if(rt != Errno.OK) {
                Log.logErr("store clear cache error;flow=%d;aid=%d;", flow, aid);
                returnRt = rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(returnRt != Errno.OK, returnRt);
        }
        return returnRt;
    }

    /**
     * 获取的商品全部组合信息
     * @param tid 创建商品的tid
     * @param siteId 创建商品的siteId
     * @param lgId 创建商品的lgId
     * @param keepPriId1 创建商品的keepPriId1
     * @param rlPdId 业务商品id
     */
    public int getProductFullInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);

            // 1 获取商品关联信息
            Param pdRelInfo = new Param();
            rt = productBasicProc.getRelInfoByRlId(aid, unionPriId, rlPdId, pdRelInfo);
            if(rt != Errno.OK){
                return rt;
            }
            int pdId = pdRelInfo.getInt(ProductRelEntity.Info.PD_ID);
            // 2 获取商品基础信息
            // 3 ... 获取商品参数啥的 ... ↓
            // 3.1 获取规格相关
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            // 获取商品规格
            FaiList<Param> pdScInfoList = new FaiList<>();
            rt = productSpecProc.getPdScInfoList(aid, tid, unionPriId, pdId, pdScInfoList, false);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            // 获取商品规格sku
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            rt = productSpecProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, true, pdScSkuInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            // 3.2 获取销售库存相关
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> pdScSkuSalesStoreInfoList = new FaiList<>();
            rt = productStoreProc.getSkuStoreSalesByPdId(aid, tid, pdId, pdScSkuSalesStoreInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            if(!pdScSkuSalesStoreInfoList.isEmpty()){
                FaiList<Integer> unionPriIdList = new FaiList<>();
                for (Param pdScSkuSalesStoreInfo : pdScSkuSalesStoreInfoList) {
                    Integer _unionPriId = pdScSkuSalesStoreInfo.getInt(StoreSalesSkuEntity.Info.UNION_PRI_ID);
                    unionPriIdList.add(_unionPriId);
                }
                FaiList<Param> primaryKeyList = new FaiList<>();
                rt = getPrimaryKeyListByUnionPriIds(flow, aid, tid, unionPriIdList, primaryKeyList);
                if(rt != Errno.OK){
                    return rt;
                }
                Map<Integer, BizPriKey> unionPriIdBizPriKeyMap = toUnionPriIdBizPriKeyMap(primaryKeyList);
                ProductStoreService.initSkuStoreSalesPrimaryInfo(unionPriIdBizPriKeyMap, pdScSkuSalesStoreInfoList);
            }
            Param productInfo = new Param();
            productInfo.setList(MgProductEntity.Info.SPEC, pdScInfoList);
            productInfo.setList(MgProductEntity.Info.SPEC_SKU, pdScSkuInfoList);
            productInfo.setList(MgProductEntity.Info.STORE_SALES, pdScSkuSalesStoreInfoList);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            productInfo.toBuffer(sendBuf, MgProductDto.Key.INFO, MgProductDto.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

    public int getProductList4Adm(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, Param combined) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);

            // 1 获取商品关联信息
            FaiList<Param> pdRelInfos = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, unionPriId, rlPdIds, pdRelInfos);
            if(rt != Errno.OK){
                return rt;
            }
            FaiList<Integer> pdIds = new FaiList<>();
            for(int i = 0; i < pdRelInfos.size(); i++) {
                Param info = pdRelInfos.get(i);
                pdIds.add(info.getInt(ProductBasicEntity.ProductInfo.PD_ID));
            }
            boolean getSpec = combined.getBoolean(MgProductEntity.Info.SPEC, false);
            boolean getSpecSku = combined.getBoolean(MgProductEntity.Info.SPEC_SKU, false);
            boolean getStoreSales = combined.getBoolean(MgProductEntity.Info.STORE_SALES, false);
            boolean getSpuSales = combined.getBoolean(MgProductEntity.Info.SPU_SALES, false);
            // 2 获取商品基础信息
            // 3 ... 获取商品参数啥的 ... ↓
            // 3.1 获取规格相关
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            // 获取商品规格
            Map<Integer, List<Param>> pdScInfoMap = new HashMap<>();
            if(getSpec) {
                FaiList<Param> pdScInfoList = new FaiList<>();
                rt = productSpecProc.getPdScInfoList4Adm(aid, unionPriId, pdIds, false, pdScInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScInfoMap = pdScInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecEntity.SpecInfo.PD_ID)));
            }

            // 获取商品规格sku
            Map<Integer, List<Param>> pdScSkuInfoMap = new HashMap<>();
            if(getSpecSku) {
                FaiList<Param> pdScSkuInfoList = new FaiList<>();
                rt = productSpecProc.getPdSkuInfoList4Adm(aid, tid, pdIds, true, pdScSkuInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScSkuInfoMap = pdScSkuInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecSkuEntity.Info.PD_ID)));
            }

            // 3.2 获取销售库存相关
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            Map<Integer, List<Param>> pdScSkuSalesStoreInfoMap = new HashMap<>();
            if(getStoreSales) {
                FaiList<Param> pdScSkuSalesStoreInfoList = new FaiList<>();
                rt = productStoreProc.getStoreSalesByPdIdsAndUIdList(aid, tid, pdIds, new FaiList<>(Arrays.asList(unionPriId)), pdScSkuSalesStoreInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScSkuSalesStoreInfoMap = pdScSkuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(StoreSalesSkuEntity.Info.PD_ID)));
            }
            // 3.2.2 spu销售库存相关
            Map<Integer, List<Param>> spuSalesStoreInfoMap = new HashMap<>();
            if(getSpuSales) {
                FaiList<Param> spuSalesStoreInfoList = new FaiList<>();
                rt = productStoreProc.getSpuBizSummaryInfoListByPdIdList(aid, tid, unionPriId, pdIds, spuSalesStoreInfoList, null);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                spuSalesStoreInfoMap = spuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(SpuBizSummaryEntity.Info.PD_ID)));
            }

            FaiList<Param> list = new FaiList<>();
            for(int i = 0; i < pdRelInfos.size(); i++) {
                Param productInfo = pdRelInfos.get(i);
                Integer pdId = productInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);
                Param info = new Param();
                info.setParam(MgProductEntity.Info.BASIC, pdRelInfos.get(i));
                if(getSpec) {
                    FaiList<Param> specList = new FaiList<>();
                    if(pdScInfoMap.containsKey(pdId)) {
                        specList.addAll(pdScInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.SPEC, specList);
                }
                if(getSpecSku) {
                    FaiList<Param> specSkuList = new FaiList<>();
                    if(pdScSkuInfoMap.containsKey(pdId)) {
                        specSkuList.addAll(pdScSkuInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.SPEC_SKU, specSkuList);
                }
                if(getStoreSales) {
                    FaiList<Param> storeSalesList = new FaiList<>();
                    if(pdScSkuSalesStoreInfoMap.containsKey(pdId)) {
                        storeSalesList.addAll(pdScSkuSalesStoreInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.STORE_SALES, storeSalesList);
                }
                if(getSpuSales) {
                    FaiList<Param> spuSalesList = new FaiList<>();
                    if(spuSalesStoreInfoMap.containsKey(pdId)) {
                        spuSalesList.addAll(spuSalesStoreInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.SPU_SALES, spuSalesList);
                }
                list.add(info);
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, MgProductDto.Key.INFO, MgProductDto.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

    public int getProductSummary4Adm(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, Param getKeys) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);

            // 1 获取商品关联信息
            FaiList<Param> pdRelInfos = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, unionPriId, rlPdIds, pdRelInfos);
            if(rt != Errno.OK){
                return rt;
            }
            FaiList<Integer> pdIds = new FaiList<>();
            for(int i = 0; i < pdRelInfos.size(); i++) {
                Param info = pdRelInfos.get(i);
                pdIds.add(info.getInt(ProductBasicEntity.ProductInfo.PD_ID));
            }
            boolean getSpec = getKeys.getBoolean(MgProductEntity.Info.SPEC, false);
            boolean getSpecSku = getKeys.getBoolean(MgProductEntity.Info.SPEC_SKU, false);
            boolean getStoreSales = getKeys.getBoolean(MgProductEntity.Info.STORE_SALES, false);
            boolean getSpuSales = getKeys.getBoolean(MgProductEntity.Info.SPU_SALES, false);
            // 2 获取商品基础信息
            // 3 ... 获取商品参数啥的 ... ↓
            // 3.1 获取规格相关
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            // 获取商品规格
            Map<Integer, List<Param>> pdScInfoMap = new HashMap<>();
            if(getSpec) {
                FaiList<Param> pdScInfoList = new FaiList<>();
                rt = productSpecProc.getPdScInfoList4Adm(aid, unionPriId, pdIds, false, pdScInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScInfoMap = pdScInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecEntity.SpecInfo.PD_ID)));
            }
            // 获取商品规格sku
            Map<Integer, List<Param>> pdScSkuInfoMap = new HashMap<>();
            if(getSpecSku) {
                FaiList<Param> pdScSkuInfoList = new FaiList<>();
                rt = productSpecProc.getPdSkuInfoList4Adm(aid, tid, pdIds, true, pdScSkuInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScSkuInfoMap = pdScSkuInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecSkuEntity.Info.PD_ID)));
            }

            // 3.2 获取销售库存相关
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            // 3.2.1 sku销售库存相关
            Map<Integer, List<Param>> pdScSkuSalesStoreInfoMap = new HashMap<>();
            if(getStoreSales) {
                FaiList<Param> pdScSkuSalesStoreInfoList = new FaiList<>();
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = new ParamMatcher(SkuSummaryEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
                rt = productStoreProc.getSkuSummaryInfoList(aid, tid, unionPriId, searchArg, pdScSkuSalesStoreInfoList, false);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScSkuSalesStoreInfoMap = pdScSkuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(StoreSalesSkuEntity.Info.PD_ID)));
            }
            // 3.2.2 spu销售库存相关
            Map<Integer, List<Param>> spuSalesStoreInfoMap = new HashMap<>();
            if(getSpuSales) {
                FaiList<Param> spuSalesStoreInfoList = new FaiList<>();
                rt = productStoreProc.getSpuSummaryInfoList(aid, tid, unionPriId, pdIds, spuSalesStoreInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                spuSalesStoreInfoMap = spuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(SpuSummaryEntity.Info.PD_ID)));
            }

            // 组装数据
            FaiList<Param> list = new FaiList<>();
            for(int i = 0; i < pdRelInfos.size(); i++) {
                Param productInfo = pdRelInfos.get(i);
                Integer pdId = productInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);
                Param info = new Param();
                info.setParam(MgProductEntity.Info.BASIC, pdRelInfos.get(i));
                if(getSpec) {
                    FaiList<Param> specList = new FaiList<>();
                    if(pdScInfoMap.containsKey(pdId)) {
                        specList.addAll(pdScInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.SPEC, specList);
                }
                if(getSpecSku) {
                    FaiList<Param> specSkuList = new FaiList<>();
                    if(pdScSkuInfoMap.containsKey(pdId)) {
                        specSkuList.addAll(pdScSkuInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.SPEC_SKU, specSkuList);
                }
                if(getStoreSales) {
                    FaiList<Param> storeSalesList = new FaiList<>();
                    if(pdScSkuSalesStoreInfoMap.containsKey(pdId)) {
                        storeSalesList.addAll(pdScSkuSalesStoreInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.STORE_SALES, storeSalesList);
                }
                if(getSpuSales) {
                    FaiList<Param> spuSalesList = new FaiList<>();
                    if(spuSalesStoreInfoMap.containsKey(pdId)) {
                        spuSalesList.addAll(spuSalesStoreInfoMap.get(pdId));
                    }
                    info.setList(MgProductEntity.Info.SPU_SALES, spuSalesList);
                }
                list.add(info);
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, MgProductDto.Key.INFO, MgProductDto.getSummaryInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

    public int clearRelData(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 处理商品基础信息
            // 1. 删除商品业务绑定关系数据
            // 2. 删除商品参数绑定关系数据
            // 3. 删除商品分类绑定关系数据
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            rt = basicProc.clearRelData(aid, unionPriId, false);
            if(rt != Errno.OK) {
                return rt;
            }

            // 处理商品参数
            // 删除商品参数业务绑定关系数据
            ProductPropProc propProc = new ProductPropProc(flow);
            rt = propProc.clearRelData(aid, unionPriId);
            if(rt != Errno.OK) {
                return rt;
            }

            // 处理库存销售信息
            // 1. 删除unionPriId下的出入库记录数据
            // 2. 删除unionPriId下的预扣记录、退库存记录、直扣记录
            // 3. 删除unionPriId下的sku库存销售信息
            // 4. 删除unionPriId下的spu库存销售信息汇总
            // 5. 重新计算aid下sku、spu库存销售信息汇总
            ProductStoreProc storeProc = new ProductStoreProc(flow);
            rt = storeProc.clearRelData(aid, unionPriId);
            if(rt != Errno.OK) {
                return rt;
            }

            Log.logStd("clear rel data ok;aid=%d;unionPriId=%d;", aid, unionPriId);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int clearAcct(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> primaryKeys) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(tid != FaiValObj.TermId.YK) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, only supports yk del;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            if(primaryKeys == null || primaryKeys.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, primaryKeys is null;flow=%d;aid=%d;tid=%d;primaryKeys=%s;", flow, aid, tid, primaryKeys);
                return rt;
            }

            for(Param primaryKeyInfo : primaryKeys) {
                Integer tmpTid = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.TID);
                // 目前只提供给门店通使用
                if(tmpTid == null || tmpTid != FaiValObj.TermId.YK) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("args error, only supports yk del;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    return rt;
                }
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            FaiList<Param> list = new FaiList<>();
            rt = getPrimaryKeyList(flow, aid, primaryKeys, list);
            if(rt != Errno.OK){
                return rt;
            }
            FaiList<Integer> unionPriIds = new FaiList<>();
            for(Param primaryKeyInfo : list) {
                Integer tmpUnionPriId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
                unionPriIds.add(tmpUnionPriId);
            }

            // 删除商品基础信息相关数据
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            rt = basicProc.clearAcct(aid, unionPriIds);
            if(rt != Errno.OK) {
                return rt;
            }

            // 删除商品参数相关数据
            ProductPropProc propProc = new ProductPropProc(flow);
            rt = propProc.clearAcct(aid, unionPriIds);
            if(rt != Errno.OK) {
                return rt;
            }

            //TODO 删除商品分类数据(商品分类还没上线)

            // 删除规格数据
            ProductSpecProc specProc = new ProductSpecProc(flow);
            rt = specProc.clearAcct(aid, unionPriIds);
            if(rt != Errno.OK) {
                return rt;
            }

            // 删除库存销售信息
            // 1. 删除unionPriId下的出入库记录数据
            // 2. 删除unionPriId下的预扣记录、退库存记录、直扣记录
            // 3. 删除unionPriId下的sku库存销售信息
            // 4. 删除unionPriId下的spu库存销售信息汇总
            // 5. 删除aid下sourceUnionPriId为指定unionPriIds的sku、spu库存销售信息汇总
            ProductStoreProc storeProc = new ProductStoreProc(flow);
            rt = storeProc.clearAcct(aid, unionPriIds);
            if(rt != Errno.OK) {
                return rt;
            }

            Log.logStd("clear rel data ok;aid=%d;do unionPriId=%d;primaryKeys=%s;", aid, unionPriId, primaryKeys);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 导入商品
     * @param ownerTid 创建商品的 ownerTid
     * @param ownerSiteId 创建商品的 ownerSiteId
     * @param ownerLgId 创建商品的 ownerLgId
     * @param ownerKeepPriId1 创建商品的 ownerKeepPriId1
     * @param productList 商品信息集合
     * @param inStoreRecordInfo 入库记录信息
     * @param useMgProductBasicInfo 是否接入使用商品中台基础信息
     */
    public int importProduct(FaiSession session, int flow, int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, FaiList<Param> productList, Param inStoreRecordInfo, boolean useMgProductBasicInfo) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        FaiList<Param> errProductList = new FaiList<>();
        try {
            if(Util.isEmptyList(productList)){
                rt = Errno.ARGS_ERROR;
                Log.logErr("productList error;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                return rt;
            }
            if(productList.size() > MgProductInfSvr.SVR_OPTION.getImportProductMaxSize()){
                rt = Errno.SIZE_LIMIT;
                Log.logErr("productList size limit;flow=%d;aid=%d;ownerTid=%d;size=%s;", flow, aid, ownerTid, productList.size());
                return rt;
            }
            if(!FaiValObj.TermId.isValidTid(ownerTid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, ownerTid is not valid;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, ownerTid, ownerSiteId, ownerLgId, ownerKeepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int ownerUnionPriId = idRef.value;
            HashSet<String> skuCodeSet = new HashSet<>();
            // 检查导入的数据，并分开正确的数据和错误的数据
            checkImportProductList(productList, skuCodeSet, errProductList, useMgProductBasicInfo);

            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            skuCodeSet.removeAll(Collections.singletonList(null)); // 移除所有null值
            if(!skuCodeSet.isEmpty()){ // 校验 skuCode 是否已经存在
                Ref<FaiList<String>> existsSkuCodeListRef = new Ref<>();
                rt = productSpecProc.getExistsSkuCodeList(aid, ownerTid, ownerUnionPriId, new FaiList<>(skuCodeSet), existsSkuCodeListRef);
                if(rt != Errno.OK){
                    return rt;
                }
                HashSet<String> existsSkuCodeSet = new HashSet<>(existsSkuCodeListRef.value);
                // 过滤掉已经存在skuCode的商品数据
                filterExistsSkuCode(productList, existsSkuCodeSet, errProductList);
            }
            // 组装批量添加的商品基础信息
            FaiList<Param> batchAddBasicInfoList = new FaiList<>(productList.size());
            FaiList<Integer> rlPdIdList = new FaiList<>();
            for (Param productInfo : productList) {
                Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
                if(!useMgProductBasicInfo){
                    int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                    batchAddBasicInfoList.add(
                            new Param().setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                    .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                    );
                    productInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdId);
                    rlPdIdList.add(rlPdId);
                }else{
                    // TODO 业务方接入商品基础信息到中台时需要删除下面报错同时实现组装逻辑
                    rt = Errno.ARGS_ERROR;
                    Log.logErr(rt,"args error;flow=%d;aid=%d;ownerTid=%d;productInfo=%s;", flow, aid, ownerTid, productInfo);
                    return rt;
                }
            }

            if(batchAddBasicInfoList.isEmpty()){
                rt = Errno.ERROR;
                Log.logErr(rt,"batchAddBasicInfoList empty;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                Log.logDbg("whalelog;flow=%d;aid=%d;ownerTid=%d;errProductList=%s;", flow, aid, ownerTid, errProductList);
                return rt;
            }

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);
            Set<Integer> alreadyExistsRlPdIdSet = new HashSet<>();
            if(!rlPdIdList.isEmpty()){
                FaiList<Param> alreadyExistsList = new FaiList<>();
                rt = productBasicProc.getRelListByRlIds(aid, ownerUnionPriId, rlPdIdList, alreadyExistsList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    Log.logErr(rt, "productBasicProc.getRelListByRlIds err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;rlPdIdList=%s;", aid, ownerTid, ownerUnionPriId, rlPdIdList);
                    return rt;
                }
                alreadyExistsRlPdIdSet = new HashSet<>(alreadyExistsList.size()*4/3+1);
                for (Param info : alreadyExistsList) {
                    int alreadyRlPdId = info.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID);
                    alreadyExistsRlPdIdSet.add(alreadyRlPdId);
                }
            }
            // 移除已经存在的商品
            if(!alreadyExistsRlPdIdSet.isEmpty()){
                for(Iterator<Param> iterator = batchAddBasicInfoList.iterator();iterator.hasNext();){
                    Param info = iterator.next();
                    if(alreadyExistsRlPdIdSet.contains(info.getInt(ProductRelEntity.Info.RL_PD_ID))){
                        iterator.remove();
                    }
                }
                for(Iterator<Param> iterator = productList.iterator();iterator.hasNext();){
                    Param product = iterator.next();
                    if(alreadyExistsRlPdIdSet.contains(product.getInt(MgProductEntity.Info.RL_PD_ID))){
                        product.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.REPEAT_IMPORT);
                        errProductList.add(product);
                        iterator.remove();
                    }
                }
            }

            if(batchAddBasicInfoList.isEmpty()){
                rt = Errno.ERROR;
                Log.logErr(rt,"batchAddBasicInfoList 2 empty;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                Log.logDbg("whalelog;flow=%d;aid=%d;ownerTid=%d;errProductList=%s;", flow, aid, ownerTid, errProductList);
                return rt;
            }

            FaiList<Param> idInfoList = new FaiList<>();
            // 批量添加商品数据
            rt = productBasicProc.batchAddProductAndRel(aid, ownerTid, ownerUnionPriId, batchAddBasicInfoList, idInfoList);
            if(rt != Errno.OK){
                for (Param product : productList) {
                    product.setInt(MgProductEntity.Info.ERRNO, rt);
                }
                errProductList.addAll(productList);
                Log.logErr(rt, "productBasicProc.batchAddProductAndRel err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;batchAddBasicInfoList=%s;", aid, ownerTid, ownerUnionPriId, batchAddBasicInfoList);
                return rt;
            }
            if(idInfoList.size() != batchAddBasicInfoList.size()){
                rt = Errno.ERROR;
                Log.logErr(rt,"size err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;idInfoList.size=%s;batchAddBasicInfoList.size=%s;", aid, ownerTid, ownerUnionPriId, idInfoList.size(), batchAddBasicInfoList.size());
                return rt;
            }
            Set<Integer> addedPdIdSet = new HashSet<>();
            if(!useMgProductBasicInfo){
                Map<Integer, Integer> ownerRlPdId_pdIdMap = new HashMap<>(idInfoList.size()*4/3+1);
                for (Param info : idInfoList) {
                    Integer rlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
                    Integer pdId = info.getInt(ProductRelEntity.Info.PD_ID);
                    addedPdIdSet.add(pdId);
                    ownerRlPdId_pdIdMap.put(rlPdId, pdId);
                }
                for (Param productInfo : productList) {
                    Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
                    int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                    int pdId = ownerRlPdId_pdIdMap.getOrDefault(rlPdId, 0);
                    if(pdId == 0){
                        rt = Errno.ERROR;
                        Log.logErr(rt,"basic add err;aid=%s;ownerTid=%s;ownerUnionPriId=%s;rlPdId=%s;", aid, ownerTid, ownerUnionPriId, rlPdId);
                        return rt;
                    }
                    productInfo.setInt(MgProductEntity.Info.PD_ID, pdId);
                    productInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdId);
                }
            }else{
                // TODO 业务方接入商品基础信息到中台时需要实现
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt,"args error;flow=%d;aid=%d;ownerTid=%d;", flow, aid, ownerTid);
                return rt;
            }
            Map<BizPriKey, Integer> bizPriKeyMap = new HashMap<>();
            // 绑定业务关联
            {
                FaiList<Param> batchBindPdRelList = new FaiList<>();
                for (Param productInfo : productList) {
                    FaiList<Param> storeSales = productInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                    if(Util.isEmptyList(storeSales)){
                        continue;
                    }
                    int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                    int ownerRlPdId = productInfo.getInt(MgProductEntity.Info.RL_PD_ID);

                    Map<Integer, Integer> unionPriIdRlPdIdMap = new HashMap<>();
                    // 先批量绑定关联
                    for (Param storeSale : storeSales) {
                        Integer tid = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.TID, ownerTid);
                        Integer siteId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, ownerSiteId);
                        Integer lgId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, ownerLgId);
                        Integer keepPriId1 = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, ownerKeepPriId1);
                        int rlPdId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.RL_PD_ID, ownerRlPdId);
                        BizPriKey bizPriKey = new BizPriKey(tid, siteId, lgId, keepPriId1);
                        Integer unionPriId = bizPriKeyMap.get(bizPriKey);
                        if(unionPriId == null){
                            unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
                            bizPriKeyMap.put(bizPriKey, unionPriId);
                        }
                        if(unionPriId != ownerUnionPriId){
                            unionPriIdRlPdIdMap.put(unionPriId, rlPdId);
                        }
                    }
                    if(!unionPriIdRlPdIdMap.isEmpty()){
                        FaiList<Param> bindPdRelList = new FaiList<>(unionPriIdRlPdIdMap.size());
                        for (Map.Entry<Integer, Integer> unionPriIdRlPdIdEntry : unionPriIdRlPdIdMap.entrySet()) {
                            int unionPriId = unionPriIdRlPdIdEntry.getKey();
                            int rlPdId = unionPriIdRlPdIdEntry.getValue();
                            bindPdRelList.add(
                                    new Param()
                                            .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                            .setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId)
                                            .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                            );
                        }
                        batchBindPdRelList.add(
                                new Param()
                                        .setInt(ProductRelEntity.Info.PD_ID, pdId)
                                        .setList(ProductRelEntity.Info.BIND_LIST, bindPdRelList)
                        );
                    }
                }
                if(!batchBindPdRelList.isEmpty()){
                    rt = productBasicProc.batchBindProductsRel(aid, ownerTid, batchBindPdRelList);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "batchBindProductsRel err;aid=%s;ownerTid=%s;batchBindPdRelList=%s", aid, ownerTid, batchBindPdRelList);
                        return rt;
                    }
                }
            }
            Log.logStd("begin;flow=%s;aid=%s;ownerTid=%s;ownerUnionPriId=%s;addedPdIdSet=%s;",flow, aid, ownerTid, ownerUnionPriId, addedPdIdSet);
            Map<Integer/*pdId*/, Map<String/*InPdScStrNameListJson*/, Long/*skuId*/>> pdIdInPdScStrNameListJsonSkuIdMap = new HashMap<>();
            Map<Integer/*pdId*/, Map<Long/*skuId*/, FaiList<Integer>/*inPdScStrIdList*/>> pdIdSkuIdInPdScStrIdMap = new HashMap<>();
            // 导入商品规格和商品规格sku
            {
                // 组装批量导入的数据
                FaiList<Param> importSpecList = new FaiList<>();
                FaiList<Param> importSpecSkuList = new FaiList<>();
                for (Param productInfo : productList) {
                    FaiList<Param> specList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC);
                    if(specList.isEmpty()){
                        continue;
                    }
                    int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                    for (Param specInfo : specList) {
                        Param importSpecInfo = new Param();
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.NAME, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.NAME);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.IN_PD_SC_VAL_LIST, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.IN_PD_SC_VAL_LIST);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.SORT, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.SORT);
                        importSpecInfo.assign(specInfo, ProductSpecEntity.SpecInfo.FLAG, fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.FLAG);
                        importSpecInfo.setInt(fai.MgProductSpecSvr.interfaces.entity.ProductSpecEntity.Info.PD_ID, pdId);
                        importSpecList.add(importSpecInfo);
                    }
                    FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
                    for (Param specSkuInfo : specSkuList) {
                        Param importSpecSkuInfo = new Param();
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST, ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST, ProductSpecSkuEntity.Info.SKU_CODE_LIST);
                        importSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                        // spu数据
                        importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SPU, ProductSpecSkuEntity.Info.SPU);
                        importSpecSkuList.add(importSpecSkuInfo);
                    }
                }
                FaiList<Param> skuIdInfoList = new FaiList<>();
                // 导入
                if(!importSpecList.isEmpty()){
                    rt = productSpecProc.importPdScWithSku(aid, ownerTid, ownerUnionPriId, importSpecList, importSpecSkuList, skuIdInfoList);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
                for (Param skuIdInfo : skuIdInfoList) {
                    int pdId = skuIdInfo.getInt(ProductSpecSkuEntity.Info.PD_ID);
                    Map<String, Long> inPdScStrNameListJsonSkuIdMap = pdIdInPdScStrNameListJsonSkuIdMap.get(pdId);
                    Map<Long, FaiList<Integer>> skuIdInPdScStrIdMap = pdIdSkuIdInPdScStrIdMap.get(pdId);
                    if(inPdScStrNameListJsonSkuIdMap == null){
                        inPdScStrNameListJsonSkuIdMap = new HashMap<>();
                        pdIdInPdScStrNameListJsonSkuIdMap.put(pdId, inPdScStrNameListJsonSkuIdMap);
                        skuIdInPdScStrIdMap = new HashMap<>();
                        pdIdSkuIdInPdScStrIdMap.put(pdId, skuIdInPdScStrIdMap);
                    }
                    int flag = skuIdInfo.getInt(ProductSpecSkuEntity.Info.FLAG, 0);
                    if(Misc.checkBit(flag, ProductSpecSkuValObj.FLag.SPU)){ // spu的数据跳过
                        continue;
                    }
                    long skuId = skuIdInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                    FaiList<String> inPdScStrNameList = skuIdInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                    String inPdScStrNameListJson = inPdScStrNameList.toJson();
                    inPdScStrNameListJsonSkuIdMap.put(inPdScStrNameListJson, skuId);

                    skuIdInPdScStrIdMap.put(skuId, skuIdInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST));
                }
            }
            // 导入库存销售sku信息并初始化库存
            {
                FaiList<Param> storeSaleSkuList = new FaiList<>();

                for (Param productInfo : productList) {
                    FaiList<Param> storeSales = productInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                    if(Util.isEmptyList(storeSales)){
                        continue;
                    }
                    int rlPdId = productInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                    int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                    Map<String, Long> inPdScStrNameListJsonSkuIdMap = pdIdInPdScStrNameListJsonSkuIdMap.get(pdId);
                    Map<Long, FaiList<Integer>> skuIdInPdScStrIdMap = pdIdSkuIdInPdScStrIdMap.get(pdId);
                    if(inPdScStrNameListJsonSkuIdMap == null){
                        Log.logStd("inPdScStrNameListJsonSkuIdMap empty;flow=%s;aid=%s;productInfo=%s;",flow, aid, productInfo);
                        continue;
                    }
                    Set<String> unionPriIdSkuIdSet = new HashSet<>();
                    for (Param storeSale : storeSales) {
                        FaiList<String> inPdScStrNameList = storeSale.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                        Long skuId = inPdScStrNameListJsonSkuIdMap.get(inPdScStrNameList.toJson());
                        if(skuId == null){
                            Log.logStd("skuId empty;flow=%s;aid=%s;productInfo=%s;inPdScStrNameList=%s;",flow, aid, productInfo, inPdScStrNameList);
                            continue;
                        }

                        FaiList<Integer> inPdScStrIdList = skuIdInPdScStrIdMap.get(skuId);

                        Integer tid = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.TID, ownerTid);
                        Integer siteId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, ownerSiteId);
                        Integer lgId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, ownerLgId);
                        Integer keepPriId1 = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, ownerKeepPriId1);
                        Integer unionPriId = bizPriKeyMap.get(new BizPriKey(tid, siteId, lgId, keepPriId1));

                        if(!unionPriIdSkuIdSet.add(unionPriId+"-"+skuId)){
                            Log.logStd("skuId already;flow=%s;aid=%s;unionPriId=%s;skuId=%s;rlPdId=%s;storeSale=%s;",flow, aid, unionPriId, skuId, rlPdId, storeSale);
                            continue;
                        }

                        Param importStoreSaleSkuInfo = new Param();
                        importStoreSaleSkuInfo.setLong(StoreSalesSkuEntity.Info.SKU_ID, skuId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.PD_ID, pdId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.SOURCE_UNION_PRI_ID, ownerUnionPriId);
                        importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.SKU_TYPE, StoreSalesSkuEntity.Info.SKU_TYPE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.SORT, StoreSalesSkuEntity.Info.SORT);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.COUNT, StoreSalesSkuEntity.Info.COUNT);
//                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.REMAIN_COUNT, StoreSalesSkuEntity.Info.REMAIN_COUNT);
//                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.HOLDING_COUNT, StoreSalesSkuEntity.Info.HOLDING_COUNT);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.PRICE, StoreSalesSkuEntity.Info.PRICE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.ORIGIN_PRICE, StoreSalesSkuEntity.Info.ORIGIN_PRICE);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.DURATION, StoreSalesSkuEntity.Info.DURATION);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.VIRTUAL_COUNT, StoreSalesSkuEntity.Info.VIRTUAL_COUNT);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.FLAG, StoreSalesSkuEntity.Info.FLAG);
                        importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.COST_PRICE, StoreSalesSkuEntity.Info.COST_PRICE);
                        importStoreSaleSkuInfo.setList(StoreSalesSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                        storeSaleSkuList.add(importStoreSaleSkuInfo);
                    }
                }
                // 导入
                if(!storeSaleSkuList.isEmpty()){
                    ProductStoreProc productStoreProc = new ProductStoreProc(flow);
                    rt = productStoreProc.importStoreSales(aid, ownerTid, ownerUnionPriId, null, storeSaleSkuList, inStoreRecordInfo);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }
            }
            Log.logStd("end;flow=%s;aid=%s;ownerTid=%s;ownerUnionPriId=%s;",flow, aid, ownerTid, ownerUnionPriId);
            rt = Errno.OK;
        }finally {
            try {
                FaiBuffer sendBuf = new FaiBuffer(true);
                errProductList.toBuffer(sendBuf, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
                session.write(rt, sendBuf);
            }finally {
                stat.end((rt != Errno.OK), rt);
            }
        }
        return rt;
    }
    private void filterExistsSkuCode(FaiList<Param> productList, HashSet<String> existsSkuCodeSet, FaiList<Param> errProductList){
        for(Iterator<Param> iterator = productList.iterator(); iterator.hasNext();){
            Param productInfo = iterator.next();
            FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
            boolean remove = false;
            tag:
            for (Param specSkuInfo : specSkuList) {
                if (existsSkuCodeSet.contains(specSkuInfo.getString(ProductSpecEntity.SpecSkuInfo.SKU_CODE))) {
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                    remove = true;
                    break tag;
                }
                FaiList<String> skuCodeList = specSkuInfo.getListNullIsEmpty(ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST);
                for (String skuCode : skuCodeList) {
                    if(existsSkuCodeSet.contains(skuCode)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                        remove = true;
                        break tag;
                    }
                }
            }
            if(remove){
                errProductList.add(productInfo);
                iterator.remove();
            }
        }
    }
    private void checkImportProductList(FaiList<Param> productList, HashSet<String> skuCodeSet, FaiList<Param> errProductList, boolean useMgProductBasicInfo) {
        for(Iterator<Param> iterator = productList.iterator(); iterator.hasNext();){
            Param productInfo = iterator.next();
            Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
            if(Str.isEmpty(basicInfo)){
                errProductList.add(productInfo);
                productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.BASIC_IS_EMPTY);
                iterator.remove();
                continue;
            }
            if(!useMgProductBasicInfo){
                int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                if(rlPdId <= 0){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.BASIC_IS_EMPTY);
                    errProductList.add(productInfo);
                    iterator.remove();
                    continue;
                }
            }
            boolean remove = false;
            FaiList<Param> specList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC);
            tag:
            for (Param spec : specList) {
                String specName = spec.getString(ProductSpecEntity.SpecInfo.NAME);
                if(!ProductSpecCheck.Spec.checkName(specName)){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SPEC_NAME_LEN_LIMIT);
                    remove = true;
                    break tag;
                }
                int flag = spec.getInt(ProductSpecEntity.SpecInfo.FLAG, 0);
                FaiList<Param> inPdScValList = spec.getListNullIsEmpty(ProductSpecEntity.SpecInfo.IN_PD_SC_VAL_LIST);
                if(inPdScValList.isEmpty()){
                    if(!Misc.checkBit(flag, ProductSpecValObj.Spec.FLag.ALLOW_IN_PD_SC_VAL_LIST_IS_EMPTY)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.IN_PD_SC_VAL_LIST_IS_EMPTY);
                        remove = true;
                        break tag;
                    }
                }else{
                    Set<String> valNameSet = new HashSet<>(inPdScValList.size()*4/3+1);
                    for (Param inPdScVal : inPdScValList) {
                        String name = inPdScVal.getString(ProductSpecValObj.Spec.InPdScValList.Item.NAME);
                        if(!ProductSpecCheck.Spec.checkName(name)){
                            productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SPEC_VAL_NAME_LEN_LIMIT);
                            remove = true;
                            break tag;
                        }
                        if(valNameSet.contains(name)){
                            productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SPEC_VAL_NAME_REPEAT);
                            remove = true;
                            break tag;
                        }
                        valNameSet.add(name);
                    }
                }
            }
            if(remove){
                errProductList.add(productInfo);
                iterator.remove();
                continue;
            }

            FaiList<Param> specSkuList = productInfo.getListNullIsEmpty(MgProductEntity.Info.SPEC_SKU);
            remove = false;
            tag:
            for (Param specSkuInfo : specSkuList) {
                String skuCode = specSkuInfo.getString(ProductSpecEntity.SpecSkuInfo.SKU_CODE);
                if(skuCodeSet.contains(skuCode)){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                    remove = true;
                    break tag;
                }
                if(!ProductSpecCheck.SpecSku.checkSkuCode(skuCode)){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_LEN_LIMIT);
                    remove = true;
                    break tag;
                }
                if(skuCode != null){
                    skuCodeSet.add(skuCode);
                }
                FaiList<String> skuCodeList = specSkuInfo.getListNullIsEmpty(ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST);
                if(skuCodeList.size() > ProductSpecValObj.SpecSku.Limit.SKU_CODE_MAX_SIZE){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_SIZE_LIMIT);
                    remove = true;
                    break tag;
                }
                for (String tmpSkuCode : skuCodeList) {
                    if(skuCodeSet.contains(tmpSkuCode)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_ALREADY_EXISTS);
                        remove = true;
                        break tag;
                    }
                    if(!ProductSpecCheck.SpecSku.checkSkuCode(tmpSkuCode)){
                        productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.SKU_CODE_LEN_LIMIT);
                        remove = true;
                        break tag;
                    }
                }
                skuCodeSet.addAll(skuCodeList);
            }
            if(remove){
                errProductList.add(productInfo);
                iterator.remove();
                continue;
            }
        }
    }

    protected Map<Integer, BizPriKey> toUnionPriIdBizPriKeyMap(FaiList<Param> primaryKeyList) {
        Map<Integer, BizPriKey> unionPriIdBizPriKeyMap = new HashMap<>(primaryKeyList.size()*4/3+1);
        toUnionPriIdBizPriKeyMap(primaryKeyList, unionPriIdBizPriKeyMap);
        return unionPriIdBizPriKeyMap;
    }

    protected void toUnionPriIdBizPriKeyMap(FaiList<Param> primaryKeyList, Map<Integer, BizPriKey> unionPriIdBizPriKeyMap) {
        for (Param primaryKeyInfo : primaryKeyList) {
            Integer tmpUnionPriId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
            Integer tmpTid = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.TID);
            Integer tmpSiteId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            Integer tmpLgId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.LGID);
            Integer tmpKeepPriId1 = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
            unionPriIdBizPriKeyMap.put(tmpUnionPriId, new BizPriKey(tmpTid, tmpSiteId, tmpLgId, tmpKeepPriId1));
        }
    }

    protected Map<BizPriKey, Integer> toBizPriKeyUnionPriIdMap(FaiList<Param> primaryKeyList) {
        Map<BizPriKey, Integer> bizPriKeyUnionPriIdMap = new HashMap<>(primaryKeyList.size()*4/3+1);

        for (Param primaryKeyInfo : primaryKeyList) {
            Integer tmpUnionPriId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
            Integer tmpTid = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.TID);
            Integer tmpSiteId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            Integer tmpLgId = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.LGID);
            Integer tmpKeepPriId1 = primaryKeyInfo.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
            bizPriKeyUnionPriIdMap.put(new BizPriKey(tmpTid, tmpSiteId, tmpLgId, tmpKeepPriId1), tmpUnionPriId);
        }

        return bizPriKeyUnionPriIdMap;
    }
}
