package fai.MgProductInfSvr.application.service;

import fai.MgBackupSvr.interfaces.cli.MgBackupCli;
import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelValObj;
import fai.MgProductGroupSvr.interfaces.cli.MgProductGroupCli;
import fai.MgProductInfSvr.application.MgProductInfSvr;
import fai.MgProductInfSvr.domain.comm.BizPriKey;
import fai.MgProductInfSvr.domain.comm.ProductSpecCheck;
import fai.MgProductInfSvr.domain.entity.RichTextConverter;
import fai.MgProductInfSvr.domain.serviceproc.*;
import fai.MgProductInfSvr.interfaces.dto.MgProductDto;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
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
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextEntity;
import fai.MgRichTextInfSvr.interfaces.entity.MgRichTextValObj;
import fai.comm.fseata.client.core.exception.TransactionException;
import fai.comm.fseata.client.tm.GlobalTransactionContext;
import fai.comm.fseata.client.tm.api.GlobalTransaction;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.middleground.app.CloneDef;
import fai.comm.util.*;
import fai.mgproduct.comm.MgProductErrno;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.service.ServicePub;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ????????????????????????service???????????????
 */
public class MgProductInfService extends ServicePub {

    /**
     * ????????????????????????
     */
    protected Param getBackupInfo(int flow, int aid, int tid, int siteId, int rlBackupId) {
        int rt = Errno.ERROR;
        MgBackupCli cli = new MgBackupCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgBackupCli error");
        }

        Param info = new Param();
        rt = cli.getBackupInfo(aid, tid, siteId, rlBackupId, info);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getBackupInfo error;flow=%d;aid=%d;tid=%d;rlBackupId=%d;", flow, aid, tid, rlBackupId);
        }
        return info;
    }

    /**
     * ??????unionPriId?????????rt
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
     * ??????unionPriId, ????????????unionPriId, ?????????????????????
     */
    protected int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) {
        int rt;
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

    protected Param getByUnionPriId(int flow, int aid, int unionPriId) {
        int rt;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgPrimaryKeyCli error");
        }
        Param info = new Param();
        rt = cli.getByUnionPriId(unionPriId, info);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getByUnionPriId error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }
        return info;
    }

    /**
     * ??????unionPriId
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
     * ??????unionPriIdList ??????????????????
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
    /**
     * ??????unionPriIdList ??????????????????
     * @param tid
     * @param unionPriIds
     */
    protected FaiList<Param> getPrimaryKeyListByUnionPriIds(int flow, int aid, int tid, FaiList<Integer> unionPriIds) {
        FaiList<Param> list = new FaiList<>();
        if(Utils.isEmptyList(unionPriIds)) {
            return list;
        }
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            throw new MgException(rt, "init MgPrimaryKeyCli error");
        }

        rt = cli.getListByUnionPriIds(unionPriIds, list);
        if(rt != Errno.OK) {
            throw new MgException(rt, "getListByUnionPriIds error;flow=%d;aid=%d;tid=%d;unionPriIds=%s;", flow, aid, tid, unionPriIds);
        }
        return list;
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

    protected int getPdIdWithAdd(int flow, int aid, int tid, int siteId, int unionPriId, int sysType, int rlPdId, String xid, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, siteId, unionPriId, sysType, rlPdId, idRef, true, xid);
    }
    /**
     * ??????PdId
     */
    protected int getPdId(int flow, int aid, int tid, int siteId, int unionPriId, int sysType, int rlPdId, Ref<Integer> idRef) {
        return getPdId(flow, aid, tid, siteId, unionPriId, sysType, rlPdId, idRef, false, null);
    }

    protected int getPdId(int flow, int aid, int tid, int siteId, int unionPriId, int sysType, int rlPdId, Ref<Integer> idRef, boolean withAdd, String xid) {
        int rt = Errno.ERROR;
        MgProductBasicCli mgProductBasicCli = new MgProductBasicCli(flow);
        if(!mgProductBasicCli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgProductBasicCli error");
            return rt;
        }

        Param pdRelInfo = new Param();
        rt = mgProductBasicCli.getRelInfoByRlId(aid, unionPriId, sysType, rlPdId, pdRelInfo);
        if(rt != Errno.OK) {
            // sysType???0???????????????
            if(withAdd && (rt == Errno.NOT_FOUND) && sysType == 0){
                if(xid == null) {
                    xid = "";
                }
                rt = mgProductBasicCli.addProductAndRel(aid, tid, siteId, unionPriId, xid, new Param()
                                .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                .setInt(ProductRelEntity.Info.SYS_TYPE, sysType)
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
     * ??????
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

        // ???????????????????????????????????????????????? ?????????????????????
        FaiList<Param> list = getPrimaryKeyListWithOutAdd(flow, aid, primaryKeys);
        Map<BizPriKey, Integer> bizPriKeyUnionPriIdMap = toBizPriKeyUnionPriIdMap(list);

        Integer tid = null;
        Integer siteId = null;

        FaiList<Param> cloneUnionPriIds = new FaiList<>();
        for(Param info : clonePrimaryKeys) {
            Param fromPrimary = info.getParam(CloneDef.Info.FROM_PRIMARY_KEY);
            int fromTid = fromPrimary.getInt(MgPrimaryKeyEntity.Info.TID);
            int fromSiteId = fromPrimary.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            int fromLgId = fromPrimary.getInt(MgPrimaryKeyEntity.Info.LGID);
            int fromKeepPriId1 = fromPrimary.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
            Integer fromUnionPriId = bizPriKeyUnionPriIdMap.get(new BizPriKey(fromTid, fromSiteId, fromLgId, fromKeepPriId1));
            // ??????fromUnionPriId????????????????????????????????????????????????
            if(fromUnionPriId == null) {
                continue;
            }
            Param toPrimary = info.getParam(CloneDef.Info.TO_PRIMARY_KEY);
            int toTid = toPrimary.getInt(MgPrimaryKeyEntity.Info.TID);
            int toSiteId = toPrimary.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            int toLgId = toPrimary.getInt(MgPrimaryKeyEntity.Info.LGID);
            int toKeepPriId1 = toPrimary.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);

            tid = tid == null ? toTid : tid;
            siteId = siteId == null ? toSiteId : tid;
            if(tid != toTid || siteId != toSiteId) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "only clone same tid and siteId;flow=%d;aid=%d;key=%s;clonePrimaryKeys=%s;cloneOption=%s;", flow, aid, primaryKey, clonePrimaryKeys, cloneOption);
                return rt;
            }

            // ????????????cli???????????????????????????????????????????????????????????????cli???????????????????????????????????????????????????
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

        // ???????????????
        boolean cloneAll = Str.isEmpty(cloneOption);

        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.GROUP, false)) {
            ProductGroupProc groupProc = new ProductGroupProc(flow);
            groupProc.cloneData(aid, fromAid, cloneUnionPriIds);
        }

        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.BASIC, false)) {
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            basicProc.cloneData(aid, tid, siteId, fromAid, cloneUnionPriIds);
        }

        // ???????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.LIB, false)) {
            ProductLibProc libProc = new ProductLibProc(flow);
            libProc.cloneData(aid, fromAid, cloneUnionPriIds);
        }
        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.TAG, false)) {
            ProductTagProc tagProc = new ProductTagProc(flow);
            tagProc.cloneData(aid, fromAid, cloneUnionPriIds);
        }
        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.PROP, false)) {
            ProductPropProc propProc = new ProductPropProc(flow);
            propProc.cloneData(aid, fromAid, cloneUnionPriIds);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("clone ok;flow=%d;aid=%d;key=%s;clonePrimaryKeys=%s;cloneOption=%s;", flow, aid, primaryKey, clonePrimaryKeys, cloneOption);
        return rt;
    }

    /**
     * ????????????
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


        // ???????????????
        boolean cloneAll = Str.isEmpty(cloneOption);

        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.GROUP, false)) {
            ProductGroupProc groupProc = new ProductGroupProc(flow);
            groupProc.incrementalClone(aid, unionPriId, fromAid, fromUnionPriId);
        }

        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.BASIC, false)) {
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            basicProc.incrementalClone(aid, tid, siteId, unionPriId, fromAid, fromUnionPriId);
        }

        // ???????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.LIB, false)) {
            ProductLibProc libProc = new ProductLibProc(flow);
            libProc.incrementalClone(aid, unionPriId, fromAid, fromUnionPriId);
        }
        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.TAG, false)) {
            ProductTagProc tagProc = new ProductTagProc(flow);
            tagProc.incrementalClone(aid, unionPriId, fromAid, fromUnionPriId);
        }
        // ??????????????????
        if(cloneAll || cloneOption.getBoolean(MgProductEntity.Option.PROP, false)) {
            ProductPropProc propProc = new ProductPropProc(flow);
            propProc.incrementalClone(aid, unionPriId, fromAid, fromUnionPriId);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd(rt, "incrementalClone ok;flow=%d;aid=%d;primaryKey=%s;fromPrimaryKey=%s;cloneOption=%s;", flow, aid, primaryKey, fromPrimaryKey, cloneOption);
        return rt;
    }

    /**
     * ??????????????????????????????
     * for ???????????????????????????
     */
    @SuccessRt(Errno.OK)
    public int cloneBizBind(FaiSession session, int flow, int aid, Param primaryKey, Param fromPrimaryKey) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || Str.isEmpty(fromPrimaryKey)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, primaryKeys is empty;flow=%d;aid=%d;primaryKey=%s;fromPrimaryKey=%s;", flow, aid, primaryKey, fromPrimaryKey);
            return rt;
        }

        int fromTid = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        int tid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        if(fromTid != FaiValObj.TermId.YK || tid != FaiValObj.TermId.YK) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, tid is not YK;flow=%d;aid=%d;primaryKey=%s;fromPrimaryKey=%s;", flow, aid, primaryKey, fromPrimaryKey);
            return rt;
        }

        int fromSiteId = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
        int fromLgId = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
        int fromKeepPriId1 = fromPrimaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
        int fromUnionPriId = getUnionPriIdWithoutAdd(flow, aid, fromTid, fromSiteId, fromLgId, fromKeepPriId1);

        int siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
        int lgId = primaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
        int keepPriId1 = primaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
        int toUnionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        // ???????????????????????????????????????????????????????????????????????????????????????
        // ?????????????????????????????????????????????
        boolean silentDel = tid == FaiValObj.TermId.YK;

        // ????????????????????????
        ProductBasicProc basicProc = new ProductBasicProc(flow);
        basicProc.cloneBizBind(aid, fromUnionPriId, toUnionPriId, silentDel);

        // ????????????????????????
        ProductStoreProc storeProc = new ProductStoreProc(flow);
        storeProc.cloneBizBind(aid, fromUnionPriId, toUnionPriId);

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd(rt, "cloneBizBind ok;flow=%d;aid=%d;primaryKey=%s;fromPrimaryKey=%s;", flow, aid, primaryKey, fromPrimaryKey);
        return rt;
    }

    /**
     * ??????
     */
    @SuccessRt(Errno.OK)
    public int backupData(FaiSession session, int flow, int aid, Param primaryKey, FaiList<Param> backupPrimaryKeys, int rlBackupId) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || Utils.isEmptyList(backupPrimaryKeys) || rlBackupId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;key=%s;backupPrimaryKeys=%s;rlBackupId=%s;", flow, aid, primaryKey, backupPrimaryKeys, rlBackupId);
            return rt;
        }
        int tid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        int siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);

        // ??????unionPriId
        FaiList<Param> list = getPrimaryKeyListWithOutAdd(flow, aid, backupPrimaryKeys);
        FaiList<Integer> unionPriIds = new FaiList<>();
        for(Param info : list) {
            unionPriIds.add(info.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID));
        }

        // ????????????????????????
        Param backupInfo = getBackupInfo(flow, aid, tid, siteId, rlBackupId);

        // ??????????????????
        ProductGroupProc groupProc = new ProductGroupProc(flow);
        groupProc.backupData(aid, unionPriIds, backupInfo);

        // ??????????????????
        ProductBasicProc basicProc = new ProductBasicProc(flow);
        basicProc.backupData(aid, unionPriIds, backupInfo);

        // ???????????????
        ProductLibProc libProc = new ProductLibProc(flow);
        libProc.backupData(aid, unionPriIds, backupInfo);
        // ??????????????????
        ProductTagProc tagProc = new ProductTagProc(flow);
        tagProc.backupData(aid, unionPriIds, backupInfo);
        // ??????????????????
        ProductPropProc propProc = new ProductPropProc(flow);
        propProc.backupData(aid, unionPriIds, backupInfo);

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("backup ok;flow=%d;aid=%d;key=%s;backupPrimaryKeys=%s;rlBackupId=%s;", flow, aid, primaryKey, backupPrimaryKeys, rlBackupId);
        return rt;
    }

    /**
     * ????????????
     */
    @SuccessRt(Errno.OK)
    public int restoreBackupData(FaiSession session, int flow, int aid, Param primaryKey, FaiList<Param> restorePrimaryKeys, int restoreId, int rlBackupId, Param restoreOption) throws IOException {
        int rt;
        if(Str.isEmpty(primaryKey) || Utils.isEmptyList(restorePrimaryKeys) || rlBackupId <= 0 || restoreId <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;key=%s;restorePrimaryKeys=%s;rlBackupId=%s;restoreId=%s;", flow, aid, primaryKey, restorePrimaryKeys, rlBackupId, restoreId);
            return rt;
        }
        int tid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        int siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);

        // ??????unionPriId
        FaiList<Param> list = getPrimaryKeyListWithOutAdd(flow, aid, restorePrimaryKeys);
        FaiList<Integer> unionPriIds = new FaiList<>();
        for(Param info : list) {
            unionPriIds.add(info.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID));
        }

        // ????????????????????????
        Param backupInfo = getBackupInfo(flow, aid, tid, siteId, rlBackupId);

        // ????????????????????????
        boolean restoreAll = Str.isEmpty(restoreOption);

        // ??????????????????
        if(restoreAll || restoreOption.getBoolean(MgProductEntity.Option.GROUP, false)) {
            ProductGroupProc groupProc = new ProductGroupProc(flow);
            groupProc.restoreBackup(aid, unionPriIds, restoreId, backupInfo);
        }

        // ??????????????????
        if(restoreAll || restoreOption.getBoolean(MgProductEntity.Option.BASIC, false)) {
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            basicProc.restoreBackup(aid, unionPriIds, restoreId, backupInfo);
        }

        // ???????????????
        if (restoreAll || restoreOption.getBoolean(MgProductEntity.Option.LIB, false)) {
            ProductLibProc libProc = new ProductLibProc(flow);
            libProc.restoreBackup(aid, unionPriIds, restoreId, backupInfo);
        }
        // ??????????????????
        if (restoreAll || restoreOption.getBoolean(MgProductEntity.Option.LIB, false)) {
            ProductTagProc tagProc = new ProductTagProc(flow);
            tagProc.restoreBackup(aid, unionPriIds, restoreId, backupInfo);
        }
        // ??????????????????
        if (restoreAll || restoreOption.getBoolean(MgProductEntity.Option.PROP, false)) {
            ProductPropProc propProc = new ProductPropProc(flow);
            propProc.restoreBackup(aid, unionPriIds, restoreId, backupInfo);
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("restore ok;flow=%d;aid=%d;key=%s;restoreKeys=%s;rlBackupId=%s;", flow, aid, primaryKey, restorePrimaryKeys, rlBackupId);
        return rt;
    }

    /**
     * ????????????
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
        int siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);

        // ????????????????????????
        Param backupInfo = getBackupInfo(flow, aid, tid, siteId, rlBackupId);

        // ????????????????????????
        ProductGroupProc groupProc = new ProductGroupProc(flow);
        groupProc.delBackup(aid, backupInfo);

        // ????????????????????????
        ProductBasicProc basicProc = new ProductBasicProc(flow);
        basicProc.delBackup(aid, backupInfo);

        // ?????????????????????
        ProductLibProc libProc = new ProductLibProc(flow);
        libProc.delBackup(aid, backupInfo);
        // ????????????????????????
        ProductTagProc tagProc = new ProductTagProc(flow);
        tagProc.delBackup(aid, backupInfo);
        // ????????????????????????
        ProductPropProc propProc = new ProductPropProc(flow);
        propProc.delBackup(aid, backupInfo);

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd("del backup ok;flow=%d;aid=%d;key=%s;rlBackupId=%s;", flow, aid, primaryKey, rlBackupId);
        return rt;
    }

    @SuccessRt({Errno.OK, Errno.NOT_FOUND})
    public int getPdInfo4ES(FaiSession session, int flow, int aid, int unionPriId, int pdId) throws IOException {
        int rt;
        if(aid < 0 || unionPriId < 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error;flow=%d;aid=%d;uid=%s;pdId=%s;", flow, aid, unionPriId, pdId);
            return rt;
        }
        ProductBasicProc productBasicProc = new ProductBasicProc(flow);
        Param info = new Param();
        rt = productBasicProc.getInfoByPdId(aid, unionPriId, pdId, info);
        if(rt != Errno.OK) {
            return rt;
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        info.toBuffer(sendBuf, MgProductDto.Key.INFO, MgProductDto.getEsPdInfoDto());
        session.write(sendBuf);
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
     * ?????????????????????????????????
     * @param tid ???????????????tid
     * @param siteId ???????????????siteId
     * @param lgId ???????????????lgId
     * @param keepPriId1 ???????????????keepPriId1
     * @param rlPdId ????????????id
     */
    public int getProductFullInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int sysType, int rlPdId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // ??????unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);

            // 1 ????????????????????????
            Param pdInfo = new Param();
            rt = productBasicProc.getProductInfo(aid, unionPriId, sysType, rlPdId, pdInfo);
            if(rt != Errno.OK){
                return rt;
            }
            int pdId = pdInfo.getInt(ProductRelEntity.Info.PD_ID);
            int sourceUnionPriId = pdInfo.getInt(ProductBasicEntity.ProductInfo.SOURCE_UNIONPRIID);
            Param primary = getByUnionPriId(flow, aid, sourceUnionPriId);
            int sourceTid = primary.getInt(MgPrimaryKeyEntity.Info.TID);
            int sourceSiteId = primary.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
            int sourceLgId = primary.getInt(MgPrimaryKeyEntity.Info.LGID);
            int sourceKeepPriId = primary.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
            // 1.1 ???????????????, ????????????sourceUnionPriId??????
            RichTextProc richProc = new RichTextProc(flow);
            FaiList<Param> richTexts = richProc.getPdRichText(aid, sourceTid, sourceSiteId, sourceLgId, sourceKeepPriId, pdId);
            for(Param richText : richTexts) {
                int richType = richText.getInt(MgRichTextEntity.Info.TYPE);
                String content = richText.getString(MgRichTextEntity.Info.CONTENT);
                pdInfo.setString(RichTextConverter.getKey(richType), content);
            }
            // 3.1 ??????????????????
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            // ??????????????????
            FaiList<Param> pdScInfoList = new FaiList<>();
            rt = productSpecProc.getPdScInfoList(aid, tid, unionPriId, pdId, pdScInfoList, false);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            // ??????????????????sku
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            rt = productSpecProc.getPdSkuScInfoList(aid, tid, unionPriId, pdId, true, pdScSkuInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            // 3.2 ????????????????????????
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> pdScSkuSalesStoreInfoList = new FaiList<>();
            rt = productStoreProc.getSkuStoreSalesByPdId(aid, tid, pdId, pdScSkuSalesStoreInfoList);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            FaiList<Param> spuSalesStoreInfoList = new FaiList<>();
            rt = productStoreProc.getSpuBizSummaryInfoListByPdIdList(aid, tid, unionPriId, Utils.asFaiList(pdId), spuSalesStoreInfoList, null);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                return rt;
            }
            Param spuInfo = new Param();
            if(!spuSalesStoreInfoList.isEmpty()) {
                spuInfo = spuSalesStoreInfoList.get(0);
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
            productInfo.setParam(MgProductEntity.Info.BASIC, pdInfo);
            productInfo.setList(MgProductEntity.Info.SPEC, pdScInfoList);
            productInfo.setList(MgProductEntity.Info.SPEC_SKU, pdScSkuInfoList);
            productInfo.setList(MgProductEntity.Info.STORE_SALES, pdScSkuSalesStoreInfoList);
            productInfo.setParam(MgProductEntity.Info.SPU_SALES, spuInfo);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            productInfo.toBuffer(sendBuf, MgProductDto.Key.INFO, MgProductDto.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
        return rt;
    }

    public int getProductList4Adm(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int sysType, FaiList<Integer> rlPdIds, Param combined) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // ??????unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);

            // 1 ????????????????????????
            FaiList<Param> pdRelInfos = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, unionPriId, sysType, rlPdIds, pdRelInfos);
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
            // 2 ????????????????????????
            // 3 ... ???????????????????????? ... ???
            // 3.1 ??????????????????
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            // ??????????????????
            Map<Integer, List<Param>> pdScInfoMap = new HashMap<>();
            if(getSpec) {
                FaiList<Param> pdScInfoList = new FaiList<>();
                rt = productSpecProc.getPdScInfoList4Adm(aid, unionPriId, pdIds, false, pdScInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScInfoMap = pdScInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecEntity.SpecInfo.PD_ID)));
            }

            // ??????????????????sku
            Map<Integer, List<Param>> pdScSkuInfoMap = new HashMap<>();
            if(getSpecSku) {
                FaiList<Param> pdScSkuInfoList = new FaiList<>();
                rt = productSpecProc.getPdSkuInfoList4Adm(aid, tid, pdIds, true, pdScSkuInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScSkuInfoMap = pdScSkuInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecSkuEntity.Info.PD_ID)));
            }

            // 3.2 ????????????????????????
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
            // 3.2.2 spu??????????????????
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

    public int getProductSummary4Adm(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int sysType, FaiList<Integer> rlPdIds, Param getKeys) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            // ??????unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc productBasicProc = new ProductBasicProc(flow);

            // 1 ????????????????????????
            FaiList<Param> pdRelInfos = new FaiList<>();
            rt = productBasicProc.getRelListByRlIds(aid, unionPriId, sysType, rlPdIds, pdRelInfos);
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
            // 2 ????????????????????????
            // 3 ... ???????????????????????? ... ???
            // 3.1 ??????????????????
            ProductSpecProc productSpecProc = new ProductSpecProc(flow);
            // ??????????????????
            Map<Integer, List<Param>> pdScInfoMap = new HashMap<>();
            if(getSpec) {
                FaiList<Param> pdScInfoList = new FaiList<>();
                rt = productSpecProc.getPdScInfoList4Adm(aid, unionPriId, pdIds, false, pdScInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScInfoMap = pdScInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecEntity.SpecInfo.PD_ID)));
            }
            // ??????????????????sku
            Map<Integer, List<Param>> pdScSkuInfoMap = new HashMap<>();
            if(getSpecSku) {
                FaiList<Param> pdScSkuInfoList = new FaiList<>();
                rt = productSpecProc.getPdSkuInfoList4Adm(aid, tid, pdIds, true, pdScSkuInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                pdScSkuInfoMap = pdScSkuInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(ProductSpecSkuEntity.Info.PD_ID)));
            }

            // 3.2 ????????????????????????
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            // 3.2.1 sku??????????????????
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
            // 3.2.2 spu??????????????????
            Map<Integer, List<Param>> spuSalesStoreInfoMap = new HashMap<>();
            if(getSpuSales) {
                FaiList<Param> spuSalesStoreInfoList = new FaiList<>();
                rt = productStoreProc.getSpuSummaryInfoList(aid, tid, unionPriId, pdIds, spuSalesStoreInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
                spuSalesStoreInfoMap = spuSalesStoreInfoList.stream().collect(Collectors.groupingBy(info->info.getInt(SpuSummaryEntity.Info.PD_ID)));
            }

            // ????????????
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

    /**
     * ???????????????????????????
     * for ???????????????????????????????????????????????????????????????????????????
     * ???????????????????????????top ?????? ????????????????????????????????????
     */
    @SuccessRt(Errno.OK)
    public int batchSet4YK(FaiSession session, int flow, int aid, Param ownPrimaryKey, int sysType, FaiList<Integer> rlPdIds, FaiList<Param> primaryKeys, ParamUpdater basicUpdater) throws IOException, TransactionException {
        int rt;
        if(Str.isEmpty(ownPrimaryKey) || Utils.isEmptyList(rlPdIds) || Utils.isEmptyList(primaryKeys)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, primaryKeys is empty;flow=%d;aid=%d;ownPrimaryKey=%s;sysType=%s;rlPdIds=%s;primaryKeys=%s;updater=%s;", flow, aid, ownPrimaryKey, sysType, rlPdIds, primaryKeys, basicUpdater);
            return rt;
        }

        if((basicUpdater == null || basicUpdater.isEmpty())) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, update is empty;flow=%d;aid=%d;ownPrimaryKey=%s;sysType=%s;rlPdIds=%s;primaryKeys=%s;updater=%s;", flow, aid, ownPrimaryKey, sysType, rlPdIds, primaryKeys, basicUpdater);
            return rt;
        }

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

        int tid = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        int siteId = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
        int lgId = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
        int keepPriId1 = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
        int ownUnionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        boolean commit = false;
        GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
        tx.begin(aid, 60000, "mgProduct-batchSet4YK", flow);
        String xid = tx.getXid();
        try {
            ProductBasicProc basicProc = new ProductBasicProc(flow);

            FaiList<Param> addList = basicProc.batchSet4YK(aid, xid, ownUnionPriId, sysType, unionPriIds, rlPdIds, basicUpdater);
            if(!Utils.isEmptyList(addList)) {
                ProductStoreProc storeProc = new ProductStoreProc(flow);
                // ??????????????????????????????????????????????????????????????????????????????
                storeProc.copyBizBind(aid, xid, ownUnionPriId, addList);
            }

            // ???????????????????????????????????????????????????????????????
            boolean restoreSoftDelProduct = false;
            // ?????????????????????????????????????????????????????????
            boolean softDel = false;
            Param updaterInfo = basicUpdater.getData();
            if(!Str.isEmpty(updaterInfo)) {
                Integer status = updaterInfo.getInt(ProductBasicEntity.ProductInfo.STATUS);
                if(status != null) {
                    if(status != ProductBasicValObj.ProductValObj.Status.DEL) {
                        restoreSoftDelProduct = true;
                    }else {
                        softDel = true;
                    }
                }
            }

            // ??????????????????
            if(restoreSoftDelProduct) {
                FaiList<Param> restoreList = new FaiList<>();
                for(int unionPriId : unionPriIds) {
                    for(int rlPdId : rlPdIds) {
                        Param info = new Param();
                        info.setInt(StoreSalesSkuEntity.Info.UNION_PRI_ID, unionPriId);
                        info.setInt(StoreSalesSkuEntity.Info.RL_PD_ID, rlPdId);
                        info.setInt(StoreSalesSkuEntity.Info.SYS_TYPE, sysType);
                        restoreList.add(info);
                    }
                }
                ProductStoreProc storeProc = new ProductStoreProc(flow);
                rt = storeProc.restoreSoftDelBizPd(aid, xid, restoreList);
                if(rt != Errno.OK) {
                    Log.logErr("restoreSoftDelBizPd err;aid=%d;ownPrimaryKey=%s;sysType=%s;rlPdIds=%s;primaryKeys=%s;updater=%s;", aid, ownPrimaryKey, sysType, rlPdIds, primaryKeys, basicUpdater);
                    return rt;
                }
            }

            if(softDel) {
                ProductStoreProc storeProc = new ProductStoreProc(flow);
                rt = storeProc.batchDelBizPdStoreSales(aid, unionPriIds, sysType, rlPdIds, xid, true);
                if(rt != Errno.OK) {
                    return rt;
                }
            }

            commit = true;
        }finally {
            if(commit) {
                tx.commit();
            }else {
                tx.rollback();
            }
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd(rt, "setPdStatus ok;flow=%d;aid=%d;ownPrimaryKey=%s;sysType=%s;rlPdIds=%s;primaryKeys=%s;updater=%s;", flow, aid, ownPrimaryKey, sysType, rlPdIds, primaryKeys, basicUpdater);
        return rt;
    }

    /**
     *  ??????????????????????????????
     */
    @SuccessRt(Errno.OK)
    public int batchSetBizBind(FaiSession session, int flow, int aid, Param ownPrimaryKey, int sysType, int rlPdId, FaiList<Param> primaryKeys, Param combinedUpdate) throws IOException, TransactionException {
        int rt;
        if(Str.isEmpty(ownPrimaryKey) || Utils.isEmptyList(primaryKeys)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, primaryKeys is empty;flow=%d;aid=%d;ownPrimaryKey=%s;sysType=%s;rlPdIds=%s;primaryKeys=%s;updater=%s;", flow, aid, ownPrimaryKey, sysType, rlPdId, primaryKeys, combinedUpdate);
            return rt;
        }

        if(Str.isEmpty(combinedUpdate)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, update is empty;flow=%d;aid=%d;ownPrimaryKey=%s;sysType=%s;rlPdIds=%s;primaryKeys=%s;updater=%s;", flow, aid, ownPrimaryKey, sysType, rlPdId, primaryKeys, combinedUpdate);
            return rt;
        }

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

        int tid = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
        int siteId = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
        int lgId = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
        int keepPriId1 = ownPrimaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
        int ownUnionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

        boolean commit = false;
        GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
        tx.begin(aid, 60000, "mgProduct-batchSetBizBind", flow);
        String xid = tx.getXid();
        try {
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            ProductStoreProc storeProc = new ProductStoreProc(flow);

            ParamUpdater basicUpdater = (ParamUpdater) combinedUpdate.getObject(MgProductEntity.Info.BASIC);
            if(basicUpdater != null && !basicUpdater.isEmpty()) {
                basicProc.batchSet4YK(aid, xid, ownUnionPriId, sysType, unionPriIds, Utils.asFaiList(rlPdId), basicUpdater);
            }

            // ?????? pdId
            Ref<Integer> idRef = new Ref<>();
            rt = getPdId(flow, aid, tid, siteId, ownUnionPriId, sysType, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            FaiList<ParamUpdater> storeUpdaters = combinedUpdate.getList(MgProductEntity.Info.STORE_SALES);

            /** ?????????????????? start */
            if (!Utils.isEmptyList(storeUpdaters)) {
                // ????????????????????? unionPriId ????????????????????? unionPriIdList?????????????????????????????????????????????????????? sku ?????????????????????????????????????????? -112 ??????
                FaiList<Integer> softDelUnionPriIdList = basicProc.getSoftDelUnionPriIdList(aid, pdId, unionPriIds);

                Map<FaiList<String>, Param> inPdScStrNameInfoMap = new HashMap<>();
                for (ParamUpdater storeUpdater : storeUpdaters) {
                    Param storeInfo = storeUpdater.getData();

                    Long skuId = storeInfo.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                    if(skuId == null){
                        FaiList<String> inPdScStrNameList = storeInfo.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                        if(inPdScStrNameList == null){
                            return rt = Errno.ARGS_ERROR;
                        }
                        inPdScStrNameInfoMap.put(inPdScStrNameList, storeInfo);
                    }
                }
                if(inPdScStrNameInfoMap.size() > 0){
                    ProductSpecProc productSpecProc = new ProductSpecProc(flow);
                    FaiList<Param> infoList = new FaiList<Param>();
                    rt = productSpecProc.getPdSkuScInfoList(aid, tid, ownUnionPriId, pdId, false, infoList);
                    if(rt != Errno.OK) {
                        return rt;
                    }
                    for (Param info : infoList) {
                        FaiList<String> inPdScStrNameList = info.getList(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST);
                        Param storeInfo = inPdScStrNameInfoMap.remove(inPdScStrNameList);
                        if(storeInfo == null){
                            continue;
                        }
                        storeInfo.assign(info, ProductSpecEntity.SpecSkuInfo.SKU_ID, ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                    }
                    if(inPdScStrNameInfoMap.size() > 0){
                        Log.logErr("args error, updaterList is err;flow=%d;aid=%d;tid=%d;inPdScStrNameInfoMap=%s;", flow, aid, tid, inPdScStrNameInfoMap);
                        return rt = Errno.ARGS_ERROR;
                    }
                }

                rt = storeProc.batchSetSkuStoreSales(aid, xid, tid, ownUnionPriId, unionPriIds, pdId, rlPdId, sysType, storeUpdaters, softDelUnionPriIdList);
                if(rt != Errno.OK) {
                    return rt;
                }

                // 2022/1/15 spu ??????????????? storeSales ??????, ?????????????????? batchSetSkuStoreSales ????????????????????????????????????????????????????????????????????? spuBiz ???????????????????????? spu ???????????????????????????????????????
                ParamUpdater spuUpdater = (ParamUpdater) combinedUpdate.getObject(MgProductEntity.Info.SPU_SALES);
                if(spuUpdater != null && !spuUpdater.isEmpty()) {
                    storeProc.batchSetSpuBizSummary(aid, xid, unionPriIds, Utils.asFaiList(pdId), spuUpdater);
                }
            }

            commit = true;
        }finally {
            if(commit) {
                tx.commit();
            }else {
                tx.rollback();
            }
        }

        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        session.write(sendBuf);
        Log.logStd(rt, "batchSetBizBind ok;flow=%d;aid=%d;ownPrimaryKey=%s;sysType=%s;rlPdId=%s;primaryKeys=%s;updater=%s;", flow, aid, ownPrimaryKey, sysType, rlPdId, primaryKeys, combinedUpdate);
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

            // ??????unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // ????????????????????????
            // 1. ????????????????????????????????????
            // 2. ????????????????????????????????????
            // 3. ????????????????????????????????????
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            rt = basicProc.clearRelData(aid, unionPriId, false);
            if(rt != Errno.OK) {
                return rt;
            }

            // ??????????????????
            // ??????????????????????????????????????????
            ProductPropProc propProc = new ProductPropProc(flow);
            rt = propProc.clearRelData(aid, unionPriId);
            if(rt != Errno.OK) {
                return rt;
            }

            // ????????????????????????
            // 1. ??????unionPriId???????????????????????????
            // 2. ??????unionPriId???????????????????????????????????????????????????
            // 3. ??????unionPriId??????sku??????????????????
            // 4. ??????unionPriId??????spu????????????????????????
            // 5. ????????????aid???sku???spu????????????????????????
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
                // ?????????????????????????????????
                if(tmpTid == null || tmpTid != FaiValObj.TermId.YK) {
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("args error, only supports yk del;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                    return rt;
                }
            }

            // ??????unionPriId
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

            // ????????????????????????????????????
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            rt = basicProc.clearAcct(aid, unionPriIds);
            if(rt != Errno.OK) {
                return rt;
            }

            // ??????????????????????????????
            ProductPropProc propProc = new ProductPropProc(flow);
            rt = propProc.clearAcct(aid, unionPriIds);
            if(rt != Errno.OK) {
                return rt;
            }

            // ????????????????????????
            ProductGroupProc groupProc = new ProductGroupProc(flow);
            rt = groupProc.clearAcct(aid, unionPriIds);
            if (rt != Errno.OK) {
                return rt;
            }

            // ?????????????????????
            RichTextProc richTextProc = new RichTextProc(flow);
            rt = richTextProc.clearAcct(aid, tid, siteId, lgId, keepPriId1, primaryKeys);
            if (rt != Errno.OK) {
                return rt;
            }

            // ??????????????????
            ProductSpecProc specProc = new ProductSpecProc(flow);
            rt = specProc.clearAcct(aid, unionPriIds);
            if(rt != Errno.OK) {
                return rt;
            }

            // ????????????????????????
            // 1. ??????unionPriId???????????????????????????
            // 2. ??????unionPriId???????????????????????????????????????????????????
            // 3. ??????unionPriId??????sku??????????????????
            // 4. ??????unionPriId??????spu????????????????????????
            // 5. ??????aid???sourceUnionPriId?????????unionPriIds???sku???spu????????????????????????
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
     * ????????????
     * @param ownerTid ??????????????? ownerTid
     * @param ownerSiteId ??????????????? ownerSiteId
     * @param ownerLgId ??????????????? ownerLgId
     * @param ownerKeepPriId1 ??????????????? ownerKeepPriId1
     * @param xid
     * @param productList ??????????????????
     * @param inStoreRecordInfo ??????????????????
     * @param useMgProductBasicInfo ??????????????????????????????????????????
     */
    public int importProduct(FaiSession session, int flow, int aid, int ownerTid, int ownerSiteId, int ownerLgId, int ownerKeepPriId1, int sysType, String xid, FaiList<Param> productList, Param inStoreRecordInfo, boolean useMgProductBasicInfo) throws IOException, TransactionException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        FaiList<Param> errProductList = new FaiList<>();
        Set<Param> needRemoveParamSet = new HashSet<>();
        FaiList<Integer> idList = new FaiList<>(productList.size());
        // ?????? idList ??????????????????????????????????????????, ???????????? rlPdId ?????? idList
        for (int i = 0; i < productList.size(); i++) {
            idList.add(0);
        }
        try {
            if(Utils.isEmptyList(productList)){
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
            // ??????unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, ownerTid, ownerSiteId, ownerLgId, ownerKeepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int ownerUnionPriId = idRef.value;

            GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
            tx.begin(aid, 60000, "mgProduct-importProduct", flow);
            xid = tx.getXid();
            boolean commit = false;
            try {
                HashSet<String> skuCodeSet = new HashSet<>();
                // ??????????????????????????????????????????????????????????????????
                checkImportProductList(productList, skuCodeSet, errProductList, idList, needRemoveParamSet, useMgProductBasicInfo);

                ProductSpecProc productSpecProc = new ProductSpecProc(flow);
                skuCodeSet.removeAll(Collections.singletonList(null)); // ????????????null???
                if(!skuCodeSet.isEmpty()){ // ?????? skuCode ??????????????????
                    Ref<FaiList<String>> existsSkuCodeListRef = new Ref<>();
                    rt = productSpecProc.getExistsSkuCodeList(aid, ownerTid, ownerUnionPriId, new FaiList<>(skuCodeSet), existsSkuCodeListRef);
                    if(rt != Errno.OK){
                        return rt;
                    }
                    HashSet<String> existsSkuCodeSet = new HashSet<>(existsSkuCodeListRef.value);
                    // ?????????????????????skuCode???????????????
                    filterExistsSkuCode(productList, existsSkuCodeSet, idList, needRemoveParamSet, errProductList);
                }

                // ?????? productList ??????????????????
                productList.removeAll(needRemoveParamSet);

                // ???????????????????????????????????????
                FaiList<Param> batchAddBasicInfoList = new FaiList<>(productList.size());
                FaiList<Integer> rlPdIdList = new FaiList<>();
                for (Param productInfo : productList) {
                    Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
                    basicInfo.setInt(ProductRelEntity.Info.SYS_TYPE, sysType);
                    // ??? rlGroupIds???rlTagIds???rlProps ????????????????????????????????????????????????????????????
                    productInfo.setList(ProductBasicEntity.ProductInfo.RL_GROUP_IDS, basicInfo.getListNullIsEmpty(ProductBasicEntity.ProductInfo.RL_GROUP_IDS));
                    productInfo.setList(ProductBasicEntity.ProductInfo.RL_TAG_IDS, basicInfo.getListNullIsEmpty(ProductBasicEntity.ProductInfo.RL_TAG_IDS));
                    productInfo.setList(ProductBasicEntity.ProductInfo.RL_PROPS, basicInfo.getListNullIsEmpty(ProductBasicEntity.ProductInfo.RL_PROPS));
                    if(!useMgProductBasicInfo){
                        int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                        batchAddBasicInfoList.add(
                                new Param().setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                        .setInt(ProductRelEntity.Info.SYS_TYPE, sysType)
                                        .setBoolean(ProductRelEntity.Info.INFO_CHECK, false)
                        );
                        productInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdId);
                        rlPdIdList.add(rlPdId);
                    }else{
                        batchAddBasicInfoList.add(basicInfo);
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
                    rt = productBasicProc.getRelListByRlIds(aid, ownerUnionPriId, sysType, rlPdIdList, alreadyExistsList);
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
                // ???????????????????????????
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
                // ????????????????????????
                rt = productBasicProc.batchAddProductAndRel(aid, xid, ownerTid, ownerSiteId, ownerUnionPriId, batchAddBasicInfoList, idInfoList);
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

                    for (int i = 0; i < productList.size(); i++) {
                        Param idInfo = idInfoList.get(i);
                        Integer rlPdId = idInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
                        Integer pdId = idInfo.getInt(ProductRelEntity.Info.PD_ID);
                        Param productInfo = productList.get(i);
                        productInfo.setInt(MgProductEntity.Info.PD_ID, pdId);
                        productInfo.setInt(MgProductEntity.Info.RL_PD_ID, rlPdId);

                        // ????????????????????????id ?????? idList ???
                        for (int j = 0; j < idList.size(); j++) {
                            int id = idList.get(j);
                            if (id == 0) {
                                idList.set(j, rlPdId);
                                break;
                            }
                        }
                    }
                }

                Map<BizPriKey, Integer> bizPriKeyMap = new HashMap<>();
                // ??????????????????
                {
                    FaiList<Param> batchBindPdRelList = new FaiList<>();
                    // ???????????????????????????????????? unionPriId + rlPdId ?????????????????? ???????????? status ?????? -1 ???????????????????????????????????? -1
                    HashMap<String, Boolean> softDelMap = new HashMap<>();
                    for (Param productInfo : productList) {
                        FaiList<Param> storeSales = productInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                        if(Utils.isEmptyList(storeSales)){
                            continue;
                        }
                        int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                        int ownerRlPdId = productInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                        FaiList<Integer> rlGroupIds = productInfo.getListNullIsEmpty(ProductBasicEntity.ProductInfo.RL_GROUP_IDS);
                        FaiList<Integer> rlTagIds = productInfo.getListNullIsEmpty(ProductBasicEntity.ProductInfo.RL_TAG_IDS);
                        FaiList<Param> rlProps = productInfo.getListNullIsEmpty(ProductBasicEntity.ProductInfo.RL_PROPS);

                        Map<Integer, Integer> unionPriIdRlPdIdMap = new HashMap<>();
                        // ?????????????????????
                        for (Param storeSale : storeSales) {
                            Integer tid = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.TID, ownerTid);
                            Integer siteId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, ownerSiteId);
                            Integer lgId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, ownerLgId);
                            Integer keepPriId1 = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, ownerKeepPriId1);
                            int rlPdId = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.RL_PD_ID, ownerRlPdId);
                            int status = storeSale.getInt(ProductStoreEntity.StoreSalesSkuInfo.STATUS, ProductRelValObj.Status.DOWN);
                            BizPriKey bizPriKey = new BizPriKey(tid, siteId, lgId, keepPriId1);
                            Integer unionPriId = bizPriKeyMap.get(bizPriKey);
                            if(unionPriId == null){
                                unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
                                bizPriKeyMap.put(bizPriKey, unionPriId);
                            }
                            if(unionPriId != ownerUnionPriId){
                                unionPriIdRlPdIdMap.put(unionPriId, rlPdId);
                                // ?????? unionPriId + rlPdId ????????????????????????????????????
                                softDelMap.computeIfAbsent(unionPriId + "-" + rlPdId, map -> status == ProductRelValObj.Status.DEL);
                                Boolean softDel = softDelMap.get(unionPriId + "-" + rlPdId);
                                softDelMap.put(unionPriId + "-" + rlPdId, softDel & status == ProductRelValObj.Status.DEL);
                            }
                        }
                        if(!unionPriIdRlPdIdMap.isEmpty()){
                            FaiList<Param> bindPdRelList = new FaiList<>(unionPriIdRlPdIdMap.size());
                            for (Map.Entry<Integer, Integer> unionPriIdRlPdIdEntry : unionPriIdRlPdIdMap.entrySet()) {
                                int unionPriId = unionPriIdRlPdIdEntry.getKey();
                                int rlPdId = unionPriIdRlPdIdEntry.getValue();
                                Param bindPdRelInfo = new Param()
                                        .setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId)
                                        .setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId)
                                        .setList(ProductBasicEntity.ProductInfo.RL_GROUP_IDS, rlGroupIds)
                                        .setList(ProductBasicEntity.ProductInfo.RL_TAG_IDS, rlTagIds)
                                        .setList(ProductBasicEntity.ProductInfo.RL_PROPS, rlProps)
                                        .setBoolean(ProductRelEntity.Info.INFO_CHECK, false);
                                if (softDelMap.get(unionPriId + "-" + rlPdId)) {
                                    bindPdRelInfo.setInt(ProductRelEntity.Info.STATUS, ProductRelValObj.Status.DEL);
                                }
                                bindPdRelList.add(bindPdRelInfo);
                            }
                            batchBindPdRelList.add(
                                    new Param()
                                            .setInt(ProductRelEntity.Info.PD_ID, pdId)
                                            .setList(ProductRelEntity.Info.BIND_LIST, bindPdRelList)
                            );
                        }
                    }
                    if(!batchBindPdRelList.isEmpty()){
                        rt = productBasicProc.batchBindProductsRel(aid, xid, ownerTid, true, batchBindPdRelList);
                        if(rt != Errno.OK){
                            Log.logErr(rt, "batchBindProductsRel err;aid=%s;ownerTid=%s;batchBindPdRelList=%s", aid, ownerTid, batchBindPdRelList);
                            return rt;
                        }
                    }
                }
                Log.logStd("begin;flow=%s;aid=%s;ownerTid=%s;ownerUnionPriId=%s;addedPdIdSet=%s;",flow, aid, ownerTid, ownerUnionPriId, addedPdIdSet);
                Map<Integer/*pdId*/, Map<String/*InPdScStrNameListJson*/, Long/*skuId*/>> pdIdInPdScStrNameListJsonSkuIdMap = new HashMap<>();
                Map<Integer/*pdId*/, Map<Long/*skuId*/, FaiList<Integer>/*inPdScStrIdList*/>> pdIdSkuIdInPdScStrIdMap = new HashMap<>();
                // ?????????????????????????????????sku
                {
                    // ???????????????????????????
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
                            // ??????????????????sort?????????flag??????
                            importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST, ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                            importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SKU_CODE_LIST, ProductSpecSkuEntity.Info.SKU_CODE_LIST);
                            importSpecSkuInfo.setInt(ProductSpecSkuEntity.Info.PD_ID, pdId);
                            // spu??????
                            importSpecSkuInfo.assign(specSkuInfo, ProductSpecEntity.SpecSkuInfo.SPU, ProductSpecSkuEntity.Info.SPU);
                            importSpecSkuList.add(importSpecSkuInfo);
                        }
                    }
                    FaiList<Param> skuIdInfoList = new FaiList<>();
                    // ??????
                    if(!importSpecList.isEmpty()){
                        rt = productSpecProc.importPdScWithSku(aid, ownerTid, ownerUnionPriId, xid, importSpecList, importSpecSkuList, skuIdInfoList);
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
                        if(Misc.checkBit(flag, ProductSpecSkuValObj.FLag.SPU)){ // spu???????????????
                            continue;
                        }
                        long skuId = skuIdInfo.getLong(ProductSpecSkuEntity.Info.SKU_ID);
                        FaiList<String> inPdScStrNameList = skuIdInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_NAME_LIST);
                        String inPdScStrNameListJson = inPdScStrNameList.toJson();
                        inPdScStrNameListJsonSkuIdMap.put(inPdScStrNameListJson, skuId);

                        skuIdInPdScStrIdMap.put(skuId, skuIdInfo.getList(ProductSpecSkuEntity.Info.IN_PD_SC_STR_ID_LIST));
                    }
                }
                // ??????spu??????
                {
                    FaiList<Param> importSpuSales = new FaiList<>();
                    for (Param productInfo : productList) {
                        int rlPdId = productInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                        int pdId = productInfo.getInt(MgProductEntity.Info.PD_ID);
                        FaiList<Param> spuSalesList = productInfo.getList(MgProductEntity.Info.SPU_SALES);
                        if (spuSalesList == null || spuSalesList.isEmpty()) {
                            continue;
                        }

                        for (Param spuSales : spuSalesList) {
                            Integer tid = spuSales.getInt(ProductStoreEntity.StoreSalesSkuInfo.TID, ownerTid);
                            Integer siteId = spuSales.getInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, ownerSiteId);
                            Integer lgId = spuSales.getInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, ownerLgId);
                            Integer keepPriId1 = spuSales.getInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, ownerKeepPriId1);
                            Integer unionPriId = bizPriKeyMap.get(new BizPriKey(tid, siteId, lgId, keepPriId1));

                            // ?????????????????? unionPriId ?????????????????????????????????????????? ???unionPriId
                            if (unionPriId == null) {
                                unionPriId = ownerUnionPriId;
                            }

                            spuSales.setInt(SpuBizSummaryEntity.Info.AID, aid);
                            spuSales.setInt(SpuBizSummaryEntity.Info.UNION_PRI_ID, unionPriId);
                            spuSales.setInt(SpuBizSummaryEntity.Info.SOURCE_UNION_PRI_ID, ownerUnionPriId);
                            spuSales.setInt(SpuBizSummaryEntity.Info.PD_ID, pdId);
                            spuSales.setInt(SpuBizSummaryEntity.Info.RL_PD_ID, rlPdId);

                            // 2022/03/07 ?????? sysType
                            spuSales.setInt(SpuBizSummaryEntity.Info.SYS_TYPE, sysType);

                            importSpuSales.add(spuSales);
                        }
                    }
                    if (!importSpuSales.isEmpty()) {
                        ProductStoreProc productStoreProc = new ProductStoreProc(flow);
                        rt = productStoreProc.batchAddSpuBizSummary(aid, xid, importSpuSales);
                        if (rt != Errno.OK) {
                            return rt;
                        }
                    }
                }

                // ??????????????????sku????????????????????????
                {
                    FaiList<Param> storeSaleSkuList = new FaiList<>();

                    for (Param productInfo : productList) {
                        FaiList<Param> storeSales = productInfo.getListNullIsEmpty(MgProductEntity.Info.STORE_SALES);
                        if(Utils.isEmptyList(storeSales)){
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
                            importStoreSaleSkuInfo.setInt(StoreSalesSkuEntity.Info.SYS_TYPE, sysType);
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
                            importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.STATUS, StoreSalesSkuEntity.Info.STATUS);
                            importStoreSaleSkuInfo.assign(storeSale, ProductStoreEntity.StoreSalesSkuInfo.WEIGHT, StoreSalesSkuEntity.Info.WEIGHT);
                            importStoreSaleSkuInfo.setList(StoreSalesSkuEntity.Info.IN_PD_SC_STR_ID_LIST, inPdScStrIdList);
                            storeSaleSkuList.add(importStoreSaleSkuInfo);
                        }
                    }
                    // ??????
                    if(!storeSaleSkuList.isEmpty()){
                        ProductStoreProc productStoreProc = new ProductStoreProc(flow);
                        rt = productStoreProc.importStoreSales(aid, ownerTid, ownerUnionPriId, sysType, xid, storeSaleSkuList, inStoreRecordInfo);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }
                }
                commit = true;
                tx.commit();
            } finally {
                if (!commit) {
                    tx.rollback();
                }
            }
            Log.logStd("end;flow=%s;aid=%s;ownerTid=%s;ownerUnionPriId=%s;",flow, aid, ownerTid, ownerUnionPriId);
            rt = Errno.OK;
        } catch (Exception e) {
            // ????????????????????????finally???????????????Errno.OK
            if (rt == Errno.OK) {
                rt = Errno.ERROR;
            }
            throw e;
        } finally {
            // ???????????????????????????????????????????????????????????????????????? errProductList
            try {
                FaiBuffer sendBuf = new FaiBuffer(true);
                errProductList.toBuffer(sendBuf, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
                idList.toBuffer(sendBuf, ProductBasicDto.Key.RL_PD_IDS);
                session.write(rt, sendBuf);
            } finally {
                stat.end((rt != Errno.OK), rt);
            }
        }
        return rt;
    }

    /**
     * ??????????????????
     * @param session session
     * @param flow flow
     * @param aid aid
     * @param primaryKey ????????????
     * @param rlPdIds ????????????id??????
     * @param sysType sysType
     * @return {@link Errno}
     */
    public int restoreData(FaiSession session, int flow, int aid, Param primaryKey, FaiList<Integer> rlPdIds, int sysType) throws IOException, TransactionException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {

            int tid = primaryKey.getInt(MgProductEntity.Info.TID);
            // todo ?????????????????????????????????????????????????????????
            if (tid != FaiValObj.TermId.YK) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "Not supported by current business;flow=%d;aid=%d;tid=%d", flow, aid, tid);
                return rt;
            }
            int siteId = primaryKey.getInt(MgProductEntity.Info.SITE_ID);
            int lgId = primaryKey.getInt(MgProductEntity.Info.LGID);
            int keepPriId1 = primaryKey.getInt(MgProductEntity.Info.KEEP_PRI_ID1);

            int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);

            // ??????????????????
            GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
            tx.begin(aid, 60000, "mgProduct-importProduct", flow);
            String xid = tx.getXid();
            boolean commit = false;
            try {
                // ??????????????????
                ProductBasicProc basicProc = new ProductBasicProc(flow);
                FaiList<Integer> pdIds = new FaiList<>();
                basicProc.restoreData(aid, unionPriId, xid, rlPdIds, sysType, pdIds);

                if (pdIds.isEmpty()) {
                    throw new MgException(rt, "not found pdIds;flow=%d;aid=%d;", flow, aid);
                }

                // ??????????????????????????????
                ProductSpecProc specProc = new ProductSpecProc(flow);
                specProc.restoreData(aid, xid, pdIds);

                // ????????????????????????
                ProductStoreProc storeProc = new ProductStoreProc(flow);
                storeProc.restoreData(aid, xid, pdIds);

                commit = true;
                tx.commit();
            } finally {
                if (!commit) {
                    tx.rollback();
                }
            }
            Log.logStd("restore data ok;aid=%d;unionPriId=%d;rlPdIds=%s;", aid, unionPriId, rlPdIds);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            return rt;
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
    }

    @SuccessRt(value = { Errno.OK, Errno.NOT_FOUND })
    public int getProductListByUidsAndRlPdIdsFromDb(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds, int sysType, FaiList<Param> primaryKeys, Param combined) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        FaiList<Param> list;
        try {
            // todo ????????? rlPdId ??? unionPriId ???????????????

            // unionPriId ????????? tid siteId lgId keepPriId1
            Map<Integer, Param> unionPriId_primaryKeyMap = new HashMap<>(primaryKeys.size() + 1);
            FaiList<Integer> unionPriIdList = new FaiList<>();
            int unionPriId = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1);
            unionPriIdList.add(unionPriId);
            unionPriId_primaryKeyMap.put(unionPriId,  new Param().setInt(MgProductEntity.Info.TID, tid).setInt(MgProductEntity.Info.SITE_ID, siteId)
                    .setInt(MgProductEntity.Info.LGID, lgId).setInt(MgProductEntity.Info.KEEP_PRI_ID1, keepPriId1));
            Ref<Integer> idRef = new Ref<>();
            for (Param primaryKey : primaryKeys) {
                int curTid = primaryKey.getInt(MgProductEntity.Info.TID);
                int curSiteId = primaryKey.getInt(MgProductEntity.Info.SITE_ID);
                int curLgId = primaryKey.getInt(MgProductEntity.Info.LGID);
                int curKeepPriId1 = primaryKey.getInt(MgProductEntity.Info.KEEP_PRI_ID1);
                rt = getUnionPriId(flow, aid, curTid, curSiteId, curLgId, curKeepPriId1, idRef);
                if (rt != Errno.OK) {
                    return rt;
                }
                unionPriId_primaryKeyMap.put(idRef.value, new Param().setInt(MgProductEntity.Info.TID, curTid).setInt(MgProductEntity.Info.SITE_ID, curSiteId)
                        .setInt(MgProductEntity.Info.LGID, curLgId).setInt(MgProductEntity.Info.KEEP_PRI_ID1, curKeepPriId1));
                unionPriIdList.add(idRef.value);
            }

            boolean getSpec = combined.getBoolean(MgProductEntity.Info.SPEC, false);
            boolean getSpecSku = combined.getBoolean(MgProductEntity.Info.SPEC_SKU, false);
            boolean getStoreSales = combined.getBoolean(MgProductEntity.Info.STORE_SALES, false);
            boolean getSpuSales = combined.getBoolean(MgProductEntity.Info.SPU_SALES, false);

            ProductBasicProc basicProc = new ProductBasicProc(flow);
            // 1.??????????????????
            FaiList<Param> basicList = basicProc.getListByUidsAndRlPdIds(aid, unionPriIdList, rlPdIds, sysType);
            if (Utils.isEmptyList(basicList)) {
                return Errno.NOT_FOUND;
            }

            Set<Integer> pdIdSet = new HashSet<>(Utils.getValList(basicList, ProductBasicEntity.ProductInfo.PD_ID));

            // 2???todo ???????????????????????????????????????

            // 3???todo ?????????????????????????????????sku

            // 4.????????????????????????
            ProductStoreProc productStoreProc = new ProductStoreProc(flow);
            FaiList<Param> pdScSkuSalesStoreInfoList = new FaiList<>();
            if (getStoreSales) {
                rt = productStoreProc.getStoreSalesByPdIdsAndUIdList(aid, tid, new FaiList<>(pdIdSet), unionPriIdList, pdScSkuSalesStoreInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }
            FaiList<Param> spuSalesStoreInfoList = new FaiList<>();
            if (getSpuSales) {
                rt = productStoreProc.getSpuBizSummaryInfoListByPdIdListAndUidList(aid, tid, unionPriIdList, new FaiList<>(pdIdSet), spuSalesStoreInfoList);
                if(rt != Errno.OK && rt != Errno.NOT_FOUND){
                    return rt;
                }
            }

            // ????????????
            list = assignInfoList(flow, aid, basicList, null, null, null, pdScSkuSalesStoreInfoList, spuSalesStoreInfoList, unionPriId_primaryKeyMap);
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, MgProductDto.Key.INFO_LIST, MgProductDto.getInfoDto());
            session.write(sendBuf);
            Log.logStd("getProductListByUidsAndRlPdIdsFromDb ok;flow=%d;aid=%d;uids=%s;rlPdIds=%s", flow, aid, unionPriIdList, rlPdIds);
            return rt;
        } finally {
            stat.end((rt != Errno.OK) && (rt != Errno.NOT_FOUND), rt);
        }
    }

    /**
     * ?????? id Map ?????? pdId ???????????????????????????????????????????????????????????????
     */
    @SuccessRt(Errno.OK)
    public int incCloneRichText(FaiSession session, int flow, int aid, int toSysType, int fromSysType, int fromAid, Param primaryKey, Param fromPrimaryKey, FaiList<Param> idMap) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || Str.isEmpty(primaryKey) || Str.isEmpty(fromPrimaryKey) || Utils.isEmptyList(idMap)) {
                rt = Errno.ARGS_ERROR;
                throw new MgException(rt, "args err;aid=%d;primaryKey=%s;fromPrimaryKey=%s;idMap=%s", aid, primaryKey, fromPrimaryKey, idMap);
            }

            // ??????????????????
            Integer toTid = primaryKey.getInt(MgProductEntity.Info.TID);
            Integer toSiteId = primaryKey.getInt(MgProductEntity.Info.SITE_ID);
            Integer toLgId = primaryKey.getInt(MgProductEntity.Info.LGID);
            Integer toKeepPriId1 = primaryKey.getInt(MgProductEntity.Info.KEEP_PRI_ID1);
            int toUnionPriId = getUnionPriId(flow, aid, toTid, toSiteId, toLgId, toKeepPriId1);

            Integer fromTid = fromPrimaryKey.getInt(MgProductEntity.Info.TID);
            Integer fromSiteId = fromPrimaryKey.getInt(MgProductEntity.Info.SITE_ID);
            Integer fromLgId = fromPrimaryKey.getInt(MgProductEntity.Info.LGID);
            Integer fromKeepPriId1 = fromPrimaryKey.getInt(MgProductEntity.Info.KEEP_PRI_ID1);
            int fromUnionPriId = getUnionPriId(flow, aid, fromTid, fromSiteId, fromLgId, fromKeepPriId1);

            // ?????? rlPdId ??? pdId ???????????????
            ProductBasicProc basicProc = new ProductBasicProc(flow);
            FaiList<Param> toRlPdIdAndPdIdList = new FaiList<>();
            FaiList<Param> fromRlPdIdAndPdIdList = new FaiList<>();
            rt = basicProc.getRlPdIdAndPdIdMap(aid, fromAid, toUnionPriId, fromUnionPriId, toSysType, fromSysType, idMap, toRlPdIdAndPdIdList, fromRlPdIdAndPdIdList);
            if (rt != Errno.OK) {
                Log.logErr(rt, "getRlPdIdAndPdIdMap error;flow=%d;aid=%d;fromAid=%d;toUnionPriId=%d;fromUnionPriId=%d;toSysType=%d;fromSysType=%d;rlPdIdMap=%s;", flow, aid, fromAid, toUnionPriId, fromUnionPriId, toSysType, fromSysType, idMap);
                return rt;
            }

            // ?????? pdId ???????????????
            Map<Integer, Integer> toRlPdIdMap = new HashMap<>(toRlPdIdAndPdIdList.size());
            for (Param toInfo : toRlPdIdAndPdIdList) {
                Integer rlPdId = toInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                Integer pdId = toInfo.getInt(MgProductEntity.Info.PD_ID);
                toRlPdIdMap.put(rlPdId, pdId);
            }

            Map<Integer, Integer> fromRlPdIdMap = new HashMap<>(fromRlPdIdAndPdIdList.size());
            for (Param fromInfo : fromRlPdIdAndPdIdList) {
                Integer rlPdId = fromInfo.getInt(MgProductEntity.Info.RL_PD_ID);
                Integer pdId = fromInfo.getInt(MgProductEntity.Info.PD_ID);
                fromRlPdIdMap.put(rlPdId, pdId);
            }

            // ??? pdId ????????????????????? list ???????????? ?????????????????????????????????
            FaiList<Param> pdIdMapList = new FaiList<>();
            for (Param info : idMap) {
                Integer toRlPdId = info.getInt(MgProductEntity.Info.TO_RL_PD_ID);
                Integer fromRlPdId = info.getInt(MgProductEntity.Info.FROM_RL_PD_ID);

                Integer toPdId = toRlPdIdMap.get(toRlPdId);
                Integer fromPdId = fromRlPdIdMap.get(fromRlPdId);

                if (toPdId == null || fromPdId == null) {
                    rt = Errno.ERROR;
                    Log.logErr(rt, "get pdId err;flow=%d;aid=%d;fromAid=%d;toUnionPriId=%d;fromUnionPriId=%d;toSysType=%d;fromSysType=%d;toRlPdId=%d;fromRlPdId=%d;", flow, aid, fromAid, toUnionPriId, fromUnionPriId, toSysType, fromSysType, toRlPdId, fromRlPdId);
                    return rt;
                }

                pdIdMapList.add(new Param().setInt(MgProductEntity.Info.TO_RL_ID, toPdId).setInt(MgProductEntity.Info.FROM_RL_ID, fromPdId));
            }

            // ?????????????????????????????????
            RichTextProc richTextProc = new RichTextProc(flow);
            richTextProc.incCloneRichText(aid, toTid, toSiteId, toLgId, toKeepPriId1, MgRichTextValObj.Biz.PRODUCT, fromAid, fromPrimaryKey, MgRichTextValObj.Biz.PRODUCT, pdIdMapList);

            rt = Errno.OK;
            session.write(new FaiBuffer(true));
            Log.logStd("incCloneRichText ok;flow=%d;aid=%d;fromAid=%d;pdIdMapList=%s", flow, aid, fromAid, pdIdMapList);
            return rt;
        } finally {
            stat.end(rt != Errno.OK, rt);
        }
    }

    private FaiList<Param> assignInfoList(int flow, int aid, FaiList<Param> basicList, FaiList<Param> richTexts, FaiList<Param> pdScInfoList, FaiList<Param> pdScSkuInfoList, FaiList<Param> pdScSkuSalesStoreInfoList, FaiList<Param> spuSalesStoreInfoList, Map<Integer, Param> unionPriId_primaryKeyMap) {
        FaiList<Param> list = new FaiList<>();
        if (Utils.isEmptyList(basicList)) {
            return list;
        }
        /* uid_pdId */
        Map<String, List<Param>> storeSalesMap = new HashMap<>();
        if (!Utils.isEmptyList(pdScSkuSalesStoreInfoList)) {
             storeSalesMap = pdScSkuSalesStoreInfoList.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductStoreEntity.StoreSalesSkuInfo.UNION_PRI_ID) + "_" + x.getInt(ProductStoreEntity.StoreSalesSkuInfo.PD_ID)));
        }
        /* uid_pdId */
        Map<String, List<Param>> spuSalesMap = new HashMap<>();
        if (!Utils.isEmptyList(spuSalesStoreInfoList)) {
            spuSalesMap = spuSalesStoreInfoList.stream().collect(Collectors.groupingBy(x -> x.getInt(ProductStoreEntity.SpuBizSummaryInfo.UNION_PRI_ID) + "_" + x.getInt(ProductStoreEntity.SpuBizSummaryInfo.PD_ID)));
        }

        for (Param pdInfo : basicList) {
            Param info = new Param();
            // 1?????? unionPriId ????????? tid siteId lgId keepPriId1
            Integer unionPriId = pdInfo.getInt(ProductBasicEntity.ProductInfo.UNION_PRI_ID);
            Param primaryKey = unionPriId_primaryKeyMap.get(unionPriId);
            if (primaryKey == null) {
                throw new MgException(Errno.ERROR, "primaryKey is null;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            }
            Integer tid = primaryKey.getInt(ProductBasicEntity.ProductInfo.TID);
            Integer siteId = primaryKey.getInt(ProductBasicEntity.ProductInfo.SITE_ID);
            Integer lgId = primaryKey.getInt(ProductBasicEntity.ProductInfo.LGID);
            Integer keepPriId1 = primaryKey.getInt(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1);
            pdInfo.setInt(ProductBasicEntity.ProductInfo.TID, tid);
            pdInfo.setInt(ProductBasicEntity.ProductInfo.SITE_ID, siteId);
            pdInfo.setInt(ProductBasicEntity.ProductInfo.LGID, lgId);
            pdInfo.setInt(ProductBasicEntity.ProductInfo.KEEP_PRI_ID1, keepPriId1);
            int pdId = pdInfo.getInt(ProductBasicEntity.ProductInfo.PD_ID);

            // todo 2????????????????????????
            if (!Utils.isEmptyList(richTexts)) {
                for (Param richText : richTexts) {
                    int richType = richText.getInt(MgRichTextEntity.Info.TYPE);
                    String content = richText.getString(MgRichTextEntity.Info.CONTENT);
                    pdInfo.setString(RichTextConverter.getKey(richType), content);
                }
            }
            info.setParam(MgProductEntity.Info.BASIC, pdInfo);

            // todo ????????????
            // todo ????????????sku

            // 3???????????????????????????
            List<Param> storeList = storeSalesMap.get(unionPriId + "_" + pdId);
            if (storeList == null) {
                info.setList(MgProductEntity.Info.STORE_SALES, new FaiList<>());
            } else {
                for (Param storeInfo : storeList) {
                    storeInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.TID, tid);
                    storeInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, siteId);
                    storeInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, lgId);
                    storeInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, keepPriId1);
                }
                info.setList(MgProductEntity.Info.STORE_SALES, new FaiList<>(storeList));
            }

            // 4????????? spuBizSales ??????
            List<Param> spuSalesList = spuSalesMap.get(unionPriId + "_" + pdId);
            if (spuSalesList == null) {
                info.setParam(MgProductEntity.Info.SPU_SALES, new Param());
            } else {
                Param spuSalesInfo = spuSalesList.get(0);
                spuSalesInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.TID, tid);
                spuSalesInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.SITE_ID, siteId);
                spuSalesInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.LGID, lgId);
                spuSalesInfo.setInt(ProductStoreEntity.StoreSalesSkuInfo.KEEP_PRI_ID1, keepPriId1);
                info.setParam(MgProductEntity.Info.SPU_SALES, spuSalesInfo);
            }

            list.add(info);
        }

        return list;
    }

    private void filterExistsSkuCode(FaiList<Param> productList, HashSet<String> existsSkuCodeSet, FaiList<Integer> idList, Set<Param> needRemoveParamSet, FaiList<Param> errProductList){
        for (int i = 0; i < productList.size(); i++) {
            Param productInfo = productList.get(i);
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
                if (!errProductList.contains(productInfo)) {
                    errProductList.add(productInfo);
                }
                idList.set(i, ERROR);
                needRemoveParamSet.add(productInfo);
            }
        }
    }
    private void checkImportProductList(FaiList<Param> productList, HashSet<String> skuCodeSet, FaiList<Param> errProductList, FaiList<Integer> idList, Set<Param> needRemoveParamSet, boolean useMgProductBasicInfo) {
        for (int i = 0; i < productList.size(); i++) {
            Param productInfo = productList.get(i);
            Param basicInfo = productInfo.getParam(MgProductEntity.Info.BASIC);
            if(Str.isEmpty(basicInfo)){
                errProductList.add(productInfo);
                productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.BASIC_IS_EMPTY);
                idList.set(i, ERROR);
                needRemoveParamSet.add(productInfo);
                continue;
            }
            if(!useMgProductBasicInfo){
                int rlPdId = basicInfo.getInt(ProductBasicEntity.ProductInfo.RL_PD_ID, 0);
                if(rlPdId <= 0){
                    productInfo.setInt(MgProductEntity.Info.ERRNO, MgProductErrno.Import.BASIC_IS_EMPTY);
                    errProductList.add(productInfo);
                    idList.set(i, ERROR);
                    needRemoveParamSet.add(productInfo);
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
                idList.set(i, ERROR);
                needRemoveParamSet.add(productInfo);
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
                idList.set(i, ERROR);
                needRemoveParamSet.add(productInfo);
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

    // ????????????????????????????????? id ????????????????????????
    private static final int ERROR = -1;
}
