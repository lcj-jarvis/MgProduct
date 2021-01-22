package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductPropProc;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreProc;
import fai.MgProductInfSvr.domain.serviceproc.SpecificationProc;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.util.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * 商品基础服务相关接口
 */
public class ProductBasicService extends MgProductInfService {

    public int getBindPropInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlLibId) throws IOException {
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

            ProductBasicProc basicProc = new ProductBasicProc(flow);
            FaiList<Param> bindPropList = new FaiList<Param>();
            rt = basicProc.getPdBindPropInfo(aid, tid, unionPriId, rlPdId, bindPropList);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiList<Integer> rlPropIds = new FaiList<Integer>();
            for(Param tmpInfo : bindPropList) {
                int rlPropId = tmpInfo.getInt(ProductBasicEntity.BindPropInfo.RL_PROP_ID);
                if(!rlPropIds.contains(rlPropId)) {
                    rlPropIds.add(rlPropId);
                }
            }
            if(bindPropList.isEmpty()) {
                rt = Errno.NOT_FOUND;
                return rt;
            }
            ProductPropProc productPropProc = new ProductPropProc(flow);
            FaiList<Param> propValList = new FaiList<Param>();
            // 根据参数id集合，获取参数值id集合
            rt = productPropProc.getPropValList(aid, tid, unionPriId, rlLibId, rlPropIds, propValList);
            if(rt != Errno.OK) {
                return rt;
            }
            // 组装数据
            Param resultInfo = new Param();
            for(Param tmpInfo : bindPropList) {
                int rlPropId = tmpInfo.getInt(ProductBasicEntity.BindPropInfo.RL_PROP_ID);
                int propValId = tmpInfo.getInt(ProductBasicEntity.BindPropInfo.PROP_VAL_ID);
                // 根据参数id，参数值id，获取参数值信息
                ParamMatcher matcher = new ParamMatcher(ProductBasicEntity.BindPropInfo.RL_PROP_ID, ParamMatcher.EQ, rlPropId);
                matcher.and(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, ParamMatcher.EQ, propValId);
                Param propValInfo = Misc.getFirst(propValList, matcher);
                if(!Str.isEmpty(propValInfo)) {
                    String val = propValInfo.getString(ProductBasicEntity.BindPropInfo.VAL);
                    FaiList<Param> props = resultInfo.getList(ProductBasicDto.BIND_PROP_DTO_PREFIX + rlPropId);
                    if(props == null) {
                        props = new FaiList<Param>();
                        resultInfo.setList(ProductBasicDto.BIND_PROP_DTO_PREFIX + rlPropId, props);
                    }
                    Param info = new Param();
                    info.setInt(ProductBasicEntity.BindPropInfo.PROP_VAL_ID, propValId);
                    info.setString(ProductBasicEntity.BindPropInfo.VAL, val);
                    props.add(info);
                }
            }

            // 将def序列化
            ParamDef def = ProductBasicDto.getBindPropDto(rlPropIds);
            ByteBuffer buf = Parser.paramDefToByteBuffer(def);
            if (buf == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "serialize ParamDef err;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putBuffer(ProductBasicDto.Key.SERIALIZE_TMP_DEF, buf);
            resultInfo.toBuffer(sendBuf, ProductBasicDto.Key.BIND_PROP_INFO, def);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int setProductBindPropInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addPropList, FaiList<Param> delPropList) throws IOException {
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

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.setPdBindPropInfo(aid, tid, unionPriId, rlPdId, addPropList, delPropList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int getRlPdByPropVal(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> proIdsAndValIds) throws IOException {
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

            FaiList<Integer> rlPdIds = new FaiList<Integer>();
            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.getRlPdByPropVal(aid, tid, unionPriId, proIdsAndValIds, rlPdIds);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            rlPdIds.toBuffer(sendBuf, ProductBasicDto.Key.RL_PD_IDS);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 新增商品数据，并添加与当前unionPriId的关联
     */
    public int addProductAndRel(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Param info) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(Str.isEmpty(info)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, info is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            Ref<Integer> rlPdIdRef = new Ref<Integer>();
            Ref<Integer> pdIdRef = new Ref<Integer>();
            rt = basicService.addProductAndRel(aid, tid, unionPriId, info, pdIdRef, rlPdIdRef);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdIdRef.value);
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;pdId=%d;", flow, aid, unionPriId, rlPdIdRef.value, pdIdRef.value);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 新增商品业务关联
     */
    public int bindProductRel(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Param bindRlPdInfo, Param info) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(Str.isEmpty(info)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, info is empty;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(Str.isEmpty(bindRlPdInfo)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, bindRlPdInfo is empty;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            Integer bindRlPdId = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.RL_PD_ID);
            if(bindRlPdId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, bindRlPdId is not exist;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            int bindTid = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.TID, 0);
            if(!FaiValObj.TermId.isValidTid(bindTid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, bindTid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, bindTid);
                return rt;
            }
            int bindSiteId = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.SITE_ID, 0);
            int bindLgid = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.LGID, 0);
            int bindKeepPriId1 = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.KEEP_PRI_ID1, 0);
            Ref<Integer> bindIdRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, bindTid, bindSiteId, bindLgid, bindKeepPriId1, bindIdRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int bindUnionPriId = bindIdRef.value;
            // 重组 bindRlPdInfo
            bindRlPdInfo.clear();
            bindRlPdInfo.setInt(ProductBasicEntity.ProductRelInfo.UNION_PRI_ID, bindUnionPriId);
            bindRlPdInfo.setInt(ProductBasicEntity.ProductRelInfo.RL_PD_ID, bindRlPdId);

            ProductBasicProc basicService = new ProductBasicProc(flow);
            Ref<Integer> rlPdIdRef = new Ref<Integer>();
            rt = basicService.bindProductRel(aid, tid, unionPriId, bindRlPdInfo, info, rlPdIdRef);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductBasicDto.Key.RL_PD_ID, rlPdIdRef.value);
            session.write(sendBuf);
            Log.logStd("bind ok;flow=%d;aid=%d;unionPriId=%d;rlPdId=%d;", flow, aid, unionPriId, rlPdIdRef.value);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 批量新增商品业务关联
     */
    public int batchBindProductRel(FaiSession session, int flow, int aid, int tid, Param bindRlPdInfo, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, infoList is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }

            Integer bindRlPdId = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.RL_PD_ID);
            if(bindRlPdId == null) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, bindRlPdId is not exist;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }

            int bindTid = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.TID, 0);
            if(!FaiValObj.TermId.isValidTid(bindTid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, bindTid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, bindTid);
                return rt;
            }
            int bindSiteId = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.SITE_ID, 0);
            int bindLgid = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.LGID, 0);
            int bindKeepPriId1 = bindRlPdInfo.getInt(ProductBasicEntity.ProductRelInfo.KEEP_PRI_ID1, 0);

            FaiList<Param> searchArgList = new FaiList<Param>();
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductBasicEntity.ProductRelInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductBasicEntity.ProductRelInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductBasicEntity.ProductRelInfo.KEEP_PRI_ID1, 0);
                searchArgList.add(new Param().setInt(MgPrimaryKeyEntity.Info.TID, tid)
                                .setInt(MgPrimaryKeyEntity.Info.SITE_ID, siteId)
                                .setInt(MgPrimaryKeyEntity.Info.LGID, lgId)
                                .setInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1, keepPriId1)
                );
            }
            searchArgList.add(new Param().setInt(MgPrimaryKeyEntity.Info.TID, bindTid)
                    .setInt(MgPrimaryKeyEntity.Info.SITE_ID, bindSiteId)
                    .setInt(MgPrimaryKeyEntity.Info.LGID, bindLgid)
                    .setInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1, bindKeepPriId1)
            );
            FaiList<Param> primaryKeyList = new FaiList<Param>();
            rt = getPrimaryKeyList(flow, aid, searchArgList, primaryKeyList);
            if(rt != Errno.OK) {
                return rt;
            }
            Map<String, Integer> primaryKeyMap = new HashMap<String, Integer>();
            for (Param primaryKey : primaryKeyList) {
                Integer resTid = primaryKey.getInt(MgPrimaryKeyEntity.Info.TID);
                Integer siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
                Integer lgId = primaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
                Integer keepPriId1 = primaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
                Integer unionPriId = primaryKey.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
                primaryKeyMap.put(resTid + "-" + siteId + "-" + lgId + "-" + keepPriId1, unionPriId);
            }
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductBasicEntity.ProductRelInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductBasicEntity.ProductRelInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductBasicEntity.ProductRelInfo.KEEP_PRI_ID1, 0);
                info.setInt(ProductBasicEntity.ProductRelInfo.UNION_PRI_ID, primaryKeyMap.get(tid + "-" + siteId + "-" + lgId + "-" + keepPriId1));
            }

            Integer bindUnionPriId = primaryKeyMap.get(bindTid + "-" + bindSiteId + "-" + bindLgid + "-" + bindKeepPriId1);
            if(bindUnionPriId == null) {
                rt = Errno.ERROR;
                Log.logErr("get unionPriId error;flow=%d;aid=%d;bindRlPdInfo=%d;", flow, aid, bindRlPdInfo);
                return rt;
            }

            // 重组 bindRlPdInfo
            bindRlPdInfo.clear();
            bindRlPdInfo.setInt(ProductBasicEntity.ProductRelInfo.UNION_PRI_ID, bindUnionPriId);
            bindRlPdInfo.setInt(ProductBasicEntity.ProductRelInfo.RL_PD_ID, bindRlPdId);

            ProductBasicProc basicService = new ProductBasicProc(flow);
            Ref<FaiList<Integer>> rlPdIdsRef = new Ref<FaiList<Integer>>();
            rt = basicService.batchBindProductRel(aid, tid, bindRlPdInfo, infoList, rlPdIdsRef);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiList<Integer> rlPdIds = rlPdIdsRef.value;
            if(rlPdIds == null) {
                rlPdIds = new FaiList<Integer>();
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            rlPdIds.toBuffer(sendBuf, ProductBasicDto.Key.RL_PD_IDS);
            session.write(sendBuf);
            Log.logStd("batch bind ok;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 取消 rlPdIds 的商品业务关联
     * @return
     */
    public int batchDelPdRelBind(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdIds is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductBasicProc basicService = new ProductBasicProc(flow);
            rt = basicService.batchDelPdRelBind(aid, unionPriId, rlPdIds);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("batch del bind ok;flow=%d;aid=%d;tid=%d;rlPdIds=%s;", flow, aid, tid, rlPdIds);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 删除 rlPdIds 的商品数据及业务关联
     * @return
     */
    public int batchDelProduct(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(rlPdIds == null || rlPdIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error, rlPdIds is empty;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductStoreProc storeProc = new ProductStoreProc(flow);
            SpecificationProc specificationProc = new SpecificationProc(flow);
            ProductBasicProc basicService = new ProductBasicProc(flow);
            FaiList<Param> list = new FaiList<>();
            rt = basicService.getRelListByRlIds(aid, unionPriId, rlPdIds, list);
            if(rt != Errno.OK){
                return rt;
            }
            FaiList<Integer> pdIdList = OptMisc.getValList(list, ProductBasicEntity.ProductRelInfo.PD_ID);
            // 没分布式事务很难做一致性

            // 删除商品基础信息
            rt = basicService.batchDelProduct(aid, tid, unionPriId, rlPdIds);
            if(rt != Errno.OK) {
                return rt;
            }
            // 删除库存销售相关信息
            rt = storeProc.batchDelPdAllStoreSales(aid, tid, pdIdList);
            if(rt != Errno.OK){
                Log.logErr(rt, "batchDelPdAllStoreSales err;aid=%s;tid=%s;pdIdList=%s;", aid, tid, pdIdList);
                return rt;
            }
            // 删除商品规格相关信息
            rt = specificationProc.batchDelPdAllSc(aid, tid, pdIdList);
            if(rt != Errno.OK){
                Log.logErr(rt, "batchDelPdAllStoreSales err;aid=%s;tid=%s;pdIdList=%s;", aid, tid, pdIdList);
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("del products ok;flow=%d;aid=%d;tid=%d;rlPdIds=%s;", flow, aid, tid, rlPdIds);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
}
