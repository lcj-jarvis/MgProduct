package fai.MgProductBasicSvr.application.service;

import fai.MgProductBasicSvr.domain.common.ESUtil;
import fai.MgProductBasicSvr.domain.common.LockUtil;
import fai.MgProductBasicSvr.domain.entity.ProductBindGroupEntity;
import fai.MgProductBasicSvr.domain.entity.ProductEntity;
import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.MgProductBasicSvr.domain.repository.cache.CacheCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductDaoCtrl;
import fai.MgProductBasicSvr.domain.repository.dao.ProductRelDaoCtrl;
import fai.MgProductBasicSvr.domain.serviceproc.ProductBindGroupProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductProc;
import fai.MgProductBasicSvr.domain.serviceproc.ProductRelProc;
import fai.MgProductBasicSvr.interfaces.dto.MigrateDef;
import fai.MgProductBasicSvr.interfaces.dto.ProductRelDto;
import fai.app.DocOplogDef;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.misc.Utils;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class DataMigrateService {

    @SuccessRt(value = Errno.OK)
    public int dataMigrate(FaiSession session, int flow, int aid, int tid, FaiList<Param> list, int sysType) throws IOException {
        int rt;
        if(Utils.isEmptyList(list)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, list is empty;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }

        FaiList<Integer> unionPriIds = new FaiList<>();
        FaiList<Param> pdList = new FaiList<>();
        for(Param info : list) {
            Param pdInfo = info.getParam(MigrateDef.Info.ADD_PD);
            pdList.add(pdInfo);
        }

        Map<String, Integer> unionPriIdRlPdId_pdId = new HashMap<>();

        LockUtil.lock(aid);
        try {
            //统一控制事务
            TransactionCtrl tc = new TransactionCtrl();
            boolean commit = false;
            try {
                tc.setAutoCommit(false);
                ProductRelProc relProc = new ProductRelProc(flow, aid, tc);
                ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
                matcher.and(ProductRelEntity.Info.SYS_TYPE, ParamMatcher.EQ, sysType);
                SearchArg searchArg = new SearchArg();
                searchArg.matcher = matcher;
                FaiList<Param> dbList = relProc.searchFromDbWithDel(aid, searchArg, null);
                for(Param info : dbList) {
                    int dbRlPdId = info.getInt(ProductRelEntity.Info.RL_PD_ID);
                    int dbPdId = info.getInt(ProductRelEntity.Info.PD_ID);
                    int dbUnionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
                    unionPriIdRlPdId_pdId.put(dbUnionPriId+"-"+dbRlPdId, dbPdId);
                }

                // 要更新的商品数据
                FaiList<Param> updatePdList = new FaiList<>();
                // 要新增的商品数据
                FaiList<Param> addPdList = new FaiList<>();
                FaiList<Integer> addPdRlPdIds = new FaiList<>(); // 需要新增的基础表数据对应的rlPdId
                for(Param pdInfo : pdList) {
                    int rlPdId = (int)pdInfo.remove(ProductRelEntity.Info.RL_PD_ID);
                    int unionPriId = pdInfo.getInt(ProductEntity.Info.SOURCE_UNIONPRIID);
                    Integer pdId = unionPriIdRlPdId_pdId.get(unionPriId + "-" + rlPdId);
                    if(pdId != null) {
                        pdInfo.setInt(ProductEntity.Info.PD_ID, pdId);
                        updatePdList.add(pdInfo);
                    }else {
                        addPdList.add(pdInfo);
                        addPdRlPdIds.add(rlPdId);
                    }
                }

                // 新加数据的id映射关系
                Map<String, Integer> newUnionPriIdRlPdId_pdId = new HashMap<>();
                // 新增商品数据
                ProductProc pdProc = new ProductProc(flow, aid, tc);
                if(!addPdList.isEmpty()) {
                    FaiList<Integer> pdIds = pdProc.batchAddProduct(aid, addPdList);
                    if(addPdList.size() != pdIds.size()) {
                        rt = Errno.ERROR;
                        Log.logErr(rt, "add pd list error;size not match;aid=%d;addList=%s", aid, addPdList);
                        return rt;
                    }
                    for(int i = 0; i < addPdList.size(); i++) {
                        Param pdInfo = addPdList.get(i);
                        int unionPriId = pdInfo.getInt(ProductEntity.Info.SOURCE_UNIONPRIID);
                        int rlPdId = addPdRlPdIds.get(i);
                        int pdId = pdIds.get(i);
                        newUnionPriIdRlPdId_pdId.put(unionPriId + "-" + rlPdId, pdId);
                    }
                }


                // 更新已存在的数据
                if(!updatePdList.isEmpty()) {
                    pdProc.batchSet(aid, updatePdList);
                }

                FaiList<Param> updatePdRelList = new FaiList<>();
                Map<Integer, FaiList<Param>> addPdRelMap = new HashMap<>();
                Map<Integer, FaiList<Param>> addPdBindGroupMap = new HashMap<>();
                for(Param info : list) {
                    Param pdInfo = info.getParam(MigrateDef.Info.ADD_PD);
                    pdList.add(pdInfo);

                    FaiList<Integer> rlGroupIds = info.getList(MigrateDef.Info.BIND_RL_GROUP, new FaiList<>());

                    int sourceUnionPriId = pdInfo.getInt(ProductEntity.Info.SOURCE_UNIONPRIID);

                    FaiList<Param> pdRelList = info.getList(MigrateDef.Info.BIND_PD_REL);
                    for(Param relInfo : pdRelList) {
                        int rlPdId = relInfo.getInt(ProductRelEntity.Info.RL_PD_ID);
                        int unionPriId = relInfo.getInt(ProductRelEntity.Info.UNION_PRI_ID);
                        Integer pdId = unionPriIdRlPdId_pdId.get(unionPriId + "-" + rlPdId);
                        if(pdId != null) {
                            updatePdRelList.add(relInfo);
                        }else {
                            // 之前unionPriId没有数据， 则说明需要新增，用sourceUnionPriId去获取id映射关系
                            pdId = unionPriIdRlPdId_pdId.get(sourceUnionPriId + "-" + rlPdId);
                            if(pdId == null) {
                                pdId = newUnionPriIdRlPdId_pdId.get(sourceUnionPriId + "-" + rlPdId);
                                if(pdId == null) {
                                    rt = Errno.ERROR;
                                    Log.logErr(rt, "new id miss too;flow=%d;aid=%d;sUid=%d;rlPdId=%d;", flow, aid, sourceUnionPriId, rlPdId);
                                    return rt;
                                }
                            }
                            unionPriIdRlPdId_pdId.put(unionPriId+"-"+rlPdId, pdId);
                            FaiList<Param> addList = addPdRelMap.get(unionPriId);
                            if(addList == null) {
                                addList = new FaiList<>();
                                addPdRelMap.put(unionPriId, addList);
                            }
                            addList.add(relInfo);
                        }
                        relInfo.setInt(ProductRelEntity.Info.PD_ID, pdId);
                        // es预处理
                        ESUtil.preLog(aid, pdId, unionPriId, DocOplogDef.Operation.UPDATE_ONE);

                        // 处理绑定分类
                        FaiList<Param> bindGroups = addPdBindGroupMap.get(unionPriId);
                        if(bindGroups == null) {
                            bindGroups = new FaiList<>();
                            addPdBindGroupMap.put(unionPriId, bindGroups);
                        }
                        for(Integer rlGroupId : rlGroupIds) {
                            Param bindGroup = new Param();
                            bindGroup.setInt(ProductBindGroupEntity.Info.AID, aid);
                            bindGroup.setInt(ProductBindGroupEntity.Info.UNION_PRI_ID, unionPriId);
                            bindGroup.setInt(ProductBindGroupEntity.Info.PD_ID, pdId);
                            bindGroup.setInt(ProductBindGroupEntity.Info.RL_PD_ID, rlPdId);
                            bindGroup.setInt(ProductBindGroupEntity.Info.RL_GROUP_ID, rlGroupId);
                            bindGroup.setInt(ProductBindGroupEntity.Info.SYS_TYPE, sysType);
                            bindGroups.add(bindGroup);
                        }
                    }
                }
                // 新增业务关系表数据
                for(int unionPriId : addPdRelMap.keySet()) {
                    unionPriIds.add(unionPriId);
                    FaiList<Param> addList = addPdRelMap.get(unionPriId);
                    if(!Utils.isEmptyList(addList)) {
                        relProc.batchAddProductRel(aid, tid, unionPriId, addList);
                    }
                }

                // 更新业务关系表数据
                if(!updatePdRelList.isEmpty()) {
                    relProc.batchSet(aid, updatePdRelList);
                }

                // 新增分类绑定
                ProductBindGroupProc bindGroupProc = new ProductBindGroupProc(flow, aid, tc);
                for(int unionPriId : addPdBindGroupMap.keySet()) {
                    FaiList<Param> bindGroups = addPdBindGroupMap.get(unionPriId);
                    ParamMatcher delMatcher = new ParamMatcher(ProductBindGroupEntity.Info.AID, ParamMatcher.EQ, aid);
                    // delMatcher.and(ProductBindGroupEntity.Info.UNION_PRI_ID, ParamMatcher.EQ, unionPriId);
                    // bindGroupProc.delPdBindGroup(aid, unionPriId, delMatcher);
                    if(!Utils.isEmptyList(bindGroups)) {
                        bindGroupProc.batchBindGroupList(aid, unionPriId, bindGroups);
                    }
                }

                commit = true;
            } finally {
                if(commit) {
                    tc.commit();
                }else {
                    tc.rollback();
                    ProductDaoCtrl.clearIdBuilderCache(aid);
                    for(int unionPriId : unionPriIds) {
                        ProductRelDaoCtrl.clearIdBuilderCache(aid, unionPriId);
                    }
                }
                tc.closeDao();
            }
            // 更新缓存
            CacheCtrl.clearCacheVersion(aid);

            // 同步数据给es
            ESUtil.commitPre(flow, aid);
        } finally {
            LockUtil.unlock(aid);
        }
        rt = Errno.OK;
        Log.logStd("dataMigrate ok;flow=%d;aid=%d;", flow, aid);
        FaiList<Param> returnList = new FaiList<>();
        for(String key : unionPriIdRlPdId_pdId.keySet()) {
            int unionPriId = Integer.parseInt(key.split("-")[0]);
            int rlPdId = Integer.parseInt(key.split("-")[1]);
            int pdId = unionPriIdRlPdId_pdId.get(key);
            Param returnInfo = new Param();
            returnInfo.setInt(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
            returnInfo.setInt(ProductRelEntity.Info.RL_PD_ID, rlPdId);
            returnInfo.setInt(ProductRelEntity.Info.PD_ID, pdId);
            returnList.add(returnInfo);
        }
        FaiBuffer sendBuf = new FaiBuffer(true);
        returnList.toBuffer(sendBuf, ProductRelDto.Key.REDUCED_INFO, ProductRelDto.getReducedInfoDto());
        session.write(sendBuf);
        return rt;
    }

    @SuccessRt(value = {Errno.OK, Errno.NOT_FOUND})
    public int getMigratePdIds(FaiSession session, int flow, int aid, int sysType) throws IOException {
        int rt = Errno.ERROR;
        FaiList<Integer> pdIds;
        TransactionCtrl tc = new TransactionCtrl();
        LockUtil.lock(aid);
        try {
            tc.setAutoCommit(false);
            ProductRelProc productRelProc = new ProductRelProc(flow, aid, tc);
            ProductProc productProc = new ProductProc(flow, aid, tc);
            ProductBindGroupProc productBindGroupProc = new ProductBindGroupProc(flow, aid, tc);
            boolean commit = false;
            try {
                pdIds = productRelProc.getMigratePdIds(aid, sysType);
                if (!pdIds.isEmpty()) {
                    pdIds = new FaiList<>(new HashSet<>(pdIds));
                    ParamMatcher matcher = new ParamMatcher(ProductRelEntity.Info.AID, ParamMatcher.EQ, aid);
                    matcher.and(ProductRelEntity.Info.PD_ID, ParamMatcher.IN, pdIds);
                    productRelProc.delProductRel(aid, matcher);

                    productProc.delProduct(aid, matcher);

                    productBindGroupProc.delPdBindGroupList(aid, pdIds);
                }

                commit = true;
                rt = Errno.OK;
            } finally {
                if (!commit) {
                    tc.rollback();
                    return rt;
                }
                tc.commit();
            }
        } finally {
            LockUtil.unlock(aid);
            tc.closeDao();
        }

        FaiBuffer sendBuf = new FaiBuffer(true);
        pdIds.toBuffer(sendBuf, ProductRelDto.Key.PD_IDS);
        session.write(sendBuf);
        return rt;
    }
}
