package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgPrimaryKeySvr.interfaces.entity.MgPrimaryKeyEntity;
import fai.MgProductBasicSvr.interfaces.cli.MgProductBasicCli;
import fai.MgProductBasicSvr.interfaces.entity.ProductRelEntity;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicService;
import fai.MgProductInfSvr.domain.serviceproc.ProductPropService;
import fai.MgProductInfSvr.domain.serviceproc.ProductStoreService;
import fai.MgProductInfSvr.domain.serviceproc.SpecificationService;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductPropDto;
import fai.MgProductInfSvr.interfaces.dto.ProductSpecDto;
import fai.MgProductInfSvr.interfaces.dto.ProductStoreDto;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductSpecEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreEntity;
import fai.MgProductInfSvr.interfaces.entity.ProductStoreValObj;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.middleground.FaiValObj;
import fai.comm.middleground.service.ServicePub;
import fai.comm.util.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MgProductInfService extends ServicePub {

    /**
     * 添加商品参数以及参数值
     */
    public int addPropInfoWithVal(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, Param propInfo, FaiList<Param> proValList) throws IOException {
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

            Ref<Integer> rlPropIdRef = new Ref<Integer>();
            //添加商品参数数据
            ProductPropService productPropService = new ProductPropService(flow);
            rt = productPropService.addPropInfoWithVal(aid, tid, unionPriId, libId, propInfo, proValList, rlPropIdRef);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            sendBuf.putInt(ProductPropDto.Key.RL_PROP_ID, rlPropIdRef.value);
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 修改商品参数以及参数值
     * 参数值的修改包括 增删改
     */
    public int setPropAndVal(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, ParamUpdater propUpdater, FaiList<Param> addValList, FaiList<ParamUpdater> setValList, FaiList<Integer> delValIds) throws IOException {
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
            boolean argsError = true;

            ProductPropService productPropService = new ProductPropService(flow);
            if(propUpdater != null && !propUpdater.isEmpty()) {
                argsError = false;
                rt = productPropService.setPropInfo(aid, tid, unionPriId, libId, rlPropId, propUpdater);
                if(rt != Errno.OK) {
                    return rt;
                }
            }

            if(addValList == null) {
                addValList = new FaiList<Param>();
            }
            if(setValList == null) {
                setValList = new FaiList<ParamUpdater>();
            }
            if(delValIds == null) {
                delValIds = new FaiList<Integer>();
            }
            if(!addValList.isEmpty() || !setValList.isEmpty() || !delValIds.isEmpty()) {
                argsError = false;
                rt = productPropService.setPropValList(aid, tid, unionPriId, libId, rlPropId, addValList, setValList, delValIds);
                if(rt != Errno.OK) {
                    return rt;
                }
            }
            if(argsError) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "all arg is null error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            Log.logStd("set ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int getPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, SearchArg searchArg) throws IOException {
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

            ProductPropService productPropService = new ProductPropService(flow);
            FaiList<Param> list = new FaiList<Param>();
            rt = productPropService.getPropList(aid, tid, unionPriId, libId, searchArg, list);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductPropDto.Key.PROP_LIST, ProductPropDto.getPropInfoDto());
            if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
                sendBuf.putInt(ProductPropDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
            }
            session.write(sendBuf);
            Log.logDbg("get list ok;flow=%d;aid=%d;size=%d;", flow, aid, list.size());
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    public int addPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Param> list) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(list == null || list.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            Ref<FaiList<Integer>> rlPropIdRef = new Ref<FaiList<Integer>>();
            ProductPropService productPropService = new ProductPropService(flow);
            rt = productPropService.addPropList(aid, tid, unionPriId, libId, list, rlPropIdRef);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiList<Integer> rlPropIds = rlPropIdRef.value;

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            if(rlPropIds != null) {
                rlPropIds.toBuffer(sendBuf, ProductPropDto.Key.RL_PROP_IDS);
            }
            session.write(sendBuf);
            Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    public int setPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductPropService productPropService = new ProductPropService(flow);
            rt = productPropService.setPropList(aid, tid, unionPriId, libId, updaterList);
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

    public int delPropList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> idList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(idList == null || idList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            ProductPropService productPropService = new ProductPropService(flow);
            rt = productPropService.delPropList(aid, tid, unionPriId, libId, idList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("del ok;flow=%d;aid=%d;unionPriId=%d;idList=%s;", flow, aid, unionPriId, idList);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 根据商品参数业务id，获取商品参数值列表
     */
    public int getPropValList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, FaiList<Integer> rlPropIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(rlPropIds == null || rlPropIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "args error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductPropService productPropService = new ProductPropService(flow);
            FaiList<Param> list = new FaiList<Param>();
            rt = productPropService.getPropValList(aid, tid, unionPriId, libId, rlPropIds, list);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            list.toBuffer(sendBuf, ProductPropDto.Key.VAL_LIST, ProductPropDto.getPropValInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    /**
     * 批量修改(包括增、删、改)指定商品库的商品参数值列表
     */
    public int setPropValList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int libId, int rlPropId, FaiList<Param> addValList, FaiList<ParamUpdater> setValList, FaiList<Integer> delValIds) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(addValList == null) {
                addValList = new FaiList<Param>();
            }
            if(setValList == null) {
                setValList = new FaiList<ParamUpdater>();
            }
            if(delValIds == null) {
                delValIds = new FaiList<Integer>();
            }
            if(addValList.isEmpty() && setValList.isEmpty() && delValIds.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr(rt, "all arg is null error;flow=%d;aid=%d;", flow, aid);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            ProductPropService productPropService = new ProductPropService(flow);
            rt = productPropService.setPropValList(aid, tid, unionPriId, libId, rlPropId, addValList, setValList, delValIds);
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

            ProductBasicService basicService = new ProductBasicService(flow);
            FaiList<Param> bindPropList = new FaiList<Param>();
            rt = basicService.getPdBindPropInfo(aid, tid, unionPriId, rlPdId, bindPropList);
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
            ProductPropService productPropService = new ProductPropService(flow);
            FaiList<Param> propValList = new FaiList<Param>();
            // 根据参数id集合，获取参数值id集合
            rt = productPropService.getPropValList(aid, tid, unionPriId, rlLibId, rlPropIds, propValList);
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

            ProductBasicService basicService = new ProductBasicService(flow);
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
            ProductBasicService basicService = new ProductBasicService(flow);
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
     * 批量添加规格模板
     */
    public int addTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> list) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.addTpScInfoList(aid, tid, unionPriId, list);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板
     */
    public int delTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlTpScIdList) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.delTpScInfoList(aid, tid, unionPriId, rlTpScIdList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板
     */
    public int setTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<ParamUpdater> updaterList) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.setTpScInfoList(aid, tid, unionPriId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取规格模板列表
     */
    public int getTpScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationService.getTpScInfoList(aid, tid, unionPriId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTemp.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量添加规格模板详情
     */
    public int addTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Param> list) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.addTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, list);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板详情
     */
    public int delTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<Integer> tpScDtIdList) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.delTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, tpScDtIdList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量修改规格模板详情
     */
    public int setTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId, FaiList<ParamUpdater> updaterList) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.setTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取规格模板列表详情
     */
    public int getTpScDetailInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlTpScId) throws IOException {
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
            SpecificationService specificationService = new SpecificationService(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationService.getTpScDetailInfoList(aid, tid, unionPriId, rlTpScId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecTempDetail.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 导入规格模板
     */
    public int importPdScInfo(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, int rlTpScId, FaiList<Integer> tpScDtIdList) throws IOException {
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

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;


            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.importPdScInfo(aid, tid, unionPriId, pdId, rlTpScId, tpScDtIdList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 修改产品规格总接口
     * 批量修改(包括增、删、改)指定商品的商品规格总接口；会自动生成sku规格，并且会调用商品库存服务的“刷新商品库存销售sku”
     */
    public int unionSetPdScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<Param> addList, FaiList<Integer> delList, FaiList<ParamUpdater> updaterList) throws IOException {
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

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            SpecificationService specificationService = new SpecificationService(flow);
            FaiList<Param> pdScSkuInfoList = new FaiList<>();
            rt = specificationService.unionSetPdScInfoList(aid, tid, unionPriId, pdId, addList, delList, updaterList, pdScSkuInfoList);
            if(rt != Errno.OK) {
                return rt;
            }

            ProductStoreService productStoreService = new ProductStoreService(flow);
            rt = productStoreService.refreshPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, pdScSkuInfoList);
            if(rt != Errno.OK) {
                return rt;
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取产品规格列表
     */
    public int getPdScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, boolean onlyGetChecked) throws IOException {
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

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            SpecificationService specificationService = new SpecificationService(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationService.getPdScInfoList(aid, tid, unionPriId, pdId, infoList, onlyGetChecked);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.Spec.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量修改产品规格SKU
     */
    public int setPdSkuScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) throws IOException {
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

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            SpecificationService specificationService = new SpecificationService(flow);
            rt = specificationService.setPdSkuScInfoList(aid, tid, unionPriId, pdId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取产品规格SKU列表
     */
    public int getPdSkuScInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId) throws IOException {
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

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            SpecificationService specificationService = new SpecificationService(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = specificationService.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductSpecDto.Key.INFO_LIST, ProductSpecDto.SpecSku.getInfoDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
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

            ProductBasicService basicService = new ProductBasicService(flow);
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

            ProductBasicService basicService = new ProductBasicService(flow);
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

            ProductBasicService basicService = new ProductBasicService(flow);
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

            ProductBasicService basicService = new ProductBasicService(flow);
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

            ProductBasicService basicService = new ProductBasicService(flow);
            rt = basicService.batchDelProduct(aid, tid, unionPriId, rlPdIds);
            if(rt != Errno.OK) {
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


    /**
     * 修改商品规格库存销售sku
     */
    public int setPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId, FaiList<ParamUpdater> updaterList) throws IOException  {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if(!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(updaterList == null || updaterList.isEmpty()){
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, updaterList is err;flow=%d;aid=%d;tid=%d;updaterList=%s;", flow, aid, tid, updaterList);
                return rt;
            }

            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;
            Log.logDbg("whalelog  updaterList=%s", updaterList);

            Map<FaiList<String>, Param> inPdScStrNameInfoMap = new HashMap<>();
            for (ParamUpdater updater : updaterList) {
                Param data = updater.getData();
                Long skuId = data.getLong(ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                if(skuId == null){
                    FaiList<String> inPdScStrNameList = data.getList(ProductStoreEntity.StoreSalesSkuInfo.IN_PD_SC_STR_NAME_LIST);
                    if(inPdScStrNameList == null){
                        return rt = Errno.ARGS_ERROR;
                    }
                    inPdScStrNameInfoMap.put(inPdScStrNameList, data);
                }
            }
            Log.logDbg("whalelog inPdScStrNameInfoMap=%s", inPdScStrNameInfoMap);
            if(inPdScStrNameInfoMap.size() > 0){
                SpecificationService specificationService = new SpecificationService(flow);
                FaiList<Param> infoList = new FaiList<Param>();
                rt = specificationService.getPdSkuScInfoList(aid, tid, unionPriId, pdId, infoList);
                if(rt != Errno.OK) {
                    return rt;
                }
                for (Param info : infoList) {
                    Param data = inPdScStrNameInfoMap.get(info.getList(ProductSpecEntity.SpecSkuInfo.IN_PD_SC_STR_NAME_LIST));
                    if(data == null){
                        continue;
                    }
                    data.assign(info, ProductSpecEntity.SpecSkuInfo.SKU_ID, ProductStoreEntity.StoreSalesSkuInfo.SKU_ID);
                }
            }

            ProductStoreService productStoreService = new ProductStoreService(flow);
            rt = productStoreService.setPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, updaterList);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 扣减库存
     * @param session
     * @param flow
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     * @param reduceMode
     *  扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @param expireTimeSeconds 配合预扣模式，单位s
     * @return
     * @throws IOException
     */
    public int reducePdSkuStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, long skuId, int rlOrderId, int count, int reduceMode, int expireTimeSeconds) throws IOException  {
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

            ProductStoreService productStoreService = new ProductStoreService(flow);
            rt = productStoreService.reducePdSkuStore(aid, tid, unionPriId, skuId, rlOrderId, count, reduceMode, expireTimeSeconds);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 预扣模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode#HOLDING} 步骤2
     * @param session
     * @param flow
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     */
    public int reducePdSkuHoldingStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, long skuId, int rlOrderId, int count) throws IOException  {
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

            ProductStoreService productStoreService = new ProductStoreService(flow);
            rt = productStoreService.reducePdSkuHoldingStore(aid, tid, unionPriId, skuId, rlOrderId, count);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 扣减库存
     * @param session
     * @param flow
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param skuId
     * @param rlOrderId 业务订单id
     * @param count 扣减数量
     * @param reduceMode
     *  扣减模式 {@link ProductStoreValObj.StoreSalesSku.ReduceMode}
     * @return
     * @throws IOException
     */
    public int makeUpStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, long skuId, int rlOrderId, int count, int reduceMode) throws IOException  {
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

            ProductStoreService productStoreService = new ProductStoreService(flow);
            rt = productStoreService.makeUpStore(aid, unionPriId, skuId, rlOrderId, count, reduceMode);
            if(rt != Errno.OK) {
                return rt;
            }

            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取商品规格库存销售sku
     */
    public int getPdScSkuSalesStore(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId)  throws IOException {
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

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            ProductStoreService productStoreService = new ProductStoreService(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreService.getPdScSkuSalesStore(aid, tid, unionPriId, pdId, rlPdId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.getStoreSalesSkuDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 添加库存出入库记录
     * @param aid
     * @param tid
     * @param siteId
     * @param lgId
     * @param keepPriId1
     * @param infoList
     * @return
     */
    public int addInOutStoreRecordInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (!FaiValObj.TermId.isValidTid(tid)) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, tid is not valid;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
                return rt;
            }
            if(infoList == null || infoList.isEmpty()){
                rt = Errno.ARGS_ERROR;
                Log.logErr("args error, infoList is not valid;flow=%d;aid=%d;tid=%d;infoList=%s;", flow, aid, tid, infoList);
                return rt;
            }
            // 获取unionPriId
            Ref<Integer> idRef = new Ref<Integer>();
            rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int unionPriId = idRef.value;
            /*FaiList<Param> searchArgList = new FaiList<>();
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1, 0);
                // 获取unionPriId
                Ref<Integer> idRef = new Ref<Integer>();
                rt = getUnionPriId(flow, aid, tid, siteId, lgId, keepPriId1, idRef);
                if(rt != Errno.OK) {
                    return rt;
                }
                info.setInt(ProductStoreEntity.InOutStoreRecordInfo.UNION_PRI_ID, idRef.value);
                searchArgList.add(
                        new Param().setInt(MgPrimaryKeyEntity.Info.TID, tid)
                        .setInt(MgPrimaryKeyEntity.Info.SITE_ID, siteId)
                        .setInt(MgPrimaryKeyEntity.Info.LGID, lgId)
                        .setInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1, keepPriId1)
                );
            }
            FaiList<Param> primaryKeyList = new FaiList<>();
            rt = getPrimaryKeyList(flow, aid, searchArgList, primaryKeyList);
            if(rt != Errno.OK) {
                return rt;
            }
            Map<String, Integer> primaryKeyMap = new HashMap<>();
            for (Param primaryKey : primaryKeyList) {
                Integer siteId = primaryKey.getInt(MgPrimaryKeyEntity.Info.SITE_ID);
                Integer lgId = primaryKey.getInt(MgPrimaryKeyEntity.Info.LGID);
                Integer keepPriId1 = primaryKey.getInt(MgPrimaryKeyEntity.Info.KEEP_PRI_ID1);
                Integer unionPriId = primaryKey.getInt(MgPrimaryKeyEntity.Info.UNION_PRI_ID);
                primaryKeyMap.put(siteId + "-" + lgId + "-" + keepPriId1, unionPriId);
            }
            for (Param info : infoList) {
                Integer siteId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.SITE_ID, 0);
                Integer lgId = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.LGID, 0);
                Integer keepPriId1 = info.getInt(ProductStoreEntity.InOutStoreRecordInfo.KEEP_PRI_ID1, 0);
                info.setInt(ProductStoreEntity.InOutStoreRecordInfo.UNION_PRI_ID, primaryKeyMap.get(siteId + "-" + lgId + "-" + keepPriId1));
            }*/

            ProductStoreService productStoreService = new ProductStoreService(flow);
            rt = productStoreService.addInOutStoreRecordInfoList(aid, tid, unionPriId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取商品业务销售信息
     */
    public int getBizSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, int rlPdId)  throws IOException {
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

            // 获取pdId
            idRef.value = null;
            rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
            if(rt != Errno.OK) {
                return rt;
            }
            int pdId = idRef.value;

            ProductStoreService productStoreService = new ProductStoreService(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreService.getBizSalesSummaryInfoList(aid, tid, unionPriId, pdId, rlPdId, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.getBizSalesSummaryDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取商品销售信息
     */
    public int getSalesSummaryInfoList(FaiSession session, int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, FaiList<Integer> rlPdIdList)  throws IOException {
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

            // unionPriId + rlPdIdList
            FaiList<Integer> pdIdList = new FaiList<>(rlPdIdList.size());
            for (Integer rlPdId : rlPdIdList) { // TODO 批量
                // 获取pdId
                idRef.value = null;
                rt = getPdId(flow, aid, tid, unionPriId, rlPdId, idRef);
                if(rt != Errno.OK) {
                    return rt;
                }
                int pdId = idRef.value;
                pdIdList.add(pdId);
            }


            ProductStoreService productStoreService = new ProductStoreService(flow);
            FaiList<Param> infoList = new FaiList<Param>();
            rt = productStoreService.getSalesSummaryInfoList(aid, tid, unionPriId, pdIdList, infoList);
            if(rt != Errno.OK) {
                return rt;
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            infoList.toBuffer(sendBuf, ProductStoreDto.Key.INFO_LIST, ProductStoreDto.getSalesSummaryDto());
            session.write(sendBuf);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }

    /**
     * 获取unionPriId
     */
    private int getUnionPriId(int flow, int aid, int tid, int siteId, int lgId, int keepPriId1, Ref<Integer> idRef) {
        int rt = Errno.ERROR;
        MgPrimaryKeyCli cli = new MgPrimaryKeyCli(flow);
        if(!cli.init()) {
            rt = Errno.ERROR;
            Log.logErr(rt, "init MgPrimaryKeyCli error");
            return rt;
        }

        rt = cli.getUnionPriId(aid, tid, siteId, lgId, keepPriId1, idRef);
        if(rt != Errno.OK) {
            Log.logErr(rt, "getUnionPriId error;flow=%d;aid=%d;tid=%d;", flow, aid, tid);
            return rt;
        }
        return rt;
    }

    private int getPrimaryKeyList(int flow, int aid, FaiList<Param> searchArgList, FaiList<Param> list) {
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

    /**
     * 获取PdId
     */
    private int getPdId(int flow, int aid, int tid, int unionPriId, int rlPdId, Ref<Integer> idRef) {
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
            Log.logErr(rt, "getRelInfoByRlId error;flow=%d;aid=%d;unionPriId=%d;rlPdId=%s;", flow, aid, unionPriId, rlPdId);
            return rt;
        }
        idRef.value = pdRelInfo.getInt(ProductRelEntity.Info.PD_ID);
        return rt;
    }


}
