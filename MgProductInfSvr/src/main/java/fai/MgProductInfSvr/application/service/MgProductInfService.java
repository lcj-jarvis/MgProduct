package fai.MgProductInfSvr.application.service;

import fai.MgPrimaryKeySvr.interfaces.cli.MgPrimaryKeyCli;
import fai.MgProductInfSvr.domain.serviceproc.ProductBasicService;
import fai.MgProductInfSvr.domain.serviceproc.ProductPropService;
import fai.MgProductInfSvr.interfaces.dto.ProductBasicDto;
import fai.MgProductInfSvr.interfaces.dto.ProductPropDto;
import fai.MgProductInfSvr.interfaces.entity.ProductBasicEntity;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.comm.middleground.FaiValObj;
import fai.comm.middleground.service.ServicePub;

import java.io.IOException;
import java.nio.ByteBuffer;

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

}
