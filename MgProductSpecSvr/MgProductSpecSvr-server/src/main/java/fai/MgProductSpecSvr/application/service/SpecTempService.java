package fai.MgProductSpecSvr.application.service;


import fai.MgProductSpecSvr.domain.comm.*;
import fai.MgProductSpecSvr.domain.entity.*;
import fai.MgProductSpecSvr.domain.repository.SpecStrDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecTempBizRelDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecTempDaoCtrl;
import fai.MgProductSpecSvr.domain.repository.SpecTempDetailDaoCtrl;
import fai.MgProductSpecSvr.domain.serviceProc.SpecStrProc;
import fai.MgProductSpecSvr.domain.serviceProc.SpecTempBizRelProc;
import fai.MgProductSpecSvr.domain.serviceProc.SpecTempDetailProc;
import fai.MgProductSpecSvr.domain.serviceProc.SpecTempProc;
import fai.MgProductSpecSvr.interfaces.dto.SpecTempDetailDto;
import fai.MgProductSpecSvr.interfaces.dto.SpecTempDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.*;


public class SpecTempService extends SpecParentService {
    /**
     * 批量添加规格模板
     */
    public int addTpScInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType, int tid, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;infoList=%s;", flow, aid, unionPriId, infoList);
                return rt;
            }
            // 数据校验和拆分
            FaiList<Integer> rlLibIdList = new FaiList<>(infoList.size());
            FaiList<Param> specTempList = new FaiList<>(infoList.size());
            FaiList<Param> specTempBizRelList = new FaiList<>(infoList.size());
            for (Param info : infoList) {
                String name = info.getString(SpecTempEntity.Info.NAME);
                if(!SpecTempArgCheck.isValidName(name)){
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("name err;flow=%d;aid=%d;unionPriId=%s;name=%s;", flow, aid, unionPriId, name);
                    return rt;
                }
                Param specTempInfo = new Param();
                specTempInfo.setString(SpecTempEntity.Info.NAME, name);
                specTempInfo.setInt(SpecTempEntity.Info.SOURCE_TID, tid);
                specTempInfo.setInt(SpecTempEntity.Info.SOURCE_UNION_PRI_ID, unionPriId);
                specTempList.add(specTempInfo);

                Param specTempBizRelInfo = new Param();
                Integer rlLibId = info.getInt(SpecTempBizRelEntity.Info.RL_LIB_ID);
                if(rlLibId == null){
                    rlLibId = SpecTempBizRelValObj.Default.RL_LIB_ID;
                }
                rlLibIdList.add(rlLibId);
                specTempBizRelInfo.setInt(SpecTempBizRelEntity.Info.SYS_TYPE, sysType);
                specTempBizRelInfo.setInt(SpecTempBizRelEntity.Info.RL_LIB_ID, rlLibId);
                specTempBizRelInfo.assign(info, SpecTempBizRelEntity.Info.RL_TP_SC_ID);
                specTempBizRelInfo.assign(info, SpecTempBizRelEntity.Info.SORT);
                specTempBizRelInfo.assign(info, SpecTempBizRelEntity.Info.FLAG);
                specTempBizRelList.add(specTempBizRelInfo);
            }
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                SpecTempDaoCtrl specTempDaoCtrl = SpecTempDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(specTempDaoCtrl)){
                    Log.logErr("register SpecTempDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SpecTempBizRelDaoCtrl specTempBizRelDaoCtrl = SpecTempBizRelDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(specTempBizRelDaoCtrl)){
                    Log.logErr("register SpecTempBizRelDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SpecTempProc specTempProc = new SpecTempProc(specTempDaoCtrl, flow);
                SpecTempBizRelProc specTempBizRelProc = new SpecTempBizRelProc(specTempBizRelDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    // 判断规格模板是否已经存在
                    Ref<Integer> countRef = new Ref<>();
                    rt = specTempBizRelProc.getCount(aid, unionPriId, sysType, rlLibIdList, countRef);
                    if(rt != Errno.OK){
                        Log.logErr(rt, "getCount err");
                        return rt;
                    }else if(countRef.value == null || countRef.value > 0){
                        rt = Errno.ALREADY_EXISTED;
                        Log.logErr(rt, "rlLibId existed rlLibIdList=%s;", rlLibIdList);
                        return rt;
                    }
                    // 不存在就开始添加
                    try {
                        transactionCtrl.setAutoCommit(false);
                        FaiList<Integer> rtIdList = new FaiList<>();
                        rt = specTempProc.batchAdd(aid, specTempList, rtIdList);
                        if(rt != Errno.OK){
                            Log.logErr(rt, "batchAdd err specTempList=%s", specTempList);
                            return rt;
                        }
                        for (int i = 0; i < rtIdList.size(); i++) {
                            Integer tpScId = rtIdList.get(i);
                            specTempBizRelList.get(i).setInt(SpecTempBizRelEntity.Info.TP_SC_ID, tpScId);
                        }
                        rt = specTempBizRelProc.batchAdd(aid, unionPriId, specTempBizRelList, null);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }catch (Exception e){
                        rt = Errno.ERROR;
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            specTempProc.clearIdBuilderCache(aid);
                            specTempBizRelProc.clearIdBuilderCache(aid, unionPriId);
                            return rt;
                        }
                        transactionCtrl.commit();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量修改规格模板
     */
    public int setTpScInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType, int tid, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;updaterList=%s;", flow, aid, unionPriId, updaterList);
                return rt;
            }

            FaiList<ParamUpdater> specTempUpdaterList = new FaiList<>();
            FaiList<ParamUpdater> specTempBizRelUpdaterList = new FaiList<>();
            FaiList<Integer> rlTpScIdList = new FaiList<>();
            HashMap<Integer, ParamUpdater> rlTpScId_specTempUpdaterMap = new HashMap<>();
            for (int i = 0; i < updaterList.size(); i++) {
                ParamUpdater updater = updaterList.get(i);
                Param info = updater.getData();
                Integer rlTpscId = info.getInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID);
                if(rlTpscId == null){
                    rt = Errno.ARGS_ERROR;
                    Log.logErr("rlTpscId err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt;
                }

                rlTpScIdList.add(rlTpscId);

                String name = info.getString(SpecTempEntity.Info.NAME);
                if(name != null){
                    if(!SpecTempArgCheck.isValidName(name)){
                        rt = Errno.ARGS_ERROR;
                        Log.logErr("name err;flow=%d;aid=%d;unionPriId=%s;name=%s;", flow, aid, unionPriId, name);
                        return rt;
                    }
                    ParamUpdater specTempUpdater = new ParamUpdater();
                    specTempUpdater.getData().setString(SpecTempEntity.Info.NAME, name);
                    specTempUpdater.getData().setInt(SpecTempEntity.Info.TP_SC_ID, 0);
                    specTempUpdaterList.add(specTempUpdater);
                    rlTpScId_specTempUpdaterMap.put(rlTpscId, specTempUpdater);
                }
                Integer sort = info.getInt(SpecTempBizRelEntity.Info.SORT);

                ParamUpdater specTempBizRelUpdater = new ParamUpdater();
                specTempBizRelUpdater.getData().setInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID, rlTpscId);
                if(sort != null){
                    specTempBizRelUpdater.getData().setInt(SpecTempBizRelEntity.Info.SORT, sort);
                }
                FaiList<ParamUpdater.DataOp> flagOpList = updater.getOpList(SpecTempBizRelEntity.Info.FLAG);
                if(!flagOpList.isEmpty()){
                    specTempBizRelUpdater.add(flagOpList);
                }

                specTempBizRelUpdater.add(updater.getOpList(SpecTempBizRelEntity.Info.FLAG));
                specTempBizRelUpdaterList.add(specTempBizRelUpdater);
            }

            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                SpecTempDaoCtrl specTempDaoCtrl = SpecTempDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(specTempDaoCtrl)){
                    Log.logErr("useSameDao SpecTempDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SpecTempBizRelDaoCtrl specTempBizRelDaoCtrl = SpecTempBizRelDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(specTempBizRelDaoCtrl)){
                    Log.logErr("useSameDao SpecTempBizRelDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SpecTempProc specTempProc = new SpecTempProc(specTempDaoCtrl, flow);
                SpecTempBizRelProc specTempBizRelProc = new SpecTempBizRelProc(specTempBizRelDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    rt = specTempBizRelProc.getList(aid, unionPriId, sysType, rlTpScIdList, listRef);
                    if(rt != Errno.OK){
                        Log.logErr(rt,"specTempBizRelProc getList err;flow=%d;aid=%d;unionPriId=%s;rlTpScIdList=%s;", flow, aid, unionPriId, rlTpScIdList);
                        return rt;
                    }
                    for (Param specTempBizRelInfo : listRef.value) {
                        Integer rlTpScId = specTempBizRelInfo.getInt(SpecTempBizRelEntity.Info.RL_TP_SC_ID);
                        ParamUpdater specTempUpdater = rlTpScId_specTempUpdaterMap.get(rlTpScId);
                        specTempUpdater.getData().setInt(SpecTempEntity.Info.TP_SC_ID, specTempBizRelInfo.getInt(SpecTempBizRelEntity.Info.TP_SC_ID));
                    }
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = specTempProc.batchSet(aid, specTempUpdaterList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = specTempBizRelProc.batchSet(aid, unionPriId, sysType, specTempBizRelUpdaterList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }catch (Exception e){
                        rt = Errno.ERROR;
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                        specTempBizRelProc.deleteDirtyCache(aid, unionPriId);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;", flow, aid, unionPriId, tid);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板
     */
    public int delTpScInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType, FaiList<Integer> rlTpScIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || rlTpScIdList == null || rlTpScIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScIdList=%s;", flow, aid, unionPriId, rlTpScIdList);
                return rt;
            }
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            try {
                SpecTempDaoCtrl specTempDaoCtrl = SpecTempDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(specTempDaoCtrl)){
                    Log.logErr("useSameDao SpecTempDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SpecTempBizRelDaoCtrl specTempBizRelDaoCtrl = SpecTempBizRelDaoCtrl.getInstance(flow, aid);
                if(!transactionCtrl.register(specTempBizRelDaoCtrl)){
                    Log.logErr("useSameDao SpecTempBizRelDaoCtrl err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                    return rt=Errno.ERROR;
                }
                SpecTempProc specTempProc = new SpecTempProc(specTempDaoCtrl, flow);
                SpecTempBizRelProc specTempBizRelProc = new SpecTempBizRelProc(specTempBizRelDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    // TODO 目前无复杂关联，暂不做处理
                    Ref<FaiList<Param>> listRef = new Ref<>();
                    rt = specTempBizRelProc.getList(aid, unionPriId, sysType, rlTpScIdList, listRef);
                    if(rt != Errno.OK){
                        Log.logErr(rt,"specTempBizRelProc getList err;flow=%d;aid=%d;unionPriId=%s;rlTpScIdList=%s;", flow, aid, unionPriId, rlTpScIdList);
                        return rt;
                    }
                    FaiList<Integer> tpScIdList = Utils.getValList(listRef.value, SpecTempBizRelEntity.Info.TP_SC_ID);
                    try {
                        transactionCtrl.setAutoCommit(false);
                        rt = specTempProc.batchDel(aid, tpScIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                        rt = specTempBizRelProc.batchDel(aid, unionPriId, sysType, rlTpScIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }catch (Exception e){
                        rt = Errno.ERROR;
                        throw e;
                    }finally {
                        if(rt != Errno.OK){
                            transactionCtrl.rollback();
                            return rt;
                        }
                        transactionCtrl.commit();
                        specTempBizRelProc.deleteDirtyCache(aid, unionPriId);
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                transactionCtrl.closeDao();
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取规格模板列表
     */
    public int getTpScInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;", flow, aid, unionPriId);
                return rt;
            }


            Ref<FaiList<Param>> specTempBizRelListRef = new Ref<>();
            SpecTempBizRelDaoCtrl specTempBizRelDaoCtrl = SpecTempBizRelDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempBizRelProc specTempBizRelProc = new SpecTempBizRelProc(specTempBizRelDaoCtrl, flow);
                rt = specTempBizRelProc.getList(aid, unionPriId, sysType, specTempBizRelListRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                specTempBizRelDaoCtrl.closeDao();
            }


            FaiList<Param> specTempBizRelList = specTempBizRelListRef.value;
            HashMap<Integer, Param> map = new HashMap<>(specTempBizRelList.size()*4/3+1);
            for (Param specTempBizRelInfo : specTempBizRelList) {
                map.put(specTempBizRelInfo.getInt(SpecTempBizRelEntity.Info.TP_SC_ID), specTempBizRelInfo);
            }
            Ref<FaiList<Param>> specTempListRef = new Ref<>();
            SpecTempDaoCtrl specTempDaoCtrl = SpecTempDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempProc specTempProc = new SpecTempProc(specTempDaoCtrl, flow);
                rt = specTempProc.getList(aid, new FaiList<>(map.keySet()), specTempListRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                specTempDaoCtrl.closeDao();
            }

            FaiList<Param> specTempList = specTempListRef.value;
            for (Param info : specTempList) {
                Integer tpScId = info.getInt(SpecTempEntity.Info.TP_SC_ID);
                Param specTempBizRelInfo = map.get(tpScId);
                info.assign(specTempBizRelInfo, SpecTempBizRelEntity.Info.RL_LIB_ID);
                info.assign(specTempBizRelInfo, SpecTempBizRelEntity.Info.RL_TP_SC_ID);
                info.assign(specTempBizRelInfo, SpecTempBizRelEntity.Info.SORT);
                info.assign(specTempBizRelInfo, SpecTempBizRelEntity.Info.FLAG);
            }
            ParamComparator comparator = new ParamComparator(SpecTempBizRelEntity.Info.SORT);
            Collections.sort(specTempList, comparator);
            sendInfoList(session, specTempList);
            Log.logDbg("ok;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }

    private void sendInfoList(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SpecTempDto.Key.INFO_LIST, SpecTempDto.getInfoDto());
        session.write(sendBuf);
    }

    /**
     * 批量添加规格模板详情列表
     */
    public int addTpScDetailInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlTpScId, FaiList<Param> infoList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || rlTpScId <= 0 || infoList == null || infoList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;infoList=%s", flow, aid, unionPriId, rlTpScId, infoList);
                return rt;
            }
            Integer tpScId = getTpScId(flow, aid, unionPriId, sysType, rlTpScId);
            if(tpScId == null){
                Log.logErr("tpScId not found;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                return rt = Errno.NOT_FOUND;
            }

            // 数据校验和拆分
            FaiList<String> specStrNameList = new FaiList<>(infoList.size());
            FaiList<Param> specTempDetailList = new FaiList<>(infoList.size());
            for (Param info : infoList) {
                String name = info.getString(SpecStrEntity.Info.NAME);
                name = Utils.trim(name);
                info.setString(SpecStrEntity.Info.NAME, name);
                if(!SpecStrArgCheck.isValidName(name)){
                    Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;name=%s", flow, aid, unionPriId, rlTpScId, name);
                    return rt = Errno.ARGS_ERROR;
                }
                specStrNameList.add(name);
                FaiList<Param> inScValList = info.getList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, SpecTempDetailValObj.Default.IN_SC_VAL_LIST);
                if(!SpecTempDetailArgCheck.isInScValList(inScValList)){
                    Log.logErr("arg inScValList err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;inScValList=%s", flow, aid, unionPriId, rlTpScId, inScValList);
                    return rt = Errno.ARGS_ERROR;
                }
                for (Param inScVal : inScValList) {
                    String inScValName = inScVal.getString(fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj.InScValList.Item.NAME);
                    inScValName = Utils.trim(inScValName);
                    inScVal.setString(fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj.InScValList.Item.NAME, inScValName);
                    if(!SpecStrArgCheck.isValidName(inScValName)){
                        Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;inScValName=%s", flow, aid, unionPriId, rlTpScId, inScValName);
                        return rt = Errno.ARGS_ERROR;
                    }
                    specStrNameList.add(inScValName);
                }
                int sort = info.getInt(SpecTempDetailEntity.Info.SORT, SpecTempDetailValObj.Default.SORT);

                Param specTempDetailInfo = new Param();
                specTempDetailInfo.setString(TMP_NAME_KEY, name); // 用于匹配规格字符串id
                specTempDetailInfo.setInt(SpecTempDetailEntity.Info.SORT, sort);
                specTempDetailInfo.setList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, inScValList);
                specTempDetailList.add(specTempDetailInfo);
            }
            // 将规格名称都替换成规格字符串id
            rt = replaceNameToScStrId(flow, aid, specStrNameList, specTempDetailList);
            if(rt != Errno.OK){
                return rt;
            }
            SpecTempDetailDaoCtrl specTempDetailDaoCtrl = SpecTempDetailDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempDetailProc specTempDetailProc = new SpecTempDetailProc(specTempDetailDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        specTempDetailDaoCtrl.setAutoCommit(false);
                        rt = specTempDetailProc.batchAdd(aid, tpScId, specTempDetailList, null);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            specTempDetailDaoCtrl.rollback();
                            specTempDetailProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        specTempDetailDaoCtrl.commit();
                    }

                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                specTempDetailDaoCtrl.closeDao();
            }
            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;unionPriId=%d;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.ALREADY_EXISTED, rt);
        }
        return rt;
    }
    /**
     * 将规格字符串转为对应的id
     */
    private int replaceNameToScStrId(int flow, int aid, FaiList<String> specStrNameList, FaiList<Param> specTempDetailList) {
        Param nameIdMap = new Param(true); // mapMode
        SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
        try {
            SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
            try {
                LockUtil.lock(aid);
                int rt = specStrProc.getListWithBatchAdd(aid, new FaiList<>(new HashSet<>(specStrNameList)), nameIdMap);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                LockUtil.unlock(aid);
            }
        }finally {
            specStrDaoCtrl.closeDao();
        }
        // 将规格值替换成对应的id
        for (Param specTempDetailInfo : specTempDetailList) {
            String tmpName = (String)specTempDetailInfo.remove(TMP_NAME_KEY);
            Integer scStrId = nameIdMap.getInt(tmpName);
            if(scStrId == null){
                Log.logErr("nameIdMap get err tmpName=%s", tmpName);
                return Errno.ERROR;
            }
            specTempDetailInfo.setInt(SpecTempDetailEntity.Info.SC_STR_ID, scStrId);
            FaiList<Param> inScValList = specTempDetailInfo.getListNullIsEmpty(SpecTempDetailEntity.Info.IN_SC_VAL_LIST);
            for (Param inScValInfo : inScValList) {
                String name = (String)inScValInfo.remove(fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj.InScValList.Item.NAME);
                scStrId = nameIdMap.getInt(name);
                if(scStrId == null){
                    Log.logErr("nameIdMap get err tmpName=%s", tmpName);
                    return Errno.ERROR;
                }
                inScValInfo.setInt(SpecTempDetailValObj.InScValList.Item.SC_STR_ID, scStrId);
            }
        }
        return Errno.OK;
    }
    /**
     * 批量修改规格模板详情列表
     */
    public int setTpScDetailInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlTpScId, FaiList<ParamUpdater> updaterList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || rlTpScId <= 0 || updaterList == null || updaterList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;updaterList=%s;", flow, aid, unionPriId, rlTpScId, updaterList);
                return rt;
            }

            Integer tpScId = getTpScId(flow, aid, unionPriId, sysType, rlTpScId);
            if(tpScId == null){
                Log.logErr("tpScId not found;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                return rt = Errno.NOT_FOUND;
            }
            // 数据校验和拆分
            FaiList<String> specStrNameList = new FaiList<>(updaterList.size());
            FaiList<Param> specTempDetailList = new FaiList<>(updaterList.size());
            FaiList<ParamUpdater> specTempDetailUpdaterList = new FaiList<>();
            for (ParamUpdater updater : updaterList) {
                Param info = updater.getData();
                Integer tpScDtId = info.getInt(SpecTempDetailEntity.Info.TP_SC_DT_ID);
                if(tpScDtId == null){
                    Log.logErr("tpScDtId is null;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                    return rt = Errno.ARGS_ERROR;
                }
                String name = info.getString(SpecStrEntity.Info.NAME);
                if(name != null){
                    name = Utils.trim(name);
                    info.setString(SpecStrEntity.Info.NAME, name);
                    if(!SpecStrArgCheck.isValidName(name)){
                        Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;name=%s", flow, aid, unionPriId, rlTpScId, name);
                        return rt = Errno.ARGS_ERROR;
                    }
                    specStrNameList.add(name);
                }

                FaiList<Param> inScValList = info.getList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST);
                if(inScValList != null){
                    if(!SpecTempDetailArgCheck.isInScValList(inScValList)){
                        Log.logErr("arg inScValList err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;inScValList=%s", flow, aid, unionPriId, rlTpScId, inScValList);
                        return rt = Errno.ARGS_ERROR;
                    }
                    for (Param inScVal : inScValList) {
                        String inScValName = inScVal.getString(fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj.InScValList.Item.NAME);
                        if(inScValName != null){
                            inScValName = Utils.trim(inScValName);
                            inScVal.setString(fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj.InScValList.Item.NAME, inScValName);
                        }
                        if(!SpecStrArgCheck.isValidName(inScValName)){
                            Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;inScValName=%s", flow, aid, unionPriId, rlTpScId, inScValName);
                            return rt = Errno.ARGS_ERROR;
                        }
                        specStrNameList.add(inScValName);
                    }
                }
                Param data = new Param();
                data.setString(TMP_NAME_KEY, name);
                data.setInt(SpecTempDetailEntity.Info.TP_SC_DT_ID, tpScDtId);
                data.setList(SpecTempDetailEntity.Info.IN_SC_VAL_LIST, inScValList);
                data.assign(info, SpecTempDetailEntity.Info.SORT);
                ParamUpdater specTempDetailUpdater = new ParamUpdater(data);
                FaiList<ParamUpdater.DataOp> flagOpList = updater.getOpList(SpecTempDetailEntity.Info.FLAG);
                if(!flagOpList.isEmpty()){
                    specTempDetailUpdater.add(flagOpList);
                }
                specTempDetailList.add(data);
                specTempDetailUpdaterList.add(specTempDetailUpdater);
            }

            rt = replaceNameToScStrId(flow, aid, specStrNameList, specTempDetailList);
            if(rt != Errno.OK){
                return rt;
            }
            SpecTempDetailDaoCtrl specTempDetailDaoCtrl = SpecTempDetailDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempDetailProc specTempDetailProc = new SpecTempDetailProc(specTempDetailDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        specTempDetailDaoCtrl.setAutoCommit(false);
                        rt = specTempDetailProc.batchSet(aid, tpScId, specTempDetailUpdaterList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            specTempDetailDaoCtrl.rollback();
                            specTempDetailProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        specTempDetailDaoCtrl.commit();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                specTempDetailDaoCtrl.closeDao();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;unionPriId=%d;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.ALREADY_EXISTED, rt);
        }
        return rt;
    }
    /**
     * 批量删除规格模板详情列表
     */
    public int delTpScDetailInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlTpScId, FaiList<Integer> tpScDtIdList) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || rlTpScId <= 0 || tpScDtIdList == null || tpScDtIdList.isEmpty()) {
                rt = Errno.ARGS_ERROR;
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;tpScDtIdList=%s;", flow, aid, unionPriId, rlTpScId, tpScDtIdList);
                return rt;
            }
            Integer tpScId = getTpScId(flow, aid, unionPriId, sysType, rlTpScId);
            if(tpScId == null){
                Log.logErr("tpScId not found;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                return rt = Errno.NOT_FOUND;
            }
            SpecTempDetailDaoCtrl specTempDetailDaoCtrl = SpecTempDetailDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempDetailProc specTempDetailProc = new SpecTempDetailProc(specTempDetailDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    try {
                        specTempDetailDaoCtrl.setAutoCommit(false);
                        rt = specTempDetailProc.batchDel(aid, tpScId, tpScDtIdList);
                        if(rt != Errno.OK){
                            return rt;
                        }
                    }finally {
                        if(rt != Errno.OK){
                            specTempDetailDaoCtrl.rollback();
                            specTempDetailProc.clearIdBuilderCache(aid);
                            return rt;
                        }
                        specTempDetailDaoCtrl.commit();
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                specTempDetailDaoCtrl.closeDao();
            }

            rt = Errno.OK;
            FaiBuffer sendBuf = new FaiBuffer(true);
            session.write(sendBuf);
            Log.logStd("ok;flow=%d;aid=%d;unionPriId=%d;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
        }finally {
            stat.end(rt != Errno.OK, rt);
        }
        return rt;
    }
    /**
     * 获取规格模板详情列表
     */
    public int getTpScDetailInfoList(FaiSession session, int flow, int aid, int unionPriId, int sysType, int rlTpScId) throws IOException {
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || rlTpScId <= 0) {
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                return  rt = Errno.ARGS_ERROR;
            }
            Integer tpScId = getTpScId(flow, aid, unionPriId, sysType, rlTpScId);
            if(tpScId == null){
                Log.logErr("tpScId not found;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                return rt = Errno.NOT_FOUND;
            }
            Ref<FaiList<Param>> specTempDetailListRef = new Ref<>();
            SpecTempDetailDaoCtrl specTempDetailDaoCtrl = SpecTempDetailDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempDetailProc specTempDetailProc = new SpecTempDetailProc(specTempDetailDaoCtrl, flow);
                rt = specTempDetailProc.getListFromDaoByTpScDtIdList(aid, tpScId, null, specTempDetailListRef);
                if(rt != Errno.OK){
                    return rt;
                }
            }finally {
                specTempDetailDaoCtrl.closeDao();
            }
            Set<Integer> scStrIdSet = new HashSet<>();
            FaiList<Param> specTempDetailList = specTempDetailListRef.value;
            for (Param specTempDetailInfo : specTempDetailList) {
                scStrIdSet.add(specTempDetailInfo.getInt(SpecTempDetailEntity.Info.SC_STR_ID));
                FaiList<Param> inScValList = specTempDetailInfo.getListNullIsEmpty(SpecTempDetailEntity.Info.IN_SC_VAL_LIST);
                for (Param inScValInfo : inScValList) {
                    scStrIdSet.add(inScValInfo.getInt(SpecTempDetailValObj.InScValList.Item.SC_STR_ID, 0));
                }
            }
            Ref<FaiList<Param>> listRef = new Ref<>();
            SpecStrDaoCtrl specStrDaoCtrl = SpecStrDaoCtrl.getInstance(flow, aid);
            try {
                SpecStrProc specStrProc = new SpecStrProc(specStrDaoCtrl, flow);
                try {
                    LockUtil.lock(aid);
                    rt = specStrProc.getList(aid, new FaiList<>(scStrIdSet), listRef);
                    if(rt != Errno.OK){
                        return rt;
                    }
                }finally {
                    LockUtil.unlock(aid);
                }
            }finally {
                specStrDaoCtrl.closeDao();
            }
            // 初始化，通过规格字符串id 填充规格字符串
            Map<Object, Param> specStrMap = Utils.getMap(listRef.value, SpecStrEntity.Info.SC_STR_ID);
            for (Param specTempDetailInfo : specTempDetailList) {
                Integer scStrId = specTempDetailInfo.getInt(SpecTempDetailEntity.Info.SC_STR_ID);
                Param specStrInfo = specStrMap.get(scStrId);
                if(Str.isEmpty(specStrInfo)){
                    Log.logErr("specStrInfo is null aid=%s;scStrId=%s", aid, scStrId);
                    return rt = Errno.ERROR;
                }
                String name = specStrInfo.getString(SpecStrEntity.Info.NAME);
                specTempDetailInfo.setString(fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailEntity.Info.NAME, name);
                FaiList<Param> inScValList = specTempDetailInfo.getListNullIsEmpty(SpecTempDetailEntity.Info.IN_SC_VAL_LIST);
                for (Param inScValInfo : inScValList) {
                    scStrId = inScValInfo.getInt(SpecTempDetailValObj.InScValList.Item.SC_STR_ID, 0);
                    specStrInfo = specStrMap.get(scStrId);
                    if(Str.isEmpty(specStrInfo)){
                        Log.logErr("specStrInfo is null aid=%s;scStrId=%s", aid, scStrId);
                        return rt = Errno.ERROR;
                    }
                    name = specStrInfo.getString(SpecStrEntity.Info.NAME);
                    inScValInfo.setString(fai.MgProductSpecSvr.interfaces.entity.SpecTempDetailValObj.InScValList.Item.NAME, name);
                }
            }

            ParamComparator comparator = new ParamComparator(SpecTempDetailEntity.Info.SORT);
            Collections.sort(specTempDetailList, comparator);
            sendDetailInfoList(session, specTempDetailList);
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
        return rt;
    }
    private void sendDetailInfoList(FaiSession session, FaiList<Param> infoList) throws IOException {
        FaiBuffer sendBuf = new FaiBuffer(true);
        infoList.toBuffer(sendBuf, SpecTempDetailDto.Key.INFO_LIST, SpecTempDetailDto.getInfoDto());
        session.write(sendBuf);
    }

    /**
     * 获取规格模板和规格模板详情
     * @return
     *  {
     *      "tpScId":1,
     *      "sourceTid":1,
     *      "detailList":[{@link fai.MgProductSpecSvr.domain.entity.SpecTempDetailEntity}...]
     *  }
     */
    public Param getTpScWithDetail(int flow, int aid, int unionPriId, int sysType, int rlTpScId, FaiList<Integer> tpScDtIdList){
        int rt = Errno.ERROR;
        Oss.SvrStat stat = new Oss.SvrStat(flow);
        try {
            if (aid <= 0 || unionPriId <= 0 || rlTpScId <= 0) {
                Log.logErr("arg err;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                return null;
            }
            Integer tpScId = getTpScId(flow, aid, unionPriId, sysType, rlTpScId);
            if(tpScId == null){
                Log.logErr("tpScId not found;flow=%d;aid=%d;unionPriId=%s;rlTpScId=%s;", flow, aid, unionPriId, rlTpScId);
                return null;
            }
            Ref<Param> specTempInfoRef = new Ref<>();
            SpecTempDaoCtrl specTempDaoCtrl = SpecTempDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempProc specTempProc = new SpecTempProc(specTempDaoCtrl, flow);
                rt = specTempProc.get(aid, tpScId, specTempInfoRef);
                if(rt != Errno.OK){
                    return null;
                }
            }finally {
                specTempDaoCtrl.closeDao();
            }

            int sourceTid = specTempInfoRef.value.getInt(SpecTempEntity.Info.SOURCE_TID);
            Ref<FaiList<Param>> specTempDetailInfoListRef = new Ref<>();
            SpecTempDetailDaoCtrl specTempDetailDaoCtrl = SpecTempDetailDaoCtrl.getInstance(flow, aid);
            try {
                SpecTempDetailProc specTempDetailProc = new SpecTempDetailProc(specTempDetailDaoCtrl, flow);
                rt = specTempDetailProc.getListFromDaoByTpScDtIdList(aid, tpScId, tpScDtIdList, specTempDetailInfoListRef);
                if(rt != Errno.OK){
                    return null;
                }
            }finally {
                specTempDetailDaoCtrl.closeDao();
            }

            Param result = new Param();
            result.setInt("tpScId", tpScId);
            result.setInt("sourceTid", sourceTid);
            result.setList("detailList", specTempDetailInfoListRef.value);
            return result;
        }finally {
            stat.end(rt != Errno.OK && rt != Errno.NOT_FOUND, rt);
        }
    }

    private Integer getTpScId(int flow, int aid, int unionPriId, int sysType, int rlTpScId) {
        SpecTempBizRelDaoCtrl specTempBizRelDaoCtrl = SpecTempBizRelDaoCtrl.getInstance(flow, aid);
        try {
            SpecTempBizRelProc specTempBizRelProc = new SpecTempBizRelProc(specTempBizRelDaoCtrl, flow);
            Ref<Integer> tpScIdRef = new Ref<>();
            specTempBizRelProc.getTpScIdByRlTpScId(aid, unionPriId, sysType, rlTpScId, tpScIdRef);
            return tpScIdRef.value;
        }finally {
            specTempBizRelDaoCtrl.closeDao();
        }
    }

    private static final String TMP_NAME_KEY = "tmpNameKey";


}
