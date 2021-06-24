package fai.MgProductLibSvr.application.service;

import fai.MgProductLibSvr.domain.common.LockUtil;
import fai.MgProductLibSvr.domain.common.ProductLibCheck;
import fai.MgProductLibSvr.domain.entity.ProductLibEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibRelValObj;
import fai.MgProductLibSvr.domain.entity.ProductLibValObj;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibCache;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibRelCache;
import fai.MgProductLibSvr.domain.serviceproc.ProductLibProc;
import fai.MgProductLibSvr.domain.serviceproc.ProductLibRelProc;
import fai.MgProductLibSvr.interfaces.dto.ProductLibRelDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:18
 */
public class ProductLibService {

    @SuccessRt(value = Errno.OK)
    public int addProductLib(FaiSession session, int flow, int aid, int unionPriId, int tid, Param info) throws IOException {
        int rt;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, info is empty;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        int libId;
        int rlLibId;
        int maxSort;

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            ProductLibRelProc libRelProc = new ProductLibRelProc(flow, aid, transactionCtrl);
            // 获取参数中最大的sort
            maxSort = libRelProc.getMaxSort(aid, unionPriId);
            if(maxSort < 0) {
                rt = Errno.ERROR;
                Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            // 未设置排序则默认排序值+1
            Integer sort = info.getInt(ProductLibRelEntity.Info.SORT);
            if(sort == null) {
                info.setInt(ProductLibRelEntity.Info.SORT, ++maxSort);
            }
            Param libInfo = new Param();
            Param relLibInfo = new Param();
            assemblyLibInfo(flow, aid, unionPriId, tid, info, libInfo, relLibInfo);

            boolean commit = false;
            transactionCtrl.setAutoCommit(false);

            ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
            try {
                libId = libProc.addLib(aid, libInfo);
                relLibInfo.setInt(ProductLibRelEntity.Info.LIB_ID, libId);

                rlLibId = libRelProc.addLibRelInfo(aid, unionPriId, relLibInfo);
                commit = true;
            } finally {
                if(commit) {
                    transactionCtrl.commit();
                    // 新增缓存
                    ProductLibCache.addCache(aid, libInfo);
                    ProductLibRelCache.InfoCache.addCache(aid, unionPriId, relLibInfo);
                    ProductLibRelCache.SortCache.set(aid, unionPriId, maxSort);
                    ProductLibRelCache.DataStatusCache.update(aid, unionPriId, 1);
                }else {
                    transactionCtrl.rollback();
                    libProc.clearIdBuilderCache(aid);
                    libRelProc.clearIdBuilderCache(aid, unionPriId);
                }
                transactionCtrl.closeDao();
            }
        }finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        sendBuf.putInt(ProductLibRelDto.Key.RL_LIB_ID, rlLibId);
        sendBuf.putInt(ProductLibRelDto.Key.LIB_ID, libId);
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlLibId=%d;libId=%d;", flow, aid, unionPriId, tid, rlLibId, libId);
        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int delLibList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlLibIds) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int setLibList(FaiSession session, int flow, int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int getLibList(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) throws IOException {
        int rt;
        if(aid <= 0) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, aid error;flow=%d;aid=%d;", flow, aid);
            return rt;
        }
        FaiList<Param> relLibList;
        FaiList<Param> libList;
        TransactionCtrl transactionCtrl = new TransactionCtrl();
        try {
            ProductLibRelProc relLibProc = new ProductLibRelProc(flow, aid, transactionCtrl);
            relLibList = relLibProc.getLibRelList(aid, unionPriId);

            ProductLibProc libProc = new ProductLibProc(flow, aid, transactionCtrl);
            libList = libProc.getLibList(aid);
        }finally {
            transactionCtrl.closeDao();
        }

        // 数据整合
        HashMap<Integer, Param> relMap = new HashMap<Integer, Param>();
        for(int i = 0; i < libList.size(); i++) {
            Param info = libList.get(i);
            Integer libId = info.getInt(ProductLibEntity.Info.LIB_ID);
            relMap.put(libId, info);
        }
        for(int i = 0; i < relLibList.size(); i++) {
            Param info = relLibList.get(i);
            Integer libId = info.getInt(ProductLibRelEntity.Info.LIB_ID);

            Param libInfo = relMap.get(libId);
            if(libInfo == null) {
                rt = Errno.ERROR;
                Log.logErr(rt, "data error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
                return rt;
            }
            info.assign(libInfo);
        }

        if(searchArg.matcher == null) {
            searchArg.matcher = new ParamMatcher();
        }
        if(searchArg.cmpor == null) {
            searchArg.cmpor = new ParamComparator();
            searchArg.cmpor.addKey(ProductLibRelEntity.Info.SORT);
        }
        searchArg.cmpor.addKey(ProductLibRelEntity.Info.RL_LIB_ID);

        Searcher searcher = new Searcher(searchArg);
        FaiList<Param> list = searcher.getParamList(relLibList);

        FaiBuffer sendBuf = new FaiBuffer(true);
        list.toBuffer(sendBuf, ProductLibRelDto.Key.INFO_LIST, ProductLibRelDto.getAllInfoDto());
        if (searchArg.totalSize != null && searchArg.totalSize.value != null) {
            sendBuf.putInt(ProductLibRelDto.Key.TOTAL_SIZE, searchArg.totalSize.value);
        }
        session.write(sendBuf);
        rt = Errno.OK;
        Log.logDbg("get list ok;flow=%d;aid=%d;unionPriId=%d;size=%d;", flow, aid, unionPriId, list.size());

        return rt;
    }

    @SuccessRt(value = Errno.OK)
    public int getAllLibRel(FaiSession session, int flow, int aid, int uninoPriId) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int getLibRelDataStatus(FaiSession session, int flow, int aid, int unionPriId) {
        return 0;
    }

    @SuccessRt(value = Errno.OK)
    public int getLibRelFromDb(FaiSession session, int flow, int aid, int uninoPriId, SearchArg searchArg) {
       return 0;
    }

    /**
     * 装配库表和库业务表的数据
     */
    private void assemblyLibInfo(int flow, int aid, int unionPriId, int tid, Param recvInfo, Param  libInfo, Param relLibInfo) {

        String libName = recvInfo.getString(ProductLibEntity.Info.LIB_NAME, "");
        if(!ProductLibCheck.isNameValid(libName)) {
            throw new MgException(Errno.ARGS_ERROR, "libName is not valid;flow=%d;aid=%d;libName=%d;", flow, aid, libName);
        }

        //库类型如果没有获取到，这里没有设置默认值
        int libType = recvInfo.getInt(ProductLibEntity.Info.LIB_TYPE);

        int flag = recvInfo.getInt(ProductLibEntity.Info.FLAG, ProductLibValObj.Default.FLAG);
        int sort = recvInfo.getInt(ProductLibRelEntity.Info.SORT, ProductLibRelValObj.Default.SORT);
        int rlFlag = recvInfo.getInt(ProductLibRelEntity.Info.RL_FLAG, ProductLibRelValObj.Default.RL_FLAG);
        Calendar now = Calendar.getInstance();
        Calendar createTime = recvInfo.getCalendar(ProductLibEntity.Info.CREATE_TIME, now);
        Calendar updateTime = recvInfo.getCalendar(ProductLibEntity.Info.UPDATE_TIME, now);

        // 库表数据
        libInfo.setInt(ProductLibEntity.Info.AID, aid);
        libInfo.setInt(ProductLibEntity.Info.SOURCE_TID, tid);
        libInfo.setInt(ProductLibEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        libInfo.setString(ProductLibEntity.Info.LIB_NAME, libName);
        libInfo.setInt(ProductLibEntity.Info.LIB_TYPE, libType);
        libInfo.setInt(ProductLibEntity.Info.FLAG, flag);
        libInfo.setCalendar(ProductLibEntity.Info.CREATE_TIME, createTime);
        libInfo.setCalendar(ProductLibEntity.Info.UPDATE_TIME, updateTime);

        // 库业务表数据
        relLibInfo.setInt(ProductLibRelEntity.Info.AID, aid);
        relLibInfo.setInt(ProductLibRelEntity.Info.UNION_PRI_ID, unionPriId);
        relLibInfo.setInt(ProductLibRelEntity.Info.LIB_TYPE, libType);
        relLibInfo.setInt(ProductLibRelEntity.Info.SORT, sort);
        relLibInfo.setInt(ProductLibRelEntity.Info.RL_FLAG, rlFlag);
        relLibInfo.setCalendar(ProductLibRelEntity.Info.CREATE_TIME, createTime);
        relLibInfo.setCalendar(ProductLibRelEntity.Info.UPDATE_TIME, updateTime);

    }
}
