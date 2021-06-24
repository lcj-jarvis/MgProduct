package fai.MgProductLibSvr.domain.serviceproc;

import fai.MgProductLibSvr.domain.common.LockUtil;
import fai.MgProductLibSvr.domain.entity.ProductLibEntity;
import fai.MgProductLibSvr.domain.entity.ProductLibValObj;
import fai.MgProductLibSvr.domain.repository.cache.ProductLibCache;
import fai.MgProductLibSvr.domain.repository.dao.ProductLibDaoCtrl;
import fai.comm.distributedkit.idBuilder.domain.IdBuilderConfig;
import fai.comm.distributedkit.idBuilder.wrapper.IdBuilderWrapper;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.DaoCtrl;
import fai.middleground.svrutil.repository.TransactionCtrl;

import static fai.app.FodderDef.ComeFrom.getList;
import static java.awt.SystemColor.info;

/**
 * @author LuChaoJi
 * @date 2021-06-23 14:23
 */
public class ProductLibProc {

    private int m_flow;
    private ProductLibDaoCtrl m_daoCtrl;

    public ProductLibProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_daoCtrl = ProductLibDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    private void init(TransactionCtrl transactionCrtl) {
        if (transactionCrtl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductLibDao err;");
        }
        if(!transactionCrtl.register(m_daoCtrl)) {
            throw new MgException("registered ProductLibDao err;");

        }
    }

    /**
     * 添加库表中的数据，同一个aid下的库不能超过100
     */
    public int addLib(int aid, Param libInfo) {
        int rt;
        if(Str.isEmpty(libInfo)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;libInfo=%s", m_flow, aid, libInfo);
        }

        FaiList<Param> list = getLibList(aid);
        int count = list.size();
        if(count >= ProductLibValObj.Limit.COUNT_MAX) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;count=%d;limit=%d;", m_flow, aid, count, ProductLibValObj.Limit.COUNT_MAX);
        }

        String libName = libInfo.getString(ProductLibEntity.Info.LIB_NAME);
        Param existInfo = Misc.getFirst(list, ProductLibEntity.Info.LIB_NAME, libName);
        if(!Str.isEmpty(existInfo)) {
            rt = Errno.ALREADY_EXISTED;
            throw new MgException(rt, "lib name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, libName);
        }

        int libId = creatAndSetId(aid, libInfo);
        rt = m_daoCtrl.insert(libInfo);
        if(rt != Errno.OK) {
            throw new MgException(rt, "insert product group error;flow=%d;aid=%d;groupId=%d;", m_flow, aid, libId);
        }

        return libId;
    }

    private int creatAndSetId(int aid, Param libInfo) {
        Integer libId = libInfo.getInt(ProductLibEntity.Info.LIB_ID, 0);
        if(libId <= 0) {
            libId = m_daoCtrl.buildId(aid, false);
            if (libId == null) {
                throw new MgException(Errno.ERROR, "libId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            libId = m_daoCtrl.updateId(aid, libId, false);
            if (libId == null) {
                throw new MgException(Errno.ERROR, "libId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        libInfo.setInt(ProductLibEntity.Info.LIB_ID, libId);

        return libId;
    }

    public FaiList<Param> getLibList(int aid) {
        return getList(aid);
    }

    private FaiList<Param> getList(int aid) {
        // 从缓存获取数据
        FaiList<Param> list = ProductLibCache.getCacheList(aid);
        if(!Util.isEmptyList(list)) {
            return list;
        }

        LockUtil.LibLock.readLock(aid);
        try {
            // check again
            list = ProductLibCache.getCacheList(aid);
            if(!Util.isEmptyList(list)) {
                return list;
            }

            Ref<FaiList<Param>> listRef = new Ref<>();
            // 从db获取数据
            SearchArg searchArg = new SearchArg();
            searchArg.matcher = new ParamMatcher(ProductLibEntity.Info.AID, ParamMatcher.EQ, aid);
            int rt = m_daoCtrl.select(searchArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                throw new MgException(rt, "getList error;flow=%d;aid=%d;", m_flow, aid);
            }
            list = listRef.value;
            if(list == null) {
                list = new FaiList<Param>();
            }
            if (list.isEmpty()) {
                rt = Errno.NOT_FOUND;
                Log.logDbg(rt, "not found;aid=%d", aid);
                return list;
            }
            // 添加到缓存
            ProductLibCache.addCacheList(aid, list);
        }finally {
            LockUtil.LibLock.readUnLock(aid);
        }

        return list;
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }
}
