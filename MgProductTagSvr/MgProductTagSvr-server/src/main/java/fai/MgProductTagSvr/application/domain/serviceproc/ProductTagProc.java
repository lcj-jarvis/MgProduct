package fai.MgProductTagSvr.application.domain.serviceproc;

import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.entity.ProductTagEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagCache;
import fai.MgProductTagSvr.application.domain.repository.dao.ProductTagDaoCtrl;
import fai.comm.util.*;
import fai.mgproduct.comm.Util;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

/**
 * @author LuChaoJi
 * @date 2021-07-12 14:03
 */
public class ProductTagProc {

    private int m_flow;
    private ProductTagDaoCtrl m_daoCtrl;

    public ProductTagProc(int flow, int aid, TransactionCtrl transactionCrtl) {
        this.m_flow = flow;
        this.m_daoCtrl = ProductTagDaoCtrl.getInstance(flow, aid);
        init(transactionCrtl);
    }

    private void init(TransactionCtrl transactionCrtl) {
        if (transactionCrtl == null) {
            throw new MgException("TransactionCtrl is null , registered ProductTagDao err;");
        }
        if(!transactionCrtl.register(m_daoCtrl)) {
            throw new MgException("registered ProductTagDao err;");

        }
    }

    /**
     * 构建自增tagId
     * @param aid
     * @param tagInfo
     * @return
     */
    private int creatAndSetId(int aid, Param tagInfo) {
        Integer tagId = tagInfo.getInt(ProductTagEntity.Info.TAG_ID, 0);
        if(tagId <= 0) {
            tagId = m_daoCtrl.buildId(aid, false);
            if (tagId == null) {
                throw new MgException(Errno.ERROR, "tagId build error;flow=%d;aid=%d;", m_flow, aid);
            }
        }else {
            tagId = m_daoCtrl.updateId(aid, tagId, false);
            if (tagId == null) {
                throw new MgException(Errno.ERROR, "tagId update error;flow=%d;aid=%d;", m_flow, aid);
            }
        }
        tagInfo.setInt(ProductTagEntity.Info.TAG_ID, tagId);

        return tagId;
    }

    public void clearIdBuilderCache(int aid) {
        m_daoCtrl.clearIdBuilderCache(aid);
    }

    /**
     * 批量添加标签表的数据
     * @param tagInfoList 保存到标签表的数据
     * @param tagIdsRef 接收标签id
     */
    public void addTagBatch(int aid, FaiList<Param> tagInfoList, FaiList<Integer> tagIdsRef) {
        int rt;
        if(Util.isEmptyList(tagInfoList)) {
            rt = Errno.ARGS_ERROR;
            throw new MgException(rt, "args err, infoList is empty;flow=%d;aid=%d;tagInfo=%s", m_flow, aid, tagInfoList);
        }

        FaiList<Param> list = getTagList(aid,null,true);
        int count = list.size();
        //判断是否超出数量限制
        boolean isOverLimit = (count >= ProductTagValObj.Limit.COUNT_MAX) ||
                (count + tagInfoList.size() >  ProductTagValObj.Limit.COUNT_MAX);
        if(isOverLimit) {
            rt = Errno.COUNT_LIMIT;
            throw new MgException(rt, "over limit;flow=%d;aid=%d;currentCount=%d;limit=%d;wantAddSize=%d;", m_flow, aid,
                    count, ProductTagValObj.Limit.COUNT_MAX, tagInfoList.size());
        }

        int tagId = 0;
        //检查名称是否合法
        for (Param tagInfo:tagInfoList) {
            String tagName = tagInfo.getString(ProductTagEntity.Info.TAG_NAME);
            Param existInfo = Misc.getFirst(list, ProductTagEntity.Info.TAG_NAME, tagName);
            if(!Str.isEmpty(existInfo)) {
                rt = Errno.ALREADY_EXISTED;
                throw new MgException(rt, "tag name is existed;flow=%d;aid=%d;name=%s;", m_flow, aid, tagName);
            }
            //自增标签id
            tagId = creatAndSetId(aid, tagInfo);
            //保存tagId
            tagIdsRef.add(tagId);
        }

        //批量插入，并且不将tagInfoList的元素设置为null
        rt = m_daoCtrl.batchInsert(tagInfoList, null, false);

        if(rt != Errno.OK) {
            throw new MgException(rt, "batch insert product tag error;flow=%d;aid=%d;", m_flow, aid);

        }
    }

    public FaiList<Param> getTagList(int aid, SearchArg searchArg, boolean getFromCache) {
        return getListByConditions(aid, searchArg, getFromCache);
    }

    /**
     * 按照条件查询数据，默认是查询同一个aid下的全部数据.
     * (该方法方便后期扩展只查DB的情形)
     * @param searchArg 查询条件
     * @param getFromCache 是否需要从缓存中查询
     * @return
     */
    private FaiList<Param> getListByConditions(int aid, SearchArg searchArg, boolean getFromCache) {
        FaiList<Param> list;
        if (getFromCache) {
            // 从缓存获取数据
            list = ProductTagCache.getCacheList(aid);
            if(!Util.isEmptyList(list)) {
                return list;
            }
        }

        LockUtil.TagLock.readLock(aid);
        try {
            if (getFromCache) {
                // check again
                list = ProductTagCache.getCacheList(aid);
                if(!Util.isEmptyList(list)) {
                    return list;
                }
            }

            //无searchArg
            if (searchArg == null) {
                searchArg = new SearchArg();
            }

            //有searchArg，无查询条件
            if (searchArg.matcher == null) {
                searchArg.matcher = new ParamMatcher();
            }

            //如果查询过来的条件已经包含这两个查询条件,就先删除
            searchArg.matcher.remove(ProductTagEntity.Info.AID);

            //有searchArg，有查询条件，加多一个查询条件
            searchArg.matcher.and(ProductTagRelEntity.Info.AID, ParamMatcher.EQ, aid);

            Ref<FaiList<Param>> listRef = new Ref<>();
            int rt = m_daoCtrl.select(searchArg, listRef);
            if(rt != Errno.OK && rt != Errno.NOT_FOUND) {
                throw new MgException(rt, "get error;flow=%d;aid=%d;", m_flow, aid);
            }
            list = listRef.value;
            if(list == null) {
                list = new FaiList<Param>();
            }
            if (list.isEmpty()) {
                rt = Errno.NOT_FOUND;
                Log.logDbg(rt, "not found;flow=%d;aid=%d;", m_flow, aid);
                return list;
            }
            //添加到缓存（直接查DB的不需要添加缓存）
            if (getFromCache) {
                ProductTagCache.addCacheList(aid, list);
            }
        }finally {
            LockUtil.TagLock.readUnLock(aid);
        }

        return list;
    }
}
