package fai.MgProductTagSvr.application.service;

import fai.MgProductTagSvr.application.domain.common.LockUtil;
import fai.MgProductTagSvr.application.domain.common.ProductTagCheck;
import fai.MgProductTagSvr.application.domain.entity.ProductTagEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelEntity;
import fai.MgProductTagSvr.application.domain.entity.ProductTagRelValObj;
import fai.MgProductTagSvr.application.domain.entity.ProductTagValObj;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagCache;
import fai.MgProductTagSvr.application.domain.repository.cache.ProductTagRelCache;
import fai.MgProductTagSvr.application.domain.serviceproc.ProductTagProc;
import fai.MgProductTagSvr.application.domain.serviceproc.ProductTagRelProc;
import fai.MgProductTagSvr.interfaces.dto.ProductTagRelDto;
import fai.comm.jnetkit.server.fai.FaiSession;
import fai.comm.util.*;
import fai.middleground.svrutil.annotation.SuccessRt;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.repository.TransactionCtrl;

import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.locks.Lock;

/**
 * @author LuChaoJi
 * @date 2021-07-12 13:51
 */
public class ProductTagService {

    /**
     * 批量添加商品标签
     * @param addInfoList  保存完整的标签信息（包含标签表和标签业务）
     * @param tagInfoList  用于保存要插入标签表的数据
     * @param relTagInfoList  用于保存要插入标签业务表的数据
     * @param relTagIds  保存插入成功后的标签业务id
     * @return
     */
    private int addTagBatch(int flow, int aid, int unionPriId, int tid,
                            TransactionCtrl transactionCtrl,
                            ProductTagProc tagProc,
                            ProductTagRelProc relTagProc,
                            FaiList<Param> addInfoList,
                            FaiList<Param> tagInfoList,
                            FaiList<Param> relTagInfoList,
                            FaiList<Integer> relTagIds) {

        int rt;
        ProductTagRelProc tagRelProc = new ProductTagRelProc(flow, aid, transactionCtrl);

        // 获取参数中最大的sort
        int maxSort = tagRelProc.getMaxSort(aid, unionPriId);
        if(maxSort < 0) {
            rt = Errno.ERROR;
            Log.logErr(rt, "getMaxSort error;flow=%d;aid=%d;unionPriId=%d;", flow, aid, unionPriId);
            return rt;
        }

        for (Param addInfo : addInfoList) {
            // 未设置排序则默认排序值+1
            Integer sort = addInfo.getInt(ProductTagRelEntity.Info.SORT);
            if (sort == null) {
                addInfo.setInt(ProductTagRelEntity.Info.SORT, ++maxSort);
            }

            Param tagInfo = new Param();
            Param relTagInfo = new Param();
            //装配标签表和标签业务表的数据
            assemblyTagInfo(flow, aid, unionPriId, tid, addInfo, tagInfo, relTagInfo);
            tagInfoList.add(tagInfo);
            relTagInfoList.add(relTagInfo);
        }

        //将事务设置为非自动提交
        if (transactionCtrl.isAutoCommit()) {
            transactionCtrl.setAutoCommit(false);
        }

        //保存tagId
        FaiList<Integer> tagIds = new FaiList<>();
        if (tagProc == null) {
            tagProc = new ProductTagProc(flow, aid, transactionCtrl);
        }
        //批量添加标签表的数据
        tagProc.addTagBatch(aid, tagInfoList, tagIds);

        for (int i = 0; i < tagIds.size(); i++) {
            Param relTagInfo = relTagInfoList.get(i);
            //设置tagId
            relTagInfo.setInt(ProductTagRelEntity.Info.TAG_ID, tagIds.get(i));
        }
        if (relTagProc == null) {
            relTagProc = new ProductTagRelProc(flow, aid, transactionCtrl);
        }
        //批量添加标签表的数据
        relTagProc.addTagRelBatch(aid, unionPriId, relTagInfoList, relTagIds);

        return maxSort;
    }

    /**
     * 添加单个标签
     */
    @SuccessRt(value = Errno.OK)
    public int addProductTag(FaiSession session, int flow, int aid, int unionPriId, int tid, Param info) throws IOException {
        int rt;
        if(Str.isEmpty(info)) {
            rt = Errno.ARGS_ERROR;
            Log.logErr("args error, info is empty;flow=%d;aid=%d;", flow, aid);
            return rt;
        }

        FaiList<Param> addInfoList = new FaiList<>();
        addInfoList.add(info);
        FaiList<Param> tagInfoList = new FaiList<>();
        FaiList<Param> relTagInfoList = new FaiList<>();
        FaiList<Integer> relTagIds = new FaiList<>();

        Lock lock = LockUtil.getLock(aid);
        lock.lock();
        try {
            boolean commit = false;
            TransactionCtrl transactionCtrl = new TransactionCtrl();
            ProductTagProc tagProc = new ProductTagProc(flow, aid, transactionCtrl);
            ProductTagRelProc tagRelProc = new ProductTagRelProc(flow, aid, transactionCtrl);
            int maxSort = 0;
            try {
                maxSort = addTagBatch(flow, aid, unionPriId, tid, transactionCtrl, tagProc, tagRelProc, addInfoList,
                        tagInfoList, relTagInfoList, relTagIds);
                commit = true;
            } finally {
                if(commit) {
                    transactionCtrl.commit();
                    // 新增缓存
                    ProductTagCache.addCache(aid, tagInfoList.get(0));
                    ProductTagRelCache.InfoCache.addCache(aid, unionPriId, relTagInfoList.get(0));
                    ProductTagRelCache.SortCache.set(aid, unionPriId, maxSort);
                    ProductTagRelCache.DataStatusCache.update(aid, unionPriId, 1);
                }else {
                    transactionCtrl.rollback();
                    tagProc.clearIdBuilderCache(aid);
                    tagRelProc.clearIdBuilderCache(aid, unionPriId);
                }
                transactionCtrl.closeDao();
            }
        }finally {
            lock.unlock();
        }
        rt = Errno.OK;
        FaiBuffer sendBuf = new FaiBuffer(true);
        Param result = relTagInfoList.get(0);

        sendBuf.putInt(ProductTagRelDto.Key.RL_TAG_ID, relTagIds.get(0));
        sendBuf.putInt(ProductTagRelDto.Key.TAG_ID, result.getInt(ProductTagRelEntity.Info.TAG_ID));
        session.write(sendBuf);
        Log.logStd("add ok;flow=%d;aid=%d;unionPriId=%d;tid=%d;rlTagId=%d;tagId=%d;", flow, aid, unionPriId,
                tid, relTagIds.get(0), result.getInt(ProductTagRelEntity.Info.TAG_ID));
        return rt;
    }


    public int delTagList(FaiSession session, int flow, int aid, int unionPriId, FaiList<Integer> rlTagIds) {
        return 0;
    }

    public int setTagList(FaiSession session, int flow, int aid, int unionPriId, FaiList<ParamUpdater> updaterList) {
        return 0;
    }

    public int getTagList(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) {
        return 0;
    }

    public int getAllTagRel(FaiSession session, int flow, int aid, int unionPriId) {
        return 0;
    }

    public int getTagRelFromDb(FaiSession session, int flow, int aid, int unionPriId, SearchArg searchArg) {
        return 0;
    }

    public int getTagRelDataStatus(FaiSession session, int flow, int aid, int unionPriId) {
        return 0;
    }

    public int unionSetTagList(FaiSession session, int flow, int aid, int unionPriId, int tid, FaiList<Param> addInfoList, FaiList<ParamUpdater> updaterList, FaiList<Integer> delRlTagIds) {
        return 0;
    }

    public int cloneData(FaiSession session, int flow, int aid, int fromAid, FaiList<Param> cloneUnionPriIds) {
        return 0;
    }

    public int incrementalClone(FaiSession session, int flow, int aid, int unionPriId, int fromAid, int fromUnionPriId) {
        return 0;
    }

    /**
     * 装配标签表和标签业务表的数据
     */
    private void assemblyTagInfo(int flow, int aid, int unionPriId, int tid, Param recvInfo, Param  tagInfo, Param relTagInfo) {

        String tagName = recvInfo.getString(ProductTagEntity.Info.TAG_NAME, "");
        if(!ProductTagCheck.isNameValid(tagName)) {
            throw new MgException(Errno.ARGS_ERROR, "tagName is not valid;flow=%d;aid=%d;tagName=%d;", flow, aid, tagName);
        }

        //标签类型如果没有获取到，这里没有设置默认值
        int tagType = recvInfo.getInt(ProductTagEntity.Info.TAG_TYPE);

        int flag = recvInfo.getInt(ProductTagEntity.Info.FLAG, ProductTagValObj.Default.FLAG);
        int sort = recvInfo.getInt(ProductTagRelEntity.Info.SORT, ProductTagRelValObj.Default.SORT);
        int rlFlag = recvInfo.getInt(ProductTagRelEntity.Info.RL_FLAG, ProductTagRelValObj.Default.RL_FLAG);
        Calendar now = Calendar.getInstance();
        Calendar createTime = recvInfo.getCalendar(ProductTagEntity.Info.CREATE_TIME, now);
        Calendar updateTime = recvInfo.getCalendar(ProductTagEntity.Info.UPDATE_TIME, now);

        // 标签表数据
        tagInfo.setInt(ProductTagEntity.Info.AID, aid);
        tagInfo.setInt(ProductTagEntity.Info.SOURCE_TID, tid);
        tagInfo.setInt(ProductTagEntity.Info.SOURCE_UNIONPRIID, unionPriId);
        tagInfo.setString(ProductTagEntity.Info.TAG_NAME, tagName);
        tagInfo.setInt(ProductTagEntity.Info.TAG_TYPE, tagType);
        tagInfo.setInt(ProductTagEntity.Info.FLAG, flag);
        tagInfo.setCalendar(ProductTagEntity.Info.CREATE_TIME, createTime);
        tagInfo.setCalendar(ProductTagEntity.Info.UPDATE_TIME, updateTime);

        // 标签业务表数据
        relTagInfo.setInt(ProductTagRelEntity.Info.AID, aid);
        relTagInfo.setInt(ProductTagRelEntity.Info.UNION_PRI_ID, unionPriId);
        relTagInfo.setInt(ProductTagRelEntity.Info.SORT, sort);
        relTagInfo.setInt(ProductTagRelEntity.Info.RL_FLAG, rlFlag);
        relTagInfo.setCalendar(ProductTagRelEntity.Info.CREATE_TIME, createTime);
        relTagInfo.setCalendar(ProductTagRelEntity.Info.UPDATE_TIME, updateTime);

    }
}
