package fai.MgProductBasicSvr.domain.common;

import fai.MgProductBasicSvr.domain.entity.ProductRelEntity;
import fai.app.DocOplogDef;
import fai.app.FaiSearchExDef;
import fai.cli.DocOplogCli;
import fai.comm.util.Errno;
import fai.comm.util.FaiList;
import fai.comm.util.Log;
import fai.comm.util.Param;
import fai.middleground.svrutil.exception.MgException;
import fai.middleground.svrutil.misc.Utils;

import java.util.HashSet;


/**
 *
 * 这里是同步数据给es的工具
 * 有两种方式：
 * 1. 直接调用cli发包给es那边
 * 2. 先将要同步的数据放到ThreadLocal中，之后再统一(一般是commit之后)发包
 * 第二种方式是因为事务是在service中控制的，所以触发同步数据的逻辑也要在service，
 * 但是有些接口不能直接拿到数据的主键，需要调用proc接口时返回主键数据或另外再查一次，
 * 很不方便，所以采用这种预同步的逻辑
 *
 */
public class ESUtil {
    private static final int BATCH_SIZE = 2000;
    private static ThreadLocal<HashSet<Param>> preLogList = new ThreadLocal<>();

    public static void clear() {
        preLogList.remove();
    }

    public static HashSet<Param> getPreList() {
        HashSet<Param> list = preLogList.get();
        if(list == null) {
            list = new HashSet<>();
            preLogList.set(list);
        }
        return list;
    }

    /**
     * 记录要同步的数据
     */
    public static void preLog(int aid, int pdId, int unionPriId, int op) {
        HashSet<Param> preList = getPreList();
        DocOplogDef.Docid docid = DocOplogDef.Docid.create(3);
        docid.add(ProductRelEntity.Info.AID, aid);
        docid.add(ProductRelEntity.Info.PD_ID, pdId);
        docid.add(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
        Param logInfo = new Param();
        logInfo.setInt(DocOplogDef.Info.OPERATION, op);
        logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidKeys());
        logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidVals());
        preList.add(logInfo);
    }

    /**
     * 批量记录要同步的数据
     */
    public static void batchPreLog(int aid, FaiList<Param> list, int op) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        HashSet<Param> preList = getPreList();
        for(Param info : list) {
            int pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            int unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            DocOplogDef.Docid docid = DocOplogDef.Docid.create(3);
            docid.add(ProductRelEntity.Info.AID, aid);
            docid.add(ProductRelEntity.Info.PD_ID, pdId);
            docid.add(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
            Param logInfo = new Param();
            logInfo.setInt(DocOplogDef.Info.OPERATION, op);
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidKeys());
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidVals());
            preList.add(logInfo);
        }
    }

    /**
     * 批量记录要同步的数据
     */
    public static void batchPreLog(int aid, int unionPriId, FaiList<Integer> pdIds, int op) {
        if(Utils.isEmptyList(pdIds)) {
            return;
        }
        HashSet<Param> preList = getPreList();
        for(Integer pdId : pdIds) {
            DocOplogDef.Docid docid = DocOplogDef.Docid.create(3);
            docid.add(ProductRelEntity.Info.AID, aid);
            docid.add(ProductRelEntity.Info.PD_ID, pdId);
            docid.add(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
            Param logInfo = new Param();
            logInfo.setInt(DocOplogDef.Info.OPERATION, op);
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidKeys());
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidVals());
            preList.add(logInfo);
        }
    }

    /**
     * 同步 preLogList 中的数据
     */
    public static void commitPre(int flow, int aid) {
        if(preLogList.get() == null) {
            return;
        }
        int rt;
        DocOplogCli cli = createDocOplogCli(flow);

        FaiList<FaiList<Param>> list = Utils.splitList(new FaiList<>(preLogList.get()), BATCH_SIZE);
        for(FaiList<Param> batchList : list) {
            rt = cli.batchAddDocOplog(aid, FaiSearchExDef.App.MG_PRODUCT, batchList);
            if(rt != Errno.OK) {
                Log.logErr("addDocid err;aid=%s;logList=%s;", aid, batchList);
            }else {
                Log.logStd("batch addDocid ok;aid=%s;logList=%s;", aid, batchList);
            }
        }

        preLogList.remove();
    }

    /**
     * 同步数据给es
     * 主键顺序必须为 aid pdId unionPriId
     * @param aid
     * @param pdId
     * @param unionPriId
     */
    public static void logDocId(int flow, int aid, int pdId, int unionPriId, int op) {
        DocOplogDef.Docid docid = DocOplogDef.Docid.create(3);
        docid.add(ProductRelEntity.Info.AID, aid);
        docid.add(ProductRelEntity.Info.PD_ID, pdId);
        docid.add(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
        DocOplogCli opCli = createDocOplogCli(flow);
        int rt = opCli.addDocOplog(aid, FaiSearchExDef.App.MG_PRODUCT, op, docid);
        if (rt != Errno.OK) {
            Log.logErr("addDocid err;aid=%s;pdId=%s;unionPriId=%s;op=%s;flow=%s;", aid, pdId, unionPriId, op, flow);
            return;
        }
        Log.logStd("addDocOplog successs;aid=%s;pdId=%s;unionPriId=%s;op=%s;flow=%s;", aid, pdId, unionPriId, op, flow);
    }

    /**
     * 批量同步数据给es
     * 主键顺序必须为 aid pdId unionPriId
     * 批量的每批次长度不能大于2000
     * @param flow
     * @param aid
     * @param list
     * @param op
     */
    public static void batchLogDocId(int flow, int aid, FaiList<Param> list, int op) {
        if(Utils.isEmptyList(list)) {
            return;
        }
        int rt;
        DocOplogCli opCli = createDocOplogCli(flow);
        FaiList<Param> logList = new FaiList<>();
        for(Param info : list) {
            int pdId = info.getInt(ProductRelEntity.Info.PD_ID);
            int unionPriId = info.getInt(ProductRelEntity.Info.UNION_PRI_ID);
            DocOplogDef.Docid docid = DocOplogDef.Docid.create(3);
            docid.add(ProductRelEntity.Info.AID, aid);
            docid.add(ProductRelEntity.Info.PD_ID, pdId);
            docid.add(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
            Param logInfo = new Param();
            logInfo.setInt(DocOplogDef.Info.OPERATION, op);
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidKeys());
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidVals());
            logList.add(logInfo);
            if(logList.size() >= BATCH_SIZE) {
                rt = opCli.batchAddDocOplog(aid, FaiSearchExDef.App.MG_PRODUCT, logList);
                if(rt != Errno.OK) {
                    Log.logErr("addDocid err;aid=%s;op=%s;logList=%s;", aid, op, logList);
                }else {
                    Log.logStd("batch addDocid ok;aid=%s;op=%s;logList=%s;", aid, op, logList);
                }
                logList.clear();
            }
        }
        if(!logList.isEmpty()) {
            rt = opCli.batchAddDocOplog(aid, FaiSearchExDef.App.MG_PRODUCT, logList);
            if(rt != Errno.OK) {
                Log.logErr("addDocid err;aid=%s;op=%s;logList=%s;", aid, op, logList);
            }else {
                Log.logStd("batch addDocid ok;aid=%s;op=%s;logList=%s;", aid, op, logList);
            }
        }
    }
    public static void batchLogDocId(int flow, int aid, int unionPriId, FaiList<Integer> pdIds, int op) {
        if(Utils.isEmptyList(pdIds)) {
            return;
        }
        int rt;
        DocOplogCli opCli = createDocOplogCli(flow);
        FaiList<Param> logList = new FaiList<>();
        for(Integer pdId : pdIds) {
            DocOplogDef.Docid docid = DocOplogDef.Docid.create(3);
            docid.add(ProductRelEntity.Info.AID, aid);
            docid.add(ProductRelEntity.Info.PD_ID, pdId);
            docid.add(ProductRelEntity.Info.UNION_PRI_ID, unionPriId);
            Param logInfo = new Param();
            logInfo.setInt(DocOplogDef.Info.OPERATION, op);
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidKeys());
            logInfo.setString(DocOplogDef.Info.DOCID_KEYS, docid.getDocidVals());
            logList.add(logInfo);
            if(logList.size() >= BATCH_SIZE) {
                rt = opCli.batchAddDocOplog(aid, FaiSearchExDef.App.MG_PRODUCT, logList);
                if(rt != Errno.OK) {
                    Log.logErr("addDocid err;aid=%s;op=%s;logList=%s;", aid, op, logList);
                }else {
                    Log.logStd("batch addDocid ok;aid=%s;op=%s;logList=%s;", aid, op, logList);
                }
                logList.clear();
            }
        }
        if(!logList.isEmpty()) {
            rt = opCli.batchAddDocOplog(aid, FaiSearchExDef.App.MG_PRODUCT, logList);
            if(rt != Errno.OK) {
                Log.logErr("addDocid err;aid=%s;op=%s;logList=%s;", aid, op, logList);
            }else {
                Log.logStd("batch addDocid ok;aid=%s;op=%s;logList=%s;", aid, op, logList);
            }
        }
    }

    private static DocOplogCli createDocOplogCli(int flow) {
        DocOplogCli opCli = new DocOplogCli(flow);
        if (!opCli.init()) {
            throw new MgException("init DocOplogCli error");
        }
        return opCli;
    }
}
