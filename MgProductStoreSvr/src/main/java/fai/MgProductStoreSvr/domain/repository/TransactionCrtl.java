package fai.MgProductStoreSvr.domain.repository;

import fai.comm.util.Dao;
import fai.comm.util.DaoPool;
import fai.comm.util.Errno;
import fai.comm.util.Log;

import java.util.*;


public class TransactionCrtl {
    public boolean registered(DaoCtrl daoCtrl){
        if(daoCtrl == null){
            return false;
        }
        DaoPool daoPool = daoCtrl.getDaoPool();
        DaoCtrl firstDaoCtrl = firstDaoCtrlCache.get(daoPool);
        if(firstDaoCtrl == null){
            firstDaoCtrlCache.put(daoPool, daoCtrl);
            return daoCtrl.openDao() == Errno.OK && registeredDaoCtrlSet.add(daoCtrl);
        }
        Dao dao = firstDaoCtrl.getDao();
        return daoCtrl.openDao(dao) == Errno.OK && registeredDaoCtrlSet.add(daoCtrl);
    }
    public int setAutoCommit(boolean autoCommit){
        for (DaoCtrl daoCtrl : firstDaoCtrlCache.values()) {
            int rt = daoCtrl.setAutoCommit(autoCommit);
            if(rt != Errno.OK){
                Log.logErr(rt, "daoCtrl.setAutoCommit err;flow=%s;group=%s;tableName=%s;", daoCtrl.getFlow(), daoCtrl.getGroup(), daoCtrl.getTableName());
                return rt;
            }
        }
        return Errno.OK;
    }
    public void commit(){
        for (DaoCtrl daoCtrl : firstDaoCtrlCache.values()) {
            daoCtrl.commit();
        }
    }
    public void rollback(){
        for (DaoCtrl daoCtrl : firstDaoCtrlCache.values()) {
            daoCtrl.rollback();
        }
    }
    public void closeDao(){
        Iterator<Map.Entry<DaoPool, DaoCtrl>> iterator = firstDaoCtrlCache.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<DaoPool, DaoCtrl> daoPoolDaoCtrlEntry =  iterator.next();
            DaoCtrl daoCtrl = daoPoolDaoCtrlEntry.getValue();
            daoCtrl.closeDao();
            iterator.remove();
        }
    }

    /**
     * 检查 DaoCtrl 是否注册成功
     */
    public boolean checkRegistered(DaoCtrl ... daoCtrls){
        for (DaoCtrl daoCtrl : daoCtrls) {
            if(!registeredDaoCtrlSet.contains(daoCtrl)){
                return false;
            }
        }
        return true;
    }

    private Map<DaoPool, DaoCtrl> firstDaoCtrlCache = new HashMap<>();
    private Set<DaoCtrl> registeredDaoCtrlSet = new HashSet<>();
}
