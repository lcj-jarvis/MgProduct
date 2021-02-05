package fai.MgProductSpecSvr.domain.comm;

import fai.comm.util.*;

import java.util.*;
import java.util.function.Consumer;

public class Utils {


    public static <K> Map<K, Param> getMap(FaiList<Param> list, String key){
        if(list == null){
            return null;
        }
        Map<K, Param> result = new HashMap(list.size() * 4 / 3 +1); // 直接计算所需最大容量，避免resize
        list.forEach(e -> {
            result.put((K) e.getObject(key), e);
        });
        return result;
    }

    public static Set<String> getMaxUpdaterKeys(FaiList<ParamUpdater> updaterList){
        return getMaxUpdaterKeys(updaterList, null);
    }
    public static Set<String> getMaxUpdaterKeys(FaiList<ParamUpdater> updaterList, Consumer<Param> consumer){
        if(updaterList == null){
            return null;
        }
        Set<String> maxUpdaterKeys = new HashSet<>();
        updaterList.forEach(updater -> {
            Param data = updater.getData();
            for (int i = 0; i < data.size(); i++) {
                Pair<String, Object> dataPair = data.get(i);
                maxUpdaterKeys.add(dataPair.first);
            }
            FaiList<ParamUpdater.DataOp> opList = updater.getOpList();
            for (ParamUpdater.DataOp dataOp : opList) {
                maxUpdaterKeys.add(dataOp.key);
            }
            if(consumer != null){
                consumer.accept(data);
            }
        });
        return maxUpdaterKeys;
    }

    public static Set<String> validUpdaterList(FaiList<ParamUpdater> updaterList, String[] validKeys, Consumer<Param> consumer){
        if(updaterList == null){
            return null;
        }
        HashSet<String> validKeySet = null;
        if(validKeys != null){
            validKeySet = new HashSet<>(Arrays.asList(validKeys));
        }
        Set<String> maxUpdaterKeys = new HashSet<>();
        Iterator<ParamUpdater> iterator = updaterList.iterator();
        while (iterator.hasNext()){
            ParamUpdater updater = iterator.next();
            Param data = updater.getData();
            for (int i = data.size()-1; i >= 0; i--) {
                Pair<String, Object> dataPair = data.get(i);
                String key = dataPair.first;
                if(validKeySet != null && !validKeySet.contains(key)){
                    data.remove(key);
                }else {
                    maxUpdaterKeys.add(key);
                }
            }
            FaiList<ParamUpdater.DataOp> opList = updater.getOpList();
            for (int i = opList.size() - 1; i >= 0; i--) {
                ParamUpdater.DataOp dataOp = opList.get(i);
                if(validKeySet != null && !validKeySet.contains(dataOp.key)){
                    opList.remove(i);
                }else{
                    maxUpdaterKeys.add(dataOp.key);
                }
            }
            if(consumer != null){
                consumer.accept(data);
            }
        }
        return maxUpdaterKeys;
    }

    public static <T> FaiList<T> getValList(FaiList<Param> list, String key) {
        if(list == null){
            return null;
        }
        FaiList<T> resultList = new FaiList<>(list.size());
        list.forEach(info->{
            resultList.add((T)info.getObject(key));
        });
        return resultList;
    }
}
