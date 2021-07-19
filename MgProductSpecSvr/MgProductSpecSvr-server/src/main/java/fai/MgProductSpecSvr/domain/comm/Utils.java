package fai.MgProductSpecSvr.domain.comm;

import fai.comm.util.FaiList;
import fai.comm.util.Pair;
import fai.comm.util.Param;
import fai.comm.util.ParamUpdater;

import java.util.*;
import java.util.function.Consumer;

public class Utils {

    /**
     * 从 list 中获取指定的key的值 和 info 组成的map  <br/>
     * 适用场景：list中各个元素key对应的value(即map的key值) 基本是唯一的  <br/>
     * 不然推荐使用{@link fai.comm.util.OptMisc#getMap(FaiList, String)}  <br/>
     * @param list
     * @param key
     * @param <K>
     */
    public static <K> Map<K, Param> getMap(FaiList<Param> list, String key){
        return getMap(list, key, null);
    }

    /**
     * 从list 中获取指定的keyKey 和 valueKey 组成的map  <br/>
     * 适用场景：list中各个元素key对应的value(即map的key值) 基本是唯一的  <br/>
     * 不然推荐使用{@link fai.comm.util.OptMisc#getMap(FaiList, String, String)}  <br/>
     * @param list
     * @param keyKey
     * @param valueKey
     * @param <K>
     * @param <V>
     */
    public static <K, V> Map<K, V> getMap(FaiList<Param> list, String keyKey, String valueKey){
        if(list == null){
            return null;
        }
        Map<K, V> resultMap = new HashMap<>(list.size()*4/3+1); // 直接计算所需最大容量，避免resize
        list.stream().forEach( info -> {
            Object keyObj = info.getObject(keyKey);
            if(valueKey == null){
                resultMap.put((K)keyObj, (V)info);
            }else{
                Object valObj = info.getObject(valueKey);
                resultMap.put((K)keyObj, (V)valObj);
            }
        });
        return resultMap;
    }


    /**
     * 保留 updaterList 中有效的 validKeys ，附加 consumer 进行额外处理
     * @param updaterList
     * @param validKeys
     * @param consumer
     */
    public static Set<String> retainValidUpdaterList(FaiList<ParamUpdater> updaterList, String[] validKeys, Consumer<Param> consumer){
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
                // 移除无效的 keys ，添加有效的 keys
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

    /**
     * 获取某个key的值集  <br/>
     * @param list
     * @param key
     * @param <T>
     */
    public static <T> FaiList<T> getValList(FaiList<Param> list, String key) {
        if(list == null){
            return null;
        }
        FaiList<T> resultList = new FaiList<>(list.size()); // 直接初始化所需最大容量
        list.forEach(info->{
            resultList.add((T)info.getObject(key));
        });
        return resultList;
    }
}
