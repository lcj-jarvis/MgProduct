package fai.mgproduct.comm;

import fai.comm.util.FaiList;

import java.util.Collection;

public class Util {
    private Util(){
    }

    public static <T> boolean isEmptyList(Collection<T> list) {
        return list == null || list.isEmpty();
    }

    /**
     * 使用 delimiter 将各个元素连接起来
     * @param delimiter 分隔符
     * @param elements 集合
     */
    public static <T> String join(String delimiter, Collection<T> elements) {
        StringBuffer sb = new StringBuffer();
        Object[] array = elements.toArray();
        for(int i = 0;i < array.length; i++){
            if(i == (array.length - 1)){
                sb.append(array[i]);
            }else{
                sb.append(array[i]).append(delimiter);
            }
        }
        return new String(sb);
    }

    public static <T> FaiList<FaiList<T>> splitList(FaiList<T> list, int count) {
        if(list == null) {
            return null;
        }
        FaiList<FaiList<T>> resList = new FaiList<FaiList<T>>();
        if(list.isEmpty()) {
            return resList;
        }

        int size = list.size();

        if(size <= count) {
            resList.add(list);
            return resList;
        }

        int batchSize = size/count;
        for(int i = 0; i < batchSize; i++){
            FaiList<T> itemList = new FaiList<T>();
            for(int j = 0; j < count; j++){
                itemList.add(list.get( i*count+j ));
            }
            resList.add(itemList);
        }

        // 最后不足count的部分
        int lastSize = size%count;
        if(lastSize > 0){
            FaiList<T> itemList = new FaiList<T>();
            for(int i = 0; i < lastSize; i++){
                itemList.add(list.get( batchSize*count+i ));
            }
            resList.add(itemList);
        }

        return resList;
    }
}
