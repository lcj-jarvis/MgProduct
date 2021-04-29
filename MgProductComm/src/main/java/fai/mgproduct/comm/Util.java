package fai.mgproduct.comm;

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
}
