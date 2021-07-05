package fai.MgProductLibSvr.domain.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author LuChaoJi
 * @date 2021-07-05 9:30
 */
public class DeleteDbTool {

    public static void main(String[] args) throws IOException {
        deleteDb();
    }

    private static void deleteDb() throws IOException {

        File file = new File("D:\\software\\DB\\deleteMgProductLib.sql");
        FileWriter fileWriter = new FileWriter(file);
        System.out.println(fileWriter.getEncoding());
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < 1000 ; i++) {
            String tableName1 = "mgProductLib_" + String.format("%04d", i);
            String tableName2 = "mgProductLibRel_" + String.format("%04d", i);
            sql.append("drop table if exists " + tableName1 + ";\n");
            sql.append("drop table if exists " + tableName2 + ";\n");
        }
        sql.append("drop table if exists mgProductLib_idBuilder;\n");
        sql.append("drop table if exists mgProductLibRel_idBuilder;");

        System.out.println(sql.toString());
        fileWriter.write(sql.toString());

    }


}
