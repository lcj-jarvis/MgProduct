package fai.MgProductLibSvr.domain.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author LuChaoJi
 * @date 2021-07-05 9:25
 */
public class DbTool {

    public static void main(String[] args) throws IOException {
        createLibTable();
    }

    public static void createLibTable() throws IOException {

        File file = new File("D:\\software\\DB\\mgProductLib.sql");
        FileWriter fileWriter = new FileWriter(file);
        System.out.println(fileWriter.getEncoding());
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < 1000 ; i++) {
            String tableName = "mgProductLib_" + String.format("%04d", i);
            sql.append("create table " + tableName + "(");
            sql.append("aid int(11) not null default '0' comment '企业aid',");
            sql.append("libId int(11) not null default '0' comment '库id',");
            sql.append("sourceTid int(11) not null default '0' comment '创建库的项目id',");
            sql.append("sourceUnionPriId int(11) not null default '0' comment '创建库的联合主建',");
            sql.append("libName varchar(100) not null default '' comment '库名称',");
            sql.append("libType tinyInt(3) not null default '0' comment '库类型',");
            sql.append("flag int(11) not null default 0 comment '标志位',");
            sql.append("sysCreateTime dateTime not null default '1970-01-01 08:00:00' comment '创建时间',");
            sql.append("sysUpdateTime dateTime not null default '1970-01-01 08:00:00' comment '修改时间',");
            sql.append("primary key(aid,libId)");
            sql.append(")engine InnoDB default charset utf8 comment '商品中台-商品库表';\n");
        }

        for (int i = 0; i < 1000 ; i++) {
            String tableName = "mgProductLibRel_" + String.format("%04d", i);
            sql.append("create table " + tableName + "(");
            sql.append("aid int(11) not null default '0' comment '企业aid',");
            sql.append("rlLibId int(11) not null default '0' comment '库业务id',");
            sql.append("libId int(11) not null default '0' comment '商品库id',");
            sql.append("unionPriId int(11) not null default '0' comment '联合主建',");
            sql.append("libType tinyInt(3) not null default '0' comment '库类型',");
            sql.append("sort int(11) not null default 0 comment '同一个业务下一种库类型的排序',");
            sql.append("rlFlag int(11) not null default 0 comment '业务标志位',");
            sql.append("sysCreateTime dateTime not null default '1970-01-01 08:00:00' comment '创建时间',");
            sql.append("sysUpdateTime dateTime not null default '1970-01-01 08:00:00' comment '修改时间',");
            sql.append("primary key(aid,libId,unionPriId)");
            sql.append(")engine InnoDB default charset utf8 comment '商品中台-商品库业务表';\n");
        }

        sql.append("create table mgProductLib_idBuilder(");
        sql.append("aid int(11) not null default '0' primary key comment '企业aid',");
        sql.append("id int(11) not null default '0' comment '库id'");
        sql.append(")engine InnoDB default charset utf8 comment '商品中台-库id自增表';\n");

        sql.append("create table mgProductLibRel_idBuilder(");
        sql.append("aid int(11) not null default '0' comment '企业aid',");
        sql.append("unionPriId int(11) not null default '0' comment '联合主建',");
        sql.append("id int(11) not null default '0' comment '库业务id',");
        sql.append("primary key(aid, unionPriId)");
        sql.append(")engine InnoDB default charset utf8 comment '商品中台-库业务id自增表';\n");
        System.out.println(sql.toString());
        fileWriter.write(sql.toString());
    }
}
