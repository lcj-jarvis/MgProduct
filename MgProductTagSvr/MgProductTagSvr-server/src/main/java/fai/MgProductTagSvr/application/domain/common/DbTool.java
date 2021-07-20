package fai.MgProductTagSvr.application.domain.common;

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

        File file = new File("D:\\software\\DB\\mgProductTag.sql");
        FileWriter fileWriter = new FileWriter(file);
        System.out.println(fileWriter.getEncoding());
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < 1000 ; i++) {
            String tableName = "mgProductTag_" + String.format("%04d", i);
            sql.append("create table " + tableName + "(");
            sql.append("aid int(11) not null default '0' comment '企业aid',");
            sql.append("tagId int(11) not null default '0' comment '标签id',");
            sql.append("sourceTid int(11) not null default '0' comment '创建标签的项目id',");
            sql.append("sourceUnionPriId int(11) not null default '0' comment '创建标签的联合主建id',");
            sql.append("tagName varchar(100) not null default '' comment '标签名称',");
            sql.append("tagType tinyInt(3) not null default '0' comment '标签类型',");
            sql.append("flag int(11) not null default 0 comment '标志位',");
            sql.append("sysCreateTime dateTime not null default '1970-01-01 08:00:00' comment '创建时间',");
            sql.append("sysUpdateTime dateTime not null default '1970-01-01 08:00:00' comment '修改时间',");
            sql.append("primary key(aid,tagId)");
            sql.append(")engine InnoDB default charset utf8 comment '商品中台-商品标签表';\n");
        }

        for (int i = 0; i < 1000 ; i++) {
            String tableName = "mgProductTagRel_" + String.format("%04d", i);
            sql.append("create table " + tableName + "(");
            sql.append("aid int(11) not null default '0' comment '企业aid',");
            sql.append("rlTagId int(11) not null default '0' comment '标签业务id',");
            sql.append("tagId int(11) not null default '0' comment '标签id',");
            sql.append("unionPriId int(11) not null default '0' comment '联合主建',");
            sql.append("sort int(11) not null default 0 comment '同一个业务下一种标签类型的排序',");
            sql.append("rlFlag int(11) not null default 0 comment '业务标志位',");
            sql.append("sysCreateTime dateTime not null default '1970-01-01 08:00:00' comment '创建时间',");
            sql.append("sysUpdateTime dateTime not null default '1970-01-01 08:00:00' comment '修改时间',");
            sql.append("primary key(aid,tagId,unionPriId)");
            sql.append(")engine InnoDB default charset utf8 comment '商品中台-商品标签业务表';\n");
        }

        for (int i = 0; i < 1000 ; i++) {
            String tableName = "mgProductTagBak_" + String.format("%04d", i);
            sql.append("create table " + tableName + "(");
            sql.append("aid int(11) not null default '0' comment '企业aid',");
            sql.append("tagId int(11) not null default '0' comment '标签id',");
            sql.append("sourceTid int(11) not null default '0' comment '创建标签的项目id',");
            sql.append("sourceUnionPriId int(11) not null default '0' comment '创建标签的联合主建id',");
            sql.append("tagName varchar(100) not null default '' comment '标签名称',");
            sql.append("tagType tinyInt(3) not null default '0' comment '标签类型',");
            sql.append("flag int(11) not null default 0 comment '标志位',");
            sql.append("sysCreateTime dateTime not null default '1970-01-01 08:00:00' comment '创建时间',");
            sql.append("sysUpdateTime dateTime not null default '1970-01-01 08:00:00' comment '修改时间',");
            sql.append("backupId int(11) not null default '0' comment '备份id',");
            sql.append("backupIdFlag int(11) not null default '0' comment '备份id标记',");
            sql.append("primary key(aid,tagId,backupId)");
            sql.append(")engine InnoDB default charset utf8 comment '商品中台-商品标签备份表';\n");
        }

        for (int i = 0; i < 1000 ; i++) {
            String tableName = "mgProductTagRelBak_" + String.format("%04d", i);
            sql.append("create table " + tableName + "(");
            sql.append("aid int(11) not null default '0' comment '企业aid',");
            sql.append("rlTagId int(11) not null default '0' comment '标签业务id',");
            sql.append("tagId int(11) not null default '0' comment '标签id',");
            sql.append("unionPriId int(11) not null default '0' comment '联合主建',");
            sql.append("sort int(11) not null default 0 comment '同一个业务下一种标签类型的排序',");
            sql.append("rlFlag int(11) not null default 0 comment '业务标志位',");
            sql.append("sysCreateTime dateTime not null default '1970-01-01 08:00:00' comment '创建时间',");
            sql.append("sysUpdateTime dateTime not null default '1970-01-01 08:00:00' comment '修改时间',");
            sql.append("backupId int(11) not null default '0' comment '备份id',");
            sql.append("backupIdFlag int(11) not null default '0' comment '备份id标记',");
            sql.append("primary key(aid,tagId,unionPriId,backupId)");
            sql.append(")engine InnoDB default charset utf8 comment '商品中台-商品标签业务备份表';\n");
        }

        sql.append("create table mgProductTag_idBuilder(");
        sql.append("aid int(11) not null default '0' primary key comment '企业aid',");
        sql.append("tagId int(11) not null default '0' comment '标签id'");
        sql.append(")engine InnoDB default charset utf8 comment '商品中台-标签id自增表';\n");

        sql.append("create table mgProductTagRel_idBuilder(");
        sql.append("aid int(11) not null default '0' comment '企业aid',");
        sql.append("unionPriId int(11) not null default '0' comment '联合主建',");
        sql.append("rlTagId int(11) not null default '0' comment '标签业务id',");
        sql.append("primary key(aid, unionPriId)");
        sql.append(")engine InnoDB default charset utf8 comment '商品中台-标签业务id自增表';\n");
        System.out.println(sql.toString());
        fileWriter.write(sql.toString());
    }
}
