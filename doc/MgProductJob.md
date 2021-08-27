## MgProductJob

### mgPdDelSaga

#### 1. 功能描述
删除商品中台中的分布式事务-saga模式 记录下来的数据

#### 2. 具体实现
商品中台保持一致的saga数据记录方式：  
- **每个库有一张saga操作表维护saga操作状态**  
- **每个库所有与分布式事务有关的数据表，都有与之对应的10张saga数据表**   

job每天10:05执行一次，默认删除6个月前的数据, 删除对象即为上述两种表  
具体代码见：fai.job.MgProductJob.handler.DelSagaHandler

#### 3. 配置说明
    "mgDelSaga":{
    	"sagaDBList":[
    		{
    			"dbName":"mgProductBasic",
    			"sagaTable":"mgPdBasicSaga",
    			"tablePrefixs":[
    				"mgProductBindGroupSaga",
    				"mgProductBindPropSaga",
    				"mgProductBindTagSaga",
    				"mgProductRelSaga",
    				"mgProductSaga"
    			]
    		}
    	],
    	"keepMonths":3
    }
配置中心MgProductJob -> mgDelSaga配置项：  
1）sagaDBList  
各个库saga相关的db信息  
- **dbName：** 库名。
- **sagaTable：** saga操作记录表名。
- **tablePrefixs：** saga数据表前缀集合。  

2）keepMonths  
数据保留的月数，选填，默认值为6(个月)

---
