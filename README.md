# bitmapudf
hive udf 读写存储到hbase的roaringbitmap 咆哮位图

在cc.youshu.redis包里面是一个redis相关的函数，比较简单，这里我就不在多说了，下面重点说一下roaringbitmap相关的udf

**本项目主要是将hive udf 、roaringbitmap 、hbase集成到一起**
打包方法
`mvn clean package`
然后将target下jar包上传到hdfs指定目录，以方便创建udf函数，或者您也可以直接上传到服务器，通过add jar命令创建udf函数。
本人上传到了`/utils/hiveUdf/`,pom文件里面的额额依赖包，放到hive的第三方依赖包目录里面
### bit_map_byte函数
 1. 作用：将数字去重的集合存储到roaringbitmap里面，输出二进制
 2. 创建UDF
```sql
create function bit_map_byte as 'cc.youshu.roaringbitmap.RoaringBitMapByteUDAF' 
using jar 'hdfs://nameservice1/utils/hiveUdf/hiveudf.jar';
```
 3. 使用例子
```sql
SELECT plan_code,bit_map_byte(id) FROM dw.rw_plan GROUP BY plan_code;
```
### hbase_put函数
 1. 将二进制或者字符串等导入hbase中
 2. 创建UDF

```sql
create function hbase_put as 'cc.youshu.roaringbitmap.HbasePut_UDF' 
using jar 'hdfs://nameservice1/utils/hiveUdf/hiveudf.jar';
```
 3. 使用
```sql
SELECT hbase_put( map('hbase.zookeeper.quorum',
'XXX,XXX,XXX',
'table_name',
'test_roaring_syf',
'family',
'group',
'qualifier',
'q'), concat('group_plan_',plan_code),
val)
FROM(
SELECT plan_code,bit_map_byte(id) FROM dw.rw_plan GROUP BY plan_code) a;
```
### hbase_put_add函数

 1. 作用：首先判断此字段有没有bitmap的二进制存在，如果有，则进行or然后，插入，如果没有直接插入
 2. 创建
 

```sql
create function hbase_put_add as 'cc.youshu.roaringbitmap.HbasePutAdd_UDF' 
using jar 'hdfs://nameservice1/utils/hiveUdf/hiveudf.jar';
```

 3. 使用
 

```sql
SELECT hbase_put_add( map('hbase.zookeeper.quorum',
'XXX,XXX,XXX',
'table_name',
'test_roaring_syf',
'family',
'group',
'qualifier',
'q'), concat('group_plan_',plan_code),
val)
FROM(
SELECT plan_code,bit_map_byte(id) FROM dw.rw_plan GROUP BY plan_code) a;
```
### bitmap_to_id函数（udtf）

 1. 作用：读取bitmap的二进制，返回里面所有的id，分成不同的行
 2. 创建
 

```sql
create function bitmap_to_id as 'cc.youshu.udtf.BitMapUDTF' using jar 'hdfs://nameservice1/utils/hiveUdf/hiveudf.jar';
```

 3. 使用
 

```sql
SELECT key, BitMapCount(user_id) from hbase_table_1 LIMIT 10;
```
**说明：hbase_table_1是我创建的一个hive on hbase的表，user_id列是存储的roaringbitmap的二进制**
hbase_table_1表创建语句

```sql
CREATE EXTERNAL TABLE hbase_table_1(key String, user_id  BINARY) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,group:user_id"
)TBLPROPERTIES("hbase.table.name" = "user_roaring_bit_map_test");
```


