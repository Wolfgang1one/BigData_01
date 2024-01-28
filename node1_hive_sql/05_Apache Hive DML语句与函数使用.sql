show databases;

use itheima;

------------Hive SQL-DML-Load加载数据---------------

--step1:建表
--建表student_local 用于演示从本地加载数据
create table student_local(num int,name string,sex string,age int,dept string) row format delimited fields terminated by ',';
--建表student_HDFS  用于演示从HDFS加载数据
create table student_HDFS(num int,name string,sex string,age int,dept string) row format delimited fields terminated by ',';


--建议使用beeline客户端 可以显示出加载过程日志信息
--step2:加载数据
-- 从本地加载数据  数据位于HS2（node1）本地文件系统  本质是hadoop fs -put上传操作
LOAD DATA LOCAL INPATH '/root/hivedata/students.txt' INTO TABLE student_local;
--从HDFS加载数据  数据位于HDFS文件系统根目录下  本质是hadoop fs -mv 移动操作
--先把数据上传到HDFS上  hadoop fs -put /root/hivedata/students.txt /
LOAD DATA INPATH '/students.txt' INTO TABLE student_HDFS;


------------Hive SQL-DML-Insert插入数据-----------------

--step1:创建一张源表student
drop table if exists student;
create table student(num int,name string,sex string,age int,dept string)
row format delimited fields terminated by ',';

--step2:加载数据
load data local inpath '/root/hivedata/students.txt' into table student;

select * from student;

--step3：创建一张目标表  只有两个字段
create table student_from_insert(sno int,sname string);

--使用insert+select插入数据到新表中
insert into table student_from_insert select num,name from student;

select * from student_from_insert;


------------Hive SQL select查询基础语法------------
--创建表t_usa_covid19
drop table if exists t_usa_covid19;
CREATE TABLE t_usa_covid19(
    count_date string,
    county string,
    state string,
    fips int,
    cases int,
    deaths int)
row format delimited fields terminated by ",";

--将数据load加载到t_usa_covid19表对应的路径下
load data local inpath '/root/hivedata/us-covid19-counties.dat' into table t_usa_covid19;


--1、select_expr
--查询所有字段或者指定字段
select *  from t_usa_covid19;

select county, cases, deaths from t_usa_covid19;
--查询常数返回 此时返回的结果和表中字段无关
select 1 from t_usa_covid19;
--查询当前数据库
select current_database(); --省去from关键字


--2、ALL DISTINCT
--返回所有匹配的行
select state from t_usa_covid19;
--相当于
select all state from t_usa_covid19;

--返回所有匹配的行 去除重复的结果
select distinct state from t_usa_covid19;
--多个字段distinct 整体去重
select distinct county,state from t_usa_covid19;

--county,state
   a      BBB
   c      BBB
   a      BBB
   d      CCC
   a      WWW

   a      BBB
   c      BBB
   d      CCC


--3、WHERE CAUSE
select * from t_usa_covid19 where 1 > 2;  -- 1 > 2 返回false
select * from t_usa_covid19 where 1 = 1;  -- 1 = 1 返回true

--找出来自于California州的疫情数据
select * from t_usa_covid19 where state = 'California';
--where条件中使用函数 找出州名字母长度超过10位的有哪些
select * from t_usa_covid19 where length(state) >10 ;

--注意：where条件中不能使用聚合函数
-- --报错 SemanticException:Not yet supported place for UDAF 'count'
--聚合函数要使用它的前提是结果集已经确定。
--而where子句还处于“确定”结果集的过程中，因而不能使用聚合函数。
select state,sum(deaths) from t_usa_covid19 where sum(deaths) >100 group by state;
--可以使用Having实现
select state,sum(deaths) from t_usa_covid19  group by state having sum(deaths) > 100;


--4、聚合操作
--统计美国总共有多少个县county
select county as itcast from t_usa_covid19;
--学会使用as 给查询返回的结果起个别名
select count(county) as county_cnts from t_usa_covid19;
--去重distinct
select count(distinct county) as county_cnts from t_usa_covid19;

--统计美国加州有多少个县
select count(county) from t_usa_covid19 where state = "California";
--统计德州总死亡病例数
select sum(deaths) from t_usa_covid19 where state = "Texas";
--统计出美国最高确诊病例数是哪个县
select max(cases) from t_usa_covid19;



--5、GROUP BY

select *
from t_usa_covid19;

--根据state州进行分组 统计每个州有多少个县county
select count(county) from t_usa_covid19 where count_date = "2021-01-28" group by state;

--想看一下统计的结果是属于哪一个州的
select state,count(county) as county_nums from t_usa_covid19 where count_date = "2021-01-28" group by state;

--再想看一下每个县的死亡病例数，我们猜想很简单呀  把deaths字段加上返回  真实情况如何呢？
select state,count(county),sum(deaths) from t_usa_covid19 where count_date = "2021-01-28" group by state;
--很尴尬 sql报错了org.apache.hadoop.hive.ql.parse.SemanticException:Line 1:27 Expression not in GROUP BY key 'deaths'

--为什么会报错？？group by的语法限制
--结论：出现在GROUP BY中select_expr的字段：要么是GROUP BY分组的字段；要么是被聚合函数应用的字段。
--deaths不是分组字段 报错
--state是分组字段 可以直接出现在select_expr中

--被聚合函数应用
select state,count(county),sum(deaths) from t_usa_covid19 where count_date = "2021-01-28" group by state;


--6、having
--统计2021-01-28死亡病例数大于10000的州
select state,sum(deaths) from t_usa_covid19 where count_date = "2021-01-28" and sum(deaths) >10000 group by state;
--where语句中不能使用聚合函数 语法报错

--先where分组前过滤，再进行group by分组， 分组后每个分组结果集确定 再使用having过滤
select state,sum(deaths) from t_usa_covid19 where count_date = "2021-01-28" group by state having sum(deaths) > 10000;
--这样写更好 即在group by的时候聚合函数已经作用得出结果 having直接引用结果过滤 不需要再单独计算一次了
select state,sum(deaths) as cnts from t_usa_covid19 where count_date = "2021-01-28" group by state having cnts> 10000;


--7、order by
--根据确诊病例数升序排序 查询返回结果
select * from t_usa_covid19 ;
select * from t_usa_covid19 order by cases;
--不写排序规则 默认就是asc升序
select * from t_usa_covid19 order by cases asc;

--根据死亡病例数倒序排序 查询返回加州每个县的结果
select * from t_usa_covid19 where state = "California" order by cases desc;

--8、limit
--没有限制返回2021.1.28 加州的所有记录
select * from t_usa_covid19 where count_date = "2021-01-28" and state ="California";

--返回结果集的前5条
select * from t_usa_covid19 where count_date = "2021-01-28" and state ="California" limit 5;

--返回结果集从第1行开始 共3行
select * from t_usa_covid19 where count_date = "2021-01-28" and state ="California" limit 2,3;
--注意 第一个参数偏移量是从0开始的


--执行顺序
select state,sum(deaths) as cnts from t_usa_covid19
where count_date = "2021-01-28"
group by state
having cnts> 10000
limit 2;


------------Hive Join SQL 语法------------
--Join语法练习 建表
drop table if exists employee_address;
drop table if exists employee_connection;
drop table if exists employee;

--table1: 员工表
CREATE TABLE employee(
   id int,
   name string,
   deg string,
   salary int,
   dept string
 ) row format delimited
fields terminated by ',';

--table2:员工家庭住址信息表
CREATE TABLE employee_address (
    id int,
    hno string,
    street string,
    city string
) row format delimited
fields terminated by ',';

--table3:员工联系方式信息表
CREATE TABLE employee_connection (
    id int,
    phno string,
    email string
) row format delimited
fields terminated by ',';

--加载数据到表中
load data local inpath '/root/hivedata/employee.txt' into table employee;
load data local inpath '/root/hivedata/employee_address.txt' into table employee_address;
load data local inpath '/root/hivedata/employee_connection.txt' into table employee_connection;

select *
from employee;

select *
from employee_address;

select *
from employee_connection;


--1、inner join
select e.id,e.name,e_a.city,e_a.street
from employee e inner join employee_address e_a
on e.id =e_a.id;

--等价于 inner join=join
select e.id,e.name,e_a.city,e_a.street
from employee e join employee_address e_a
on e.id =e_a.id;


--等价于 隐式连接表示法
select e.id,e.name,e_a.city,e_a.street
from employee e , employee_address e_a
where e.id =e_a.id;



--2、left join
select e.id,e.name,e_conn.phno,e_conn.email
from employee e left join employee_connection e_conn
on e.id =e_conn.id;

--等价于 left outer join
select e.id,e.name,e_conn.phno,e_conn.email
from employee e left outer join  employee_connection e_conn
on e.id =e_conn.id;



-----------------Hive 常用的内置函数----------------------
show functions;
describe function extended count;


------------String Functions 字符串函数------------
select length("itcast");
select reverse("itcast");

select concat("angela","baby");
--带分隔符字符串连接函数：concat_ws(separator, [string | array(string)]+)
select concat_ws('.', 'www', array('itcast', 'cn'));

--字符串截取函数：substr(str, pos[, len]) 或者  substring(str, pos[, len])
select substr("angelababy",-2); --pos是从1开始的索引，如果为负数则倒着数
select substr("angelababy",2,2);
--分割字符串函数: split(str, regex)
--split针对字符串数据进行切割  返回是数组array  可以通过数组的下标取内部的元素 注意下标从0开始的
select split('apache hive', ' ');
select split('apache hive', ' ')[0];
select split('apache hive', ' ')[1];


----------- Date Functions 日期函数 -----------------
--获取当前日期: current_date
select current_date();
--获取当前UNIX时间戳函数: unix_timestamp
select unix_timestamp();
--日期转UNIX时间戳函数: unix_timestamp
select unix_timestamp("2011-12-07 13:01:03");
--指定格式日期转UNIX时间戳函数: unix_timestamp
select unix_timestamp('20111207 13:01:03','yyyyMMdd HH:mm:ss');
--UNIX时间戳转日期函数: from_unixtime
select from_unixtime(1618238391);
select from_unixtime(0, 'yyyy-MM-dd HH:mm:ss');

--日期比较函数: datediff  日期格式要求'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'
select datediff('2012-12-08','2012-05-09');
--日期增加函数: date_add
select date_add('2012-02-28',10);
--日期减少函数: date_sub
select date_sub('2012-01-1',10);


----Mathematical Functions 数学函数-------------
--取整函数: round  返回double类型的整数值部分 （遵循四舍五入）
select round(3.1415926);
--指定精度取整函数: round(double a, int d) 返回指定精度d的double类型
select round(3.1415926,4);
--取随机数函数: rand 每次执行都不一样 返回一个0到1范围内的随机数
select rand();
--指定种子取随机数函数: rand(int seed) 得到一个稳定的随机数序列
select rand(3);


-----Conditional Functions 条件函数------------------
--使用之前课程创建好的student表数据
select * from student limit 3;

--if条件判断: if(boolean testCondition, T valueTrue, T valueFalseOrNull)
select if(1=2,100,200);
select if(sex ='男','M','W') from student limit 3;

--空值转换函数: nvl(T value, T default_value)
select nvl("allen","itcast");
select nvl(null,"itcast");

--条件转换函数: CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END
select case 100 when 50 then 'tom' when 100 then 'mary' else 'tim' end;
select case sex when '男' then 'male' else 'female' end from student limit 3;