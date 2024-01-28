create database BigDataCourse;
use BigDataCourse;

create table if not exists BigDataCourse.initial_table (
    id string comment "报名专业记录id",
    sid string comment "学生用户id",
    school string comment "学校名称",
    city_code string comment "学生城市代码",
    city string comment "学生城市名称",
    subject string comment "专业名称",
    sub_type string comment "专业分类",
    major_code string comment "专业代码",
    stu_time string comment "学生时间",
    batch string comment "学生批次",
    state string comment "状态",
    type string comment "培养类型",
    year string comment "招生时间",
    level string comment "学历等级",
    profess string comment "专业名称",
    plan string comment "计划招生人数",
    uniname string comment "大学名称",
    unicode string comment "大学代码"
)row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/bigDataCourse/data.txt' into table BigDataCourse.0_initial_table;

-- 1、数据清洗完成 *
-- 2、学生来源分析 *
-- 3、全国招生最多的专业Top10 *
-- 4、招生最少的专业Top10 *
-- 5、全国招生计划按省份分布 *
-- 6、招生最多学校Top10
-- 7、全国文科招生数 *
-- 8、全国理科招生数 *
-- 9、全国985学校招生数及占比
-- 10、全国211学校招生数及占比
-- 11、各省985学校分布
-- 12、各省211学校分布
-- 13、各省双一流学校分布
-- 14、双一流学科查询
-- 15、各省高校数分布
-- 16、各省考生人数及招生计划比例（录取比例）
-- 17、其他感兴趣分析项
-- 18、分析数据可视化展示15）各省高校数分布
-- 16、各省考生人数及招生计划比例（录取比例）
-- 17、其他感兴趣分析项
-- 18、分析数据可视化展示


-- 2、学生来源分析
select city, sum(plan) as sum_plan
from 0_initial_table
where city is not null
group by city;


-- 建表统计学生来自的省份并且建立新表
create table if not exists BigDataCourse.studentSource_cnt
comment "学生来源"
as
select
       city,
       sum(plan) as sum_plan
from 0_initial_table
where city is not null
group by city;


--
--
-- 3-0 重写profess
create table if not exists update_profess_1
comment "初始表重更新profess"
as
select major_code as m_code,profess as m_name
from 0_initial_table
order by m_code ,m_name ;

create table if not exists BigDataCourse.update_profess
as
select a.m_code,a.m_name
from (select m_code, m_name, row_number() over (partition by m_code order by m_name) as rnk1
    from update_profess_1
    where m_code is not null and m_name is not null) a
where a.rnk1=1;

-- 3、全国招生最多的专业Top10
select
    major_code,
    sum(plan) as profess_cnt
from
     0_initial_table
where
    major_code is not null
order by profess_cnt desc
limit 10;

-- 删表
drop table if exists BigDataCourse.3_top10_profess;


create table if not exists BigDataCourse.top10_profess
comment "全国招生最多专业top10"
as
select a.m_code, b.m_name, a.profess_cnt
from(select
    i.major_code as m_code,
    sum(i.plan) as profess_cnt
from
     0_initial_table as i
where
    i.major_code is not null and i.major_code != 'None'
group by i.major_code
order by profess_cnt desc
limit 10) a left join update_profess as b on a.m_code=b.m_code ;

-- 4、招生最少的专业Top10
create table if not exists BigDataCourse.bottom10_profess
comment "全国招生最少专业top10"
as
select a.m_code, b.m_name, a.profess_cnt
from(select
    i.major_code as m_code,
    sum(i.plan) as profess_cnt
from
     0_initial_table as i
where
    i.major_code is not null and i.major_code != 'None'
group by i.major_code
order by profess_cnt
limit 10) a left join update_profess as b on a.m_code=b.m_code ;

-- 5、全国招生计划按省份分布

create table if not exists province_name (
    P_name string comment "省名，直辖市，自治区"
);

create table if not exists 5_province_cnt
comment "招生计划按省份分布"
as
select pn.P_name, sc.sum_plan
from province_name as pn left join 2_studentsource_cnt as sc on pn.P_name = sc.city;

-- 6、招生最多学校Top10
create table if not exists 6_most_enrollment
comment "招生最多学校Top10"
as
select uniname, sum(plan) as plan_sum
from 0_initial_table
where uniname is not null
group by uniname
order by plan_sum desc
limit 10;


-- 7、全国文科招生数
create table if not exists 7_la_sum
comment "全国文科招生数"
as
select
       subject,
       sum(plan) as la_sum
from
     0_initial_table
where
    subject = '文史'
group by subject;

-- 8、全国理科招生数
create table if not exists 8_science_sum
comment "全国理科招生数"
as
select
       subject,
       sum(plan) as s_sum
from
     0_initial_table
where
    subject = '理工'
group by subject;

-- 9、全国985学校招生数及占比
create table if not exists BigDataCourse.985_name(
  uni_name string comment "985学校名称"
);

create table if not exists BigDataCourse.sum_plan
as
select sum(plan) as number1
from 0_initial_table;

drop table if exists 9_985_part;

create table if not exists BigDataCourse.9_985_part
comment "985各个学校招生人数占比"
as
select a.uniname, a.985_cnt, a.985_cnt/sum_plan.number1 as 985_whole_ratio
from
(select ini.uniname, sum(ini.plan) as 985_cnt
from 0_initial_table as ini join 985_name on ini.uniname = 985_name.uni_name
group by ini.uniname) as a, sum_plan;

-- 10、全国211学校招生数及占比
create table if not exists BigDataCourse.211_name(
  uni_name string comment "211学校名称"
);

create table if not exists BigDataCourse.10_211_part
comment "211各个学校招生人数占比"
as
select a.uniname, a.211_cnt, a.211_cnt/sum_plan.number1 as 211_whole_ratio
from
(select ini.uniname, sum(ini.plan) as 211_cnt
from 0_initial_table as ini join 211_name on ini.uniname = 211_name.uni_name
group by ini.uniname) as a, sum_plan;


-- 11、各省985学校分布
drop table if exists province_985;

create table if not exists BigDataCourse.province_985(
    city string,
    uni_name string
)row format delimited fields terminated by '\t';

-- 12、各省211学校分布
create table if not exists BigDataCourse.province_211(
    city string,
    uni_name string
)row format delimited fields terminated by '\t';

-- 13、各省双一流学校分布


-- 14、双一流学科查询


-- 15、各省高校数分布


-- 16、各省考生人数及招生计划比例（录取比例）
-- 17、其他感兴趣分析项
-- 18、分析数据可视化展示15）各省高校数分布
-- 16、各省考生人数及招生计划比例（录取比例）
-- 17、其他感兴趣分析项
-- 18、分析数据可视化展示

