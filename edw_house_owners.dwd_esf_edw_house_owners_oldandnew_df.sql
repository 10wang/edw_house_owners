-- ========================================================================================
-- Script Name : edw_house_owners.dwd_esf_edw_house_owners_oldandnew_df.sql
-- Purpose : 
-- Source Table : 
-- Target Table : edw_house_owners.dwd_esf_edw_house_owners_oldandnew_df
------------------------------------ Change Log -------------------------------------------
-- Date Generated   Updated By     Description
-------------------------------------------------------------------------------------------
-- 2016-07-05       Li Jiang     Initial Version
-- ========================================================================================

alter table edw_house_owners.dwd_esf_edw_house_owners_oldandnew_df drop if exists partition (dt='${dt}');
alter table edw_house_owners.dwd_esf_edw_house_owners_oldandnew_df add if not exists partition (dt='${dt}');

insert overwrite table edw_house_owners.dwd_esf_edw_house_owners_oldandnew_df partition (dt='${dt}')
select      owner_id
            ,if(create_time < date_sub(to_date(from_unixtime(unix_timestamp('${dt}','yyyyMMdd'))),6),2,1)  as onwer_oldandnew_type_code
            ,'${wf:id()}'                                                              as load_job_number
            ,'${wf:name()}'                                                            as load_job_name
            ,current_timestamp                                                         as insert_timestamp
            ,1                                                                         as source_system_code
from (
            select      owner_id,
                        create_time,
                        row_number() over(partition by owner_id order by create_time asc) as rank
            from        ods_edw_house.ods_t_second_house
            where       dt='${dt}'
            and         city_id in (3,121,267,852,1337,2316)
            and         house_property=2
            and         (house_audit_status=1 or house_audit_status=2)
            and         house_is_delete = 0
     ) t
where  t.rank = 1
