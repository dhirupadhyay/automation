With T as (select
 
gl.service_group_level_1 as SGL,
 
count(distinct gf.peoplekey_id ) as Unique_Clicks, 1 as Ref from prd-65343-datalake-bd-88394358.mcdatalakepe_103875_in.anlc_gma_fact gf
left join prd-65343-datalake-bd-88394358.mcdatalakepe_103875_in.anlc_dim_peoplekey pk on gf.peoplekey_id = pk.peoplekey_id
left join prd-65343-datalake-bd-88394358.mcdatalakepe_103875_in.anlc_dim_people_service_group_level gl on gl.service_group_level_rk = pk.service_group_level_rk
where gf.action in ('click','Click')
group by 1
order by 2 desc),T1 as
 
(select  count(distinct gf.peoplekey_id ) as Unique_Clicks,1 as Ref from prd-65343-datalake-bd-88394358.mcdatalakepe_103875_in. anlc_gma_fact gf
 
where action in ('click','Click')
group by 2)
 
select T.SGL,Round(((T.Unique_Clicks/T1.Unique_Clicks)*100),2) as UC from T,T1
order by 2 DESC