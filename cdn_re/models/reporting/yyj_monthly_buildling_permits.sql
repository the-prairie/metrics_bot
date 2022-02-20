
  with yyj_building_permits as (
      select *
      from {{ ref('yyj_building_permits_source')}}
  ),

  monthly_stats as (

      select 
        last_day(issued_on) as issued_month,
        count(distinct id) as permits_requested,
        round(avg(days_to_approval),2) as average_days_to_approve,
        round(avg(work_value),2) as average_cost_of_work
      from  yyj_building_permits
      group by 1


  )

  select *
  from monthly_stats