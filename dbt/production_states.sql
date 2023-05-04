{{ config(materialized='table') }}
{{
 config(
   materialized = 'table',
   partition_by = {
     'field': 'year', 
     'data_type': 'integer'
   }
 )
}}

SELECT state, year, annual_average,  annual_total,
{{ get_state_category(state) }} as state_category
  FROM `dtc-gc`.`dbt_rjonyo`.`stg_eiadata`
  WHERE {{ get_state_category(state) }} = 'Single state'
 GROUP BY 1,2,3,4