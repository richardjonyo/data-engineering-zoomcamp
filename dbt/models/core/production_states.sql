{{
 config(
   materialized = 'view',
   partition_by = {
     'field': 'year', 
     'data_type': 'integer'
   }
 )
}}

with production_data as (
    select state, year, annual_average, annual_total, state_category
    from {{ ref('stg_eiadata') }}
),

dim_production_states as (
    select * from {{ ref('dim_production_states') }}
)
select
    state_name.State,
    State_Code,
    Year,
    Region, 
    Division, 
    Basin, 
    East_West_Mississippi_River, 
    Interior,
    annual_average,
    annual_total
from production_data
inner join dim_production_states as state_name
on production_data.state = state_name.State
where Coal_State = 1 and state_category = 'State'