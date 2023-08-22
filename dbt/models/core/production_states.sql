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
    select eia_id,state,year,week_1,week_2,week_3,week_4,week_5,week_6,week_7,week_8,week_9,week_10,
    week_11,week_12,week_13,week_14,week_15,week_16,week_17,week_18,week_19,week_20,
    week_21,week_22,week_23,week_24,week_25,week_26,week_27,week_28,week_29,week_30,
    week_31,week_32,week_33,week_34,week_35,week_36,week_37,week_38,week_39,week_40,
    week_41,week_42,week_43,week_44,week_45,week_46,week_47,week_48,week_49,week_50,
    week_51,week_52,week_53,annual_average,annual_total, state_category
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