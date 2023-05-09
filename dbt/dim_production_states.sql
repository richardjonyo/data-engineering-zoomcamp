select
	state,
    state_code,
    region,
    division,
    basin,
    east_west_mississippi_river,
    interior,
    coal_state	
from {{ ref('states_lookup')}}
where coal_state = 1