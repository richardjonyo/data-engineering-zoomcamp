{% macro categorize_state(state) -%}

    case {{ state }}
        when  'Eastern (KY)' then 'Region'
        when  'Western (KY)' then 'Region'
        when  'Northern (WV)' then 'Region'
        when  'Southern (WV)' then 'Region'
        when  'Interior Total' then 'Region'
        when  'Appalachian Total' then 'Region'
        when  'Western Total' then 'Region'
        when  'East of Mississippi River' then 'Region'
        when  'West of Mississippi River' then 'Region'
        when  'Bituminous and Lignite' then 'Region'
        when  'Anthracite (PA)' then 'Region'
        when  'Bituminous (PA)' then 'Region'
        when  'US Total' then 'Region'
        else 'State'
    end

{%- endmacro %}