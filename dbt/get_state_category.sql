{% macro get_state_category(state) -%}
    
      case {{ state }}
        when state LIKE '%Total%' then 'Region'
        else 'Single state'
    end

{%- endmacro %}