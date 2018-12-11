{% macro source(table_name) %}

	{% if target.type == 'bigquery' %}
	  {{ return("`" + target.project + "`." ~ (table_name | replace("\"", "`"))) }}
	{% else %}
	  {{ return(table_name) }}
	{% endif %}

{% endmacro %}