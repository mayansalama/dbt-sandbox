{% macro date_part(part, col_name) %}

	{% if target.type == 'bigquery' %}
	  {{ return( "EXTRACT(" + part + " FROM " + col_name + ")" ) }}
	{% else %}
	  {{ return( "DATE_PART('" + part + "', " + col_name + ")" ) }}
	{% endif %}

{% endmacro %}
