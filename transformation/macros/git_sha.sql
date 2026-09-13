{%- macro project_git_sha() -%}
  {{- var('git_sha', env_var('GIT_SHA', 'local')) | trim -}}
{%- endmacro -%}
