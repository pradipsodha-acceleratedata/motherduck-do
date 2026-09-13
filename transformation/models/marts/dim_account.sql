WITH source AS (
    SELECT
        account_id,
        account_name,
        type,
        industry,
        annual_revenue,
        number_of_employees,
        ownership,
        ticker_symbol,
        rating,
        phone,
        fax,
        website,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        description,
        owner_id,
        created_date,
        last_modified_date,
        system_modstamp,
        is_partner,
        is_customer_portal,
        clean_status,
        customer_priority,
        sla,
        active,
        number_of_locations,
        upsell_opportunity,
        sla_serial_number,
        sla_expiration_date,
        last_viewed_date,
        last_referenced_date
    FROM {{ ref('silver_salesforce_account') }}
)

final AS (
    -- one row per account_id
    SELECT
        *,
        CURRENT_TIMESTAMP AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id,
        '{{ project_git_sha() }}' AS _git_sha
    FROM source
)

SELECT * FROM final