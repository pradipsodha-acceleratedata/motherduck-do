WITH source AS (
    SELECT
        contact_id,
        salutation,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        fax,
        mobile_phone,
        assistant_phone,
        reports_to_id,
        mailing_street,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_country,
        other_street,
        other_city,
        other_state,
        other_postal_code,
        other_country,
        account_id,
        owner_id,
        title,
        department,
        birth_date,
        lead_source,
        description,
        is_email_opted_out,
        is_fax_opted_out,
        created_date,
        last_modified_date,
        system_modstamp,
        last_viewed_date,
        last_referenced_date,
        email_bounced_reason,
        email_bounced_date,
        clean_status,
        has_opted_out_of_tracking
    FROM {{ ref('silver_salesforce_contact') }}
)

final AS (
    -- one row per contact_id
    SELECT
        *,
        CURRENT_TIMESTAMP AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id,
        '{{ project_git_sha() }}' AS _git_sha
    FROM source
)

SELECT * FROM final