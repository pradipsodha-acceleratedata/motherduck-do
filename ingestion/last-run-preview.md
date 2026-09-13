# Bronze Load Preview

## Run Summary

| Resource | Write Disposition | Records | Table |
|----------|-------------------|---------|-------|
| account | merge (incremental, LastModifiedDate) | 33 | `main.account` |
| contact | replace | 37 | `main.contact` |

## Schema

### account (51 columns)

Key columns: `id`, `name`, `type`, `industry`, `annual_revenue`, `phone`, `website`, `billing_city`, `billing_state`, `billing_country`, `owner_id`, `created_date`, `last_modified_date`, `is_deleted`, `is_partner`, `is_customer_portal`, `number_of_employees`, `ownership`, `ticker_symbol`, `rating`, `description`, `customer_priority__c`, `sla__c`, `active__c`, `upsell_opportunity__c`, `_dlt_id`, `_dlt_load_id`

### contact (51 columns)

Key columns: `id`, `first_name`, `last_name`, `email`, `title`, `phone`, `mobile_phone`, `department`, `account_id`, `owner_id`, `created_date`, `last_modified_date`, `birthdate`, `lead_source`, `mailing_city`, `mailing_state`, `mailing_country`, `is_email_bounced`, `is_priority_record`, `level__c`, `languages__c`, `description`, `_dlt_id`, `_dlt_load_id`

## Sample Data

### account (sparse sample, 5 rows)

| id | name | type | industry | phone | website |
|---|---|---|---|---|---|
| 001fj00000d0E7sAAE | ABC Trading Corp | | | | |
| 001fj00000d1FkvAAE | ABC Trading Pvt Ltd | | | | |
| 001fj00000dZAPTAA4 | Akshaya Patra Foundation | NGO | | | |
| 001fj00000dZAVxAAO | Bill & Melinda Gates Foundation | Foundation | | | |
| 001fj00000bOtE7AAK | Burlington Textiles Corp of America | Customer - Direct | Apparel | (336) 222-7000 | www.burlington.com |

### contact (sparse sample, 5 rows)

| id | first_name | last_name | email | title | phone | account_id |
|---|---|---|---|---|---|---|
| 003fj000015IjbBAAS | Aparna | Aparna | aparna.singh@augustinnovate.com | | | |
| 003fj00000ZIrlzAAD | Tim | Barr | barr_tim@grandhotels.com | SVP, Administration and Finance | (312) 596-1000 | 001fj00000bP1IAAA0 |
| 003fj00000ZIrm0AAD | John | Bond | bond_john@grandhotels.com | VP, Facilities | (312) 596-1000 | 001fj00000bP1IAAA0 |
| 003fj00000ZIrm2AAD | Lauren | Boyle | lboyle@uog.com | SVP, Technology | (212) 842-5500 | 001fj00000bP1IBAA0 |
| 003fj00000ZIrm9AAD | Liz | D'Cruz | ldcruz@uog.com | VP, Production | (650) 450-8810 | 001fj00000bP1IFAA0 |

## Tier-1 Checks

| Check | Status |
|-------|--------|
| account._dlt_id non-null unique | ✅ PASS (33/33) |
| contact._dlt_id non-null unique | ✅ PASS (37/37) |