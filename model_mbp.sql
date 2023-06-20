WITH transfers AS (

    SELECT * FROM {{ ref('fact_transfers') }}

)

, pia AS (

    SELECT * FROM {{ ref('fact_pia') }}

)

, mbp AS (

    SELECT * FROM {{ ref('model_payments_mbp') }}

)

, final AS (

    SELECT
        transfers.bank_name_from AS bank_name
        , transfers.bcra_id_from AS bcra_id
        , transfers.user_id_from AS user_id
        , transfers.status_detail AS status
        , DATE(DATE_TRUNC('month', transfers.operation_date)) AS month_year
    FROM transfers
    WHERE transfers.status_detail = 'APPROVED'
    UNION ALL
    SELECT
        pia.bank_name AS bank_name
        , pia.bcra_id AS bcra_id
        , pia.user_id
        , pia.status_detail AS status
        , DATE(DATE_TRUNC('month', pia.transaction_created_at)) AS month_year
    FROM pia
    WHERE pia.status_detail = 'APPROVED'
    UNION ALL
    SELECT
        mbp.bank_name
        , mbp.bcra_id
        , mbp.user_id
        , mbp.status
        , mbp.month_year
    FROM mbp

)

SELECT *
FROM final
