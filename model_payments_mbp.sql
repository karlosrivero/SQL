WITH transactions_core AS (

    SELECT *
    FROM {{ ref('fact_transactions_core') }}

)

, payments AS (

    SELECT *
    FROM {{ ref('fact_payments') }}

)

, max_transaction_date AS (

    SELECT
        payment_id
        , amount
        , DATE(DATE_TRUNC('month', operation_transaction_date)) AS month_year
        , MAX(operation_transaction_date) AS date_max
    FROM transactions_core
    GROUP BY 1, 2, 3
)

, final_transactions AS (

    SELECT
        maxi.payment_id
        , maxi.amount
        , maxi.month_year
        , maxi.date_max
        , transactions_core.status
    FROM max_transaction_date AS maxi
    LEFT OUTER JOIN transactions_core
        ON transactions_core.payment_id = maxi.payment_id
    WHERE maxi.date_max = transactions_core.operation_transaction_date

)

, final AS (

    SELECT
        payments.bank_name
        , payments.bcra_id
        , payments.type
        , payments.user_id
        , final.month_year
        , final.payment_id
        , final.status
        , final.amount
    FROM payments
    LEFT OUTER JOIN final_transactions AS final

        ON final.payment_id = payments.payment_id
    WHERE final.status = 'APPROVED'

)

SELECT *
FROM final
