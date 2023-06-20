WITH transactions_original AS (
    SELECT *
    FROM {{ ref('fact_transactions') }}
)

, transactions AS (
    SELECT
        *
        , DATE(DATE_TRUNC('month', operation_date)) AS months
    FROM transactions_original
    WHERE status = 'APPROVED' AND source = 'APP BANCARIA'
)

, maps AS (
    SELECT
        transaction_id
        , months
        , user_id AS maps
    FROM transactions
)

, transactions_tpv AS (
    SELECT
        transaction_id
        , months
        , amount AS tpv
    FROM transactions
    WHERE transaction_type = 'PAYMENTS'
)

, transactions_transfers AS (
    SELECT
        months
        , transaction_id AS transfers
    FROM transactions
    WHERE transaction_type = 'TRANSFER'
)

, final AS (
    SELECT
        transactions.months
        , transactions.bank_name
        , transactions.transaction_type
        , transfers
        , tpv
        , maps
    FROM transactions
    LEFT OUTER JOIN transactions_tpv
        ON transactions_tpv.transaction_id = transactions.transaction_id
    LEFT OUTER JOIN transactions_transfers
        ON transactions_transfers.transfers = transactions.transaction_id
    LEFT OUTER JOIN maps
        ON maps.transaction_id = transactions.transaction_id
)

SELECT *
FROM final
