with orders as (
    select * from {{ ref('stg_orders') }}
)

, payments as (
    select * from {{ ref('stg_payments') }}
)

, order_payments as (
    select 
        order_id
        , sum(order_amount) as order_amount
    from payments
    group by 1 
)

, final as (
    select
        o.order_id
        , o.customer_id
        , o.order_date
        , o.status
        , op.order_amount

    from orders o
    join order_payments op 
        on op.order_id = o.order_id
)

select * from final