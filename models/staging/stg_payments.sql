{{
  config(
    materialized='view'
  )
}}


with source_payments as (

    select * from {{ source('stripe', 'payment') }}

),

renamed_payments as (

    select
       id as payment_id
     , orderid as order_id
     , paymentmethod as payment_method
     , status
     , (amount)/100 ::decimal (19,2) as order_amount
    from source_payments
    where status = 'success'
)

select * from renamed_payments