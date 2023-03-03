with sales as(
    select
        *
    from
        {{ ref('vm_sales') }}
),
compradores as(
    select
        (firstname || ' ' || lastname) as Nome_Completo,
        userid
    from
        users
),
top_10_compradores as (
    select
        compradores.Nome_Completo as Nome_Completo,
        sum(sales.QUANTIDADE_VENDIDA) as Vendas_Totais,
        sales.COMISSAO as Comissao
    from
        sales 
        inner join compradores on sales.ID_COMPRADOR = compradores.userid
    group by
        compradores.userid,
        compradores.Nome_Completo, 
        sales.Comissao
    order by 
        Vendas_Totais desc
    limit 10
)

select 
    * 
from 
    top_10_compradores