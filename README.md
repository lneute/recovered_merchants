# Merchants Recuperados

## Contexto:
Hoje dentro do time de FVI (Força de Vendas Interna) precisamos saber quais clientes já eram clientes da SumUp porém ficaram muito tempo sem realizar transações e compraram um leitor novo do nosso time de FVI.

Os critérios para classificar o merchant como recuperado são:
* Ter comprado uma máquina com o time de FVI;
* Ser cliente antigo;
* O intervalo entre a ***sua última transação antes da data da compra*** e a ***data da compra*** deve ser *>=* (maior ou igual) a **90** dias
> O cálculo ficaria assim: data da compra - última transação antes da data da compra

Query para identificar as vendas para merchants antigos de FVI:
```sql 
select 
	s.* 
from attribution.sales s 
inner join reports.fvi_employee_data fed on s.facilitator_code = fed.partner_code 
where 1=1 --Pode ignorar, serve apenas para facilitar a adição/remoção de filtros
and s."type" != 'transaction_count_activation' --Devemos desprezar todas as linhas com essa informação, ela é algo exclusivo para os mercado Europeu
and s.deleted_at isnull --Filtro de vendas que foram removidas da base, não devemos leva-las em consideração
and s.cancelled_at isnull --Filtro de vendas que foram canceladas, não devemos leva-las em consideração
and not s.merchant_attribution --Filtro de clientes antigos.
```
> Perceba que deixei um comentário em cada ***and statment*** para facilitar o entendimento sobre os filtros que faço na query

Para que tudo funcione eu dividi o processo em algumas etapas:
1. Criar um base com todos os merchants que são antigos e que vieram de FVI:
    ```sql
    drop table if exists analytics_sandbox.recovered_merchants_fvi; --Apaga a tabela caso ela exista.

    -- Daqui em diante é criado a tabela analytics_sandbox.recovered_merchants_fvi
    create table analytics_sandbox.recovered_merchants_fvi as 
    select distinct on (s.merchant_code)
        s.facilitator_code,
        s.occurred_on,
        s.merchant_code,
        s.serial_number
    from attribution.sales s 
    where s."type" != 'transaction_count_activation'
    and deleted_at isnull 
    and cancelled_at isnull
    and not s.merchant_attribution
    and s.facilitator_code in (select partner_code from reports.fvi_employee_data fed)
    order by s.merchant_code, s.occurred_on asc
    ```
    > Da forma em que eu criei a query ela vai sempre pegar a primeira ocorrencia (mais antiga) de venda (CRS) dos merchants de FVI.

    A expectativa de retorno é uma base estruturada da seguinte maneira:

    |facilitator_code|occurred_on|merchant_code|serial_number|
    |----------------|-------|-|-|
    |é o mesmo que partner_code|data em que o merchant comprou a máquina|Identificação alfanumerica do merchant|serial comprado

1. Agora é necessário uma base com as transações dos merchants, pois, a partir delas conseguiremos fazer o calculo e descobrir quem teve sua ultima tx (transação) há mais de 90 dias
    ```sql
    --Apaga a tabela caso ela exista.
    drop table if exists analytics_sandbox.recovered_merchants_fvi_transactions;

    --Daqui em diante é criado a tabela analytics_sandbox.recovered_merchants_fvi_transactions
    create table analytics_sandbox.recovered_merchants_fvi_transactions as
    select
    	dm.merchant_code,
    	v.event_updated_at as tx_time,
    	v.tpv_accounting_amount as amount,
    	v.transaction_id,
    	v.card_reader_id as external_reader_id
    FROM olap.v_p_fact_transaction_and_fee v 
    JOIN olap.v_p_dim_product_group pg on v.dim_product_group_id = pg.dim_product_group_id
    JOIN olap.v_m_dim_merchant dm ON dm.dim_merchant_id = v.dim_merchant_id
    LEFT JOIN olap.v_m_dim_cooperation coo on dm.dim_merchant_id = coo.dim_merchant_id
    WHERE dm.is_test is false 
    AND tx_result = 11 
    AND pg.technology <> 'cash' 
    AND (coo.cooperation_type <> 'Internal' OR coo.cooperation_type IS null)
    and dm.merchant_code in (select merchant_code from analytics_sandbox.recovered_merchants_fvi)
    order by dm.merchant_code, v.event_updated_at desc
    ```

1. Aqui começam todos os cálculos, Nesta etapa basicamente juntamos as bases criadas até o momento para conseguirmos descobrir quais merchants compraram máquinas qual é o intervalo (em dias) entre a ultima transação antes da compra da nova maquina e a data da compra.
    ```sql
    with big_data as (
    select 
        rmf.*, 
        txs.tx_time,
        txs.transaction_id,
        cr.code,
        extract(day from date_trunc('day', rmf.occurred_on) - date_trunc('day', txs.tx_time)) as tx_interval
    from analytics_sandbox.recovered_merchants_fvi rmf 
    left join analytics_sandbox.recovered_merchants_fvi_transactions txs on rmf.merchant_code = txs.merchant_code
    left join card_readers cr on txs.external_reader_id = cr.id 
    order by rmf.merchant_code, txs.tx_time)
    select distinct on (bg.merchant_code)
        bg.*
    from big_data bg
    where 1=1 
    and tx_time < occurred_on 
    and bg.code != bg.serial_number
    order by bg.merchant_code, bg.tx_time desc
    ```
> Lembre-se, O retorno da query acima é uma base contendo apenas as informações de compra da máquina, informações da ultima tx realizada antes da compra e também o intervalo entre as duas datas, você precisará quando for usar essa base, realizar o filtro dos 90 dias.

# Known Bugs
* Caso um merchant tenha comprado com FVI e fique mais de 90 dias sem transacionar e depois comprar outra máquina ele não entrará como recuperado, isso acontece por conta da forma que a query foi montada.
    - Ela sempre ira retornar a primeira compra que esse merchant fez, a segunda não será considerada, cabe a nós entender se queremos manter esse comportamento ou se devemos considerar o mesmo merchant como recuperado sempre que ele ficar 90 ou mais dias sem transacionar e depois compre outras máquinas.

# Informações adicionais
1. Você pode trocar nas queries que criam tabelas os nomes das tabelas que eu defini, apenas lember-se de trocar em todos os momentos em que elas aparecem.
