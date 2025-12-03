select distinct
*
from (
    select distinct
    ide.NUMBER_EVENT as numero_evento,
    iss.DESCRIPTION as status,
    CAST(ide.DATE_EVENT as DATE) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    ider.COVERAGE as coverage,
    case b.DESCRIPTION
    	when 'REPARAÇÃO A TERCEIROS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	when 'ASSISTÊNCIA 24 HORAS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	else b.DESCRIPTION
    end as beneficio,
    coalesce(iv.id, it.id) as id_placa,
    coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa,
    coalesce(ma.DESCRICAO, coalesce(maa.DESCRICAO, it.brand)) as marca,
    coalesce(iv.year_model, it.year_model) as ano_modelo,
    --orcamento_coverage.AMOUNT as soma_orcamento,
    --quantidade_coverage.CONTAGEM as contagem_placas,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as despesa_casco,
    --assistencia.valor,
    --quantidade_placa.quantidade,
    cast(coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10,2)) as despesa_auxiliar,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) + coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10, 2)) as valor_casco,
	case 
		when tc.PARTICIPATORY_QUOTA = 0 then cast(round(
				coalesce((ider.quota_value / nullif(quantidade_coverage.CONTAGEM, 0)), 0) / nullif(irsc.VEHICLE_VALUE, 0)
				, 2) * 100
			as decimal(10, 2)) 
		else tc.PARTICIPATORY_QUOTA 
	end as cota_porcen, -- coluna acrescentada por Lucas. Foi usado a função 'case' pois havia cotas zeradas e para arrumar isso fiz um cálculo manual
    cast(coalesce((ider.quota_value / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as cota, 
    'Segtruck' as cooperativa
    
    from silver.INSURANCE_DAM_EVENT ide
    left join silver.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join silver.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    left join silver.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join silver.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join silver.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    left join silver.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    left join silver.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    left join silver.CATEGORY c on c.ID = tc.ID_CATEGORY
    left join silver.BENEFITS b on b.ID = c.ID_BENEFITS
    left join silver.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join silver.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join silver.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join silver.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
    left join silver.representante r on r.codigo = ide.id_unity
    left join silver.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join silver.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join silver.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    left join silver.marca_veiculo maa on maa.codigo = it.code_brand_vehicle
    left join silver.insurance_status iss on iss.id = ide.ID_STATUS
    
    left join ( -- subquery para pegar a soma dos valores de orçamento por coverage
    	select
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	sum(AMOUNT) as AMOUNT
    	from (
    		select 
    		ide.NUMBER_EVENT,
    		ider.COVERAGE,
    		ideb.ID,
    		ideb.AMOUNT
    		from silver.INSURANCE_DAM_EVENT_BUDGET ideb
    		left join silver.INSURANCE_DAM_EVENT_REFERENCE ider on ider.ID = ideb.PARENT
    		left join silver.INSURANCE_DAM_EVENT ide on ide.ID = ider.PARENT
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as orcamento_coverage on orcamento_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and orcamento_coverage.COVERAGE = ider.COVERAGE
    
    left join ( -- subquery para pegar a quantidade de coverages por evento
    	select distinct
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	count(bx.coverage) as CONTAGEM
    	from (
    		select distinct
    		ide.NUMBER_EVENT,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as PLACA,
    		ider.COVERAGE
    		from silver.INSURANCE_DAM_EVENT ide
    		left join silver.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join silver.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    		left join silver.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join silver.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join silver.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as quantidade_coverage on quantidade_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and quantidade_coverage.COVERAGE = ider.COVERAGE
    
    left join ( -- subquery para pegar as despesas auxiliares de casco que se encontram na area da assistencia
    	select distinct
    	ide.number_event as numero_evento,
    	sum(bx.valor) as valor
    	from silver.INSURANCE_DAM_EVENT ide
    	left join (
    		select distinct
    		ia.NUMBER_EVENT as numero_evento,
    		IAB.PARENT as atendimento,
    		iab.BENEFIT as beneficio,
    		iap.ID as id_despesa,
    		iap.assistance_total as valor
    		from silver.INSURANCE_ASSISTANCE_BENEFITS iab
    		left join silver.INSURANCE_ASSISTANCE_PROVIDERS iap on iap.COVERAGE_ID = iab.coverage and iab.PARENT = iap.PARENT
    		left join silver.INSURANCE_ASSISTANCE ia on ia.ID = iab.PARENT
    		where 
    		iap.CANCEL <> 'S'
    		and iab.BENEFIT in ('REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'Reparação ou Reposição do (SEMI)REBOQUE', 'Reparação ou Reposição do VEÍCULO')
    	) as bx on bx.numero_evento = ide.NUMBER_EVENT
    	group by ide.NUMBER_EVENT
    ) as assistencia on assistencia.numero_evento = ide.NUMBER_EVENT
    
    left join ( -- subquery para pegar a quantidade de reboques que há em um coverage
    	select distinct
    	bx.numero_evento,
    	count(bx.numero_evento) as quantidade
    	from (
    		select distinct
    		ide.NUMBER_EVENT as numero_evento,
    		ider.COVERAGE as coverage,
    		coalesce(iv.id, it.id) as id_placa,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa
    		from silver.INSURANCE_DAM_EVENT ide
    		left join silver.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join silver.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join silver.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join silver.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.numero_evento
    ) as quantidade_placa on quantidade_placa.numero_evento = ide.NUMBER_EVENT
    
    where 
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like '%RR%'
) as casco_seg

-------------
union all
-------------

select
*
from(
    select distinct
    ide.NUMBER_EVENT as numero_evento,
    iss.description as status,
    CAST(ide.DATE_EVENT as DATE) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    ider.COVERAGE as coverage,
    case b.DESCRIPTION
    	when 'REPARAÇÃO A TERCEIROS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	when 'ASSISTÊNCIA 24 HORAS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	else b.DESCRIPTION
    end as beneficio,
    coalesce(iv.id, it.id) as id_placa,
    coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa,
    coalesce(ma.DESCRICAO, coalesce(maa.DESCRICAO, it.brand)) as marca,
    coalesce(iv.year_model, it.year_model) as ano_modelo,
    --orcamento_coverage.AMOUNT as soma_orcamento,
    --quantidade_coverage.CONTAGEM as contagem_placas,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as despesa_casco,
    --assistencia.valor,
    --quantidade_placa.quantidade,
    cast(coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10,2)) as despesa_auxiliar,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) + coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10, 2)) as valor_casco,
	case 
		when tc.PARTICIPATORY_QUOTA = 0 then cast(round(
				coalesce((ider.quota_value / nullif(quantidade_coverage.CONTAGEM, 0)), 0) / nullif(irsc.VEHICLE_VALUE, 0)
				, 2) * 100
			as decimal(10, 2)) 
		else tc.PARTICIPATORY_QUOTA 
	end as cota_porcen, -- coluna acrescentada por Lucas. Foi usado a função 'case' pois havia cotas zeradas e para arrumar isso fiz um cálculo manual
    cast(coalesce((ider.quota_value / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as cota, 
    'Stcoop' as cooperativa  
    
    from stcoop.INSURANCE_DAM_EVENT ide
    left join stcoop.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join stcoop.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    left join stcoop.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join stcoop.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join stcoop.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    left join stcoop.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    left join stcoop.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    left join stcoop.CATEGORY c on c.ID = tc.ID_CATEGORY
    left join stcoop.BENEFITS b on b.ID = c.ID_BENEFITS
    left join stcoop.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join stcoop.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join stcoop.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join stcoop.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
    left join stcoop.representante r on r.codigo = ide.id_unity
    left join stcoop.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join stcoop.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join stcoop.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    left join stcoop.marca_veiculo maa on maa.codigo = it.code_brand_vehicle
    left join stcoop.insurance_status iss on iss.id = ide.ID_STATUS   
    left join ( -- subquery para pegar a soma dos valores de orçamento por coverage
    	select
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	sum(AMOUNT) as AMOUNT
    	from (
    		select 
    		ide.NUMBER_EVENT,
    		ider.COVERAGE,
    		ideb.ID,
    		ideb.AMOUNT
    		from stcoop.INSURANCE_DAM_EVENT_BUDGET ideb
    		left join stcoop.INSURANCE_DAM_EVENT_REFERENCE ider on ider.ID = ideb.PARENT
    		left join stcoop.INSURANCE_DAM_EVENT ide on ide.ID = ider.PARENT
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as orcamento_coverage on orcamento_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and orcamento_coverage.COVERAGE = ider.COVERAGE
    
    left join ( -- subquery para pegar a quantidade de coverages por evento
    	select distinct
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	count(bx.coverage) as CONTAGEM
    	from (
    		select distinct
    		ide.NUMBER_EVENT,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as PLACA,
    		ider.COVERAGE
    		from stcoop.INSURANCE_DAM_EVENT ide
    		left join stcoop.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join stcoop.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    		left join stcoop.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join stcoop.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join stcoop.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as quantidade_coverage on quantidade_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and quantidade_coverage.COVERAGE = ider.COVERAGE
    
    left join ( -- subquery para pegar as despesas auxiliares de casco que se encontram na area da assistencia
    	select distinct
    	ide.number_event as numero_evento,
    	sum(bx.valor) as valor
    	from stcoop.INSURANCE_DAM_EVENT ide
    	left join (
    		select distinct
    		ia.NUMBER_EVENT as numero_evento,
    		IAB.PARENT as atendimento,
    		iab.BENEFIT as beneficio,
    		iap.ID as id_despesa,
    		iap.assistance_total as valor
    		from stcoop.INSURANCE_ASSISTANCE_BENEFITS iab
    		left join stcoop.INSURANCE_ASSISTANCE_PROVIDERS iap on iap.COVERAGE_ID = iab.coverage and iab.PARENT = iap.PARENT
    		left join stcoop.INSURANCE_ASSISTANCE ia on ia.ID = iab.PARENT
    		where 
    		iap.CANCEL <> 'S'
    		and iab.BENEFIT in ('REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'Reparação ou Reposição do (SEMI)REBOQUE', 'Reparação ou Reposição do VEÍCULO')
    	) as bx on bx.numero_evento = ide.NUMBER_EVENT
    	group by ide.NUMBER_EVENT
    ) as assistencia on assistencia.numero_evento = ide.NUMBER_EVENT
    
    left join ( -- subquery para pegar a quantidade de reboques que há em um coverage
    	select distinct
    	bx.numero_evento,
    	count(bx.numero_evento) as quantidade
    	from (
    		select distinct
    		ide.NUMBER_EVENT as numero_evento,
    		ider.COVERAGE as coverage,
    		coalesce(iv.id, it.id) as id_placa,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa
    		from stcoop.INSURANCE_DAM_EVENT ide
    		left join stcoop.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join stcoop.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join stcoop.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join stcoop.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.numero_evento
    ) as quantidade_placa on quantidade_placa.numero_evento = ide.NUMBER_EVENT
    
    where 
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like '%RR%'
) as casco_st

-------------
union all
-------------

select
*
from(
    select distinct
    ide.NUMBER_EVENT as numero_evento,
    iss.description as status,
    CAST(ide.DATE_EVENT as DATE) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    ider.COVERAGE as coverage,
    case b.DESCRIPTION
    	when 'REPARAÇÃO A TERCEIROS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	when 'ASSISTÊNCIA 24 HORAS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	else b.DESCRIPTION
    end as beneficio,
    coalesce(iv.id, it.id) as id_placa,
    coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa,
    coalesce(ma.DESCRICAO, coalesce(maa.DESCRICAO, it.brand)) as marca,
    coalesce(iv.year_model, it.year_model) as ano_modelo,
    --orcamento_coverage.AMOUNT as soma_orcamento,
    --quantidade_coverage.CONTAGEM as contagem_placas,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as despesa_casco,
    --assistencia.valor,
    --quantidade_placa.quantidade,
    cast(coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10,2)) as despesa_auxiliar,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) + coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10, 2)) as valor_casco,
	case 
		when tc.PARTICIPATORY_QUOTA = 0 then cast(round(
				coalesce((ider.quota_value / nullif(quantidade_coverage.CONTAGEM, 0)), 0) / nullif(irsc.VEHICLE_VALUE, 0)
				, 2) * 100
			as decimal(10, 2)) 
		else tc.PARTICIPATORY_QUOTA 
	end as cota_porcen, -- coluna acrescentada por Lucas. Foi usado a função 'case' pois havia cotas zeradas e para arrumar isso fiz um cálculo manual
    cast(coalesce((ider.quota_value / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as cota, 
    'Viavante' as cooperativa  

    from viavante.INSURANCE_DAM_EVENT ide
    left join viavante.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join viavante.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    left join viavante.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join viavante.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join viavante.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    left join viavante.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    left join viavante.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    left join viavante.CATEGORY c on c.ID = tc.ID_CATEGORY
    left join viavante.BENEFITS b on b.ID = c.ID_BENEFITS
    left join viavante.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join viavante.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join viavante.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join viavante.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
    left join viavante.representante r on r.codigo = ide.id_unity
    left join viavante.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join viavante.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join viavante.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    left join viavante.marca_veiculo maa on maa.codigo = it.code_brand_vehicle
    left join viavante.insurance_status iss on iss.id = ide.ID_STATUS   
    left join ( -- subquery para pegar a soma dos valores de orçamento por coverage
    	select
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	sum(AMOUNT) as AMOUNT
    	from (
    		select 
    		ide.NUMBER_EVENT,
    		ider.COVERAGE,
    		ideb.ID,
    		ideb.AMOUNT
    		from viavante.INSURANCE_DAM_EVENT_BUDGET ideb
    		left join viavante.INSURANCE_DAM_EVENT_REFERENCE ider on ider.ID = ideb.PARENT
    		left join viavante.INSURANCE_DAM_EVENT ide on ide.ID = ider.PARENT
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as orcamento_coverage on orcamento_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and orcamento_coverage.COVERAGE = ider.COVERAGE

    left join ( -- subquery para pegar a quantidade de coverages por evento
    	select distinct
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	count(bx.coverage) as CONTAGEM
    	from (
    		select distinct
    		ide.NUMBER_EVENT,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as PLACA,
    		ider.COVERAGE
    		from viavante.INSURANCE_DAM_EVENT ide
    		left join viavante.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join viavante.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    		left join viavante.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join viavante.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join viavante.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 

    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as quantidade_coverage on quantidade_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and quantidade_coverage.COVERAGE = ider.COVERAGE

    left join ( -- subquery para pegar as despesas auxiliares de casco que se encontram na area da assistencia
    	select distinct
    	ide.number_event as numero_evento,
    	sum(bx.valor) as valor
    	from viavante.INSURANCE_DAM_EVENT ide
    	left join (
    		select distinct
    		ia.NUMBER_EVENT as numero_evento,
    		IAB.PARENT as atendimento,
    		iab.BENEFIT as beneficio,
    		iap.ID as id_despesa,
    		iap.assistance_total as valor
    		from viavante.INSURANCE_ASSISTANCE_BENEFITS iab
    		left join viavante.INSURANCE_ASSISTANCE_PROVIDERS iap on iap.COVERAGE_ID = iab.coverage and iab.PARENT = iap.PARENT
    		left join viavante.INSURANCE_ASSISTANCE ia on ia.ID = iab.PARENT
    		where 
    		iap.CANCEL <> 'S'
    		and iab.BENEFIT in ('REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'Reparação ou Reposição do (SEMI)REBOQUE', 'Reparação ou Reposição do VEÍCULO')
    	) as bx on bx.numero_evento = ide.NUMBER_EVENT
    	group by ide.NUMBER_EVENT
    ) as assistencia on assistencia.numero_evento = ide.NUMBER_EVENT

    left join ( -- subquery para pegar a quantidade de reboques que há em um coverage
    	select distinct
    	bx.numero_evento,
    	count(bx.numero_evento) as quantidade
    	from (
    		select distinct
    		ide.NUMBER_EVENT as numero_evento,
    		ider.COVERAGE as coverage,
    		coalesce(iv.id, it.id) as id_placa,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa
    		from viavante.INSURANCE_DAM_EVENT ide
    		left join viavante.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join viavante.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join viavante.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join viavante.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.numero_evento
    ) as quantidade_placa on quantidade_placa.numero_evento = ide.NUMBER_EVENT

    where 
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like '%RR%'
) as casco_viavante

-------------
union all
-------------

select
*
from(
    select distinct
    ide.NUMBER_EVENT as numero_evento,
    iss.description as status,
    CAST(ide.DATE_EVENT as DATE) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    ider.COVERAGE as coverage,
    case b.DESCRIPTION
    	when 'REPARAÇÃO A TERCEIROS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	when 'ASSISTÊNCIA 24 HORAS' then 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO'
    	else b.DESCRIPTION
    end as beneficio,
    coalesce(iv.id, it.id) as id_placa,
    coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa,
    coalesce(ma.DESCRICAO, coalesce(maa.DESCRICAO, it.brand)) as marca,
    coalesce(iv.year_model, it.year_model) as ano_modelo,
    --orcamento_coverage.AMOUNT as soma_orcamento,
    --quantidade_coverage.CONTAGEM as contagem_placas,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as despesa_casco,
    --assistencia.valor,
    --quantidade_placa.quantidade,
    cast(coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10,2)) as despesa_auxiliar,
    cast(coalesce((orcamento_coverage.AMOUNT / quantidade_coverage.CONTAGEM), 0) + coalesce((assistencia.valor / quantidade_placa.quantidade), 0) as decimal(10, 2)) as valor_casco,
	case 
		when tc.PARTICIPATORY_QUOTA = 0 then cast(round(
				coalesce((ider.quota_value / nullif(quantidade_coverage.CONTAGEM, 0)), 0) / nullif(irsc.VEHICLE_VALUE, 0)
				, 2) * 100
			as decimal(10, 2)) 
		else tc.PARTICIPATORY_QUOTA 
	end as cota_porcen, -- coluna acrescentada por Lucas. Foi usado a função 'case' pois havia cotas zeradas e para arrumar isso fiz um cálculo manual
    cast(coalesce((ider.quota_value / quantidade_coverage.CONTAGEM), 0) as decimal(10, 2)) as cota, 
    'Tag' as cooperativa  

    from tag.INSURANCE_DAM_EVENT ide
    left join tag.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join tag.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    left join tag.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join tag.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join tag.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    left join tag.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    left join tag.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    left join tag.CATEGORY c on c.ID = tc.ID_CATEGORY
    left join tag.BENEFITS b on b.ID = c.ID_BENEFITS
    left join tag.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join tag.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join tag.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join tag.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
    left join tag.representante r on r.codigo = ide.id_unity
    left join tag.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join tag.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join tag.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    left join tag.marca_veiculo maa on maa.codigo = it.code_brand_vehicle
    left join tag.insurance_status iss on iss.id = ide.ID_STATUS   
    left join ( -- subquery para pegar a soma dos valores de orçamento por coverage
    	select
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	sum(AMOUNT) as AMOUNT
    	from (
    		select 
    		ide.NUMBER_EVENT,
    		ider.COVERAGE,
    		ideb.ID,
    		ideb.AMOUNT
    		from tag.INSURANCE_DAM_EVENT_BUDGET ideb
    		left join tag.INSURANCE_DAM_EVENT_REFERENCE ider on ider.ID = ideb.PARENT
    		left join tag.INSURANCE_DAM_EVENT ide on ide.ID = ider.PARENT
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as orcamento_coverage on orcamento_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and orcamento_coverage.COVERAGE = ider.COVERAGE

    left join ( -- subquery para pegar a quantidade de coverages por evento
    	select distinct
    	bx.NUMBER_EVENT,
    	bx.COVERAGE,
    	count(bx.coverage) as CONTAGEM
    	from (
    		select distinct
    		ide.NUMBER_EVENT,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as PLACA,
    		ider.COVERAGE
    		from tag.INSURANCE_DAM_EVENT ide
    		left join tag.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join tag.INSURANCE_DAM_EVENT_BUDGET ideb on ideb.PARENT = ider.ID
    		left join tag.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join tag.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join tag.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 

    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.NUMBER_EVENT,
    	bx.COVERAGE
    ) as quantidade_coverage on quantidade_coverage.NUMBER_EVENT = ide.NUMBER_EVENT and quantidade_coverage.COVERAGE = ider.COVERAGE

    left join ( -- subquery para pegar as despesas auxiliares de casco que se encontram na area da assistencia
    	select distinct
    	ide.number_event as numero_evento,
    	sum(bx.valor) as valor
    	from tag.INSURANCE_DAM_EVENT ide
    	left join (
    		select distinct
    		ia.NUMBER_EVENT as numero_evento,
    		IAB.PARENT as atendimento,
    		iab.BENEFIT as beneficio,
    		iap.ID as id_despesa,
    		iap.assistance_total as valor
    		from tag.INSURANCE_ASSISTANCE_BENEFITS iab
    		left join tag.INSURANCE_ASSISTANCE_PROVIDERS iap on iap.COVERAGE_ID = iab.coverage and iab.PARENT = iap.PARENT
    		left join tag.INSURANCE_ASSISTANCE ia on ia.ID = iab.PARENT
    		where 
    		iap.CANCEL <> 'S'
    		and iab.BENEFIT in ('REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'Reparação ou Reposição do (SEMI)REBOQUE', 'Reparação ou Reposição do VEÍCULO')
    	) as bx on bx.numero_evento = ide.NUMBER_EVENT
    	group by ide.NUMBER_EVENT
    ) as assistencia on assistencia.numero_evento = ide.NUMBER_EVENT

    left join ( -- subquery para pegar a quantidade de reboques que há em um coverage
    	select distinct
    	bx.numero_evento,
    	count(bx.numero_evento) as quantidade
    	from (
    		select distinct
    		ide.NUMBER_EVENT as numero_evento,
    		ider.COVERAGE as coverage,
    		coalesce(iv.id, it.id) as id_placa,
    		coalesce(coalesce(iv.BOARd,it.BOARD),ider.board) as placa
    		from tag.INSURANCE_DAM_EVENT ide
    		left join tag.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join tag.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		left join tag.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    		left join tag.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER 
    		where 
    		ider.COVERAGE <> 0
    		and ide.NUMBER_EVENT like '%RR%'
    	) as bx
    	group by
    	bx.numero_evento
    ) as quantidade_placa on quantidade_placa.numero_evento = ide.NUMBER_EVENT

    where 
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like '%RR%'
    and CAST(ide.DATE_EVENT as DATE) >= DATE '2025-08-01'
) as casco_tag