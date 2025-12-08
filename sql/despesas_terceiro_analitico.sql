select
*,
'Segtruck' as "cooperativa"
from(
    select distinct	
    ide.NUMBER_EVENT as numero_evento,
    cast(ide.DATE_EVENT as date) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    iv.BOARD as placa,
    ma.DESCRICAO as marca,
    iv.year_model as ano_modelo,
    ide.AMMOUNT_THIRD as total_terce,
    'REPARAÇÃO A TERCEIROS' as beneficio,
    contagem.contagem,
    ide.AMMOUNT_THIRD / contagem.contagem as valor_terceiro,
    coalesce(coverage.coverage, FIRST_VALUE(coverage_2.coverage) over (partition by irsc.parent order by coverage_2.coverage)) as coverage
    from silver.INSURANCE_DAM_EVENT ide
    left join silver.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join silver.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join silver.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join silver.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join silver.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join silver.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
	left join silver.representante r on r.codigo = ide.id_unity
	left join silver.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join silver.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join silver.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join silver.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
	left join silver.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle    
    left join ( -- tabela para pegar o coverage do benef�cio de terceiros da placa
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from silver.INSURANCE_REG_SET_COVERAGE irsc
    	left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join silver.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join silver.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join silver.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join silver.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 7
    ) as coverage on coverage.placa = iv.BOARD and coverage.conjunto = irsc.PARENT
    left join ( --tabela para pegar a placa correta
    	select
    	irsc.ID, 
    	b.DESCRIPTION, 
    	iv.BOARD
    	from silver.INSURANCE_REG_SET_COVERAGE irsc
    	left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join silver.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join silver.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join silver.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join silver.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 7
    ) as terceiro on terceiro.BOARD = iv.board
    left join (-- tabela para fazer a contagem de quantas placas h� no evento
    	select distinct
    	tabela.numero_event,
    	count(tabela.numero_event) as contagem
    	from(
    		select distinct	
    		ide.NUMBER_EVENT as numero_event,
    		irsc.PARENT as conjunto,
    		iv.BOARD as placa
    		from silver.INSURANCE_DAM_EVENT ide
    		left join silver.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join silver.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join silver.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    		left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		where 
    		ider.COVERAGE <> 0 
    		and ide.DATE_EVENT >= cast('2020-01-01' as date)
    		and ide.NUMBER_EVENT like ('%TE%')
    		and iv.BOARD is not null
    	) as tabela
    	group by tabela.numero_event
    ) as contagem on contagem.numero_event = ide.NUMBER_EVENT
    left join ( -- tabela para pegar o coverage do conjunto mais pr�ximo (caso n�o ache o coverage principal, usar esta tabela)
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from silver.INSURANCE_REG_SET_COVERAGE irsc
    	left join silver.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join silver.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join silver.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join silver.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join silver.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 7
    ) as coverage_2 on coverage_2.placa = iv.BOARD and coverage_2.conjunto < irsc.PARENT
    
    where
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like ('%TE%')
    and iv.BOARD is not null
    and coverage.coverage is not null
) as terceiro_seg
--------------
UNION ALL
--------------
select
*,
'Stcoop' as "cooperativa"
from(
    select distinct	
    ide.NUMBER_EVENT as numero_evento,
    cast(ide.DATE_EVENT as date) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    iv.BOARD as placa,
    ma.DESCRICAO as marca,
    iv.year_model as ano_modelo,    
    ide.AMMOUNT_THIRD as total_terce,
    'REPARAÇÃO A TERCEIROS' as beneficio,
    contagem.contagem,
    ide.AMMOUNT_THIRD / contagem.contagem as valor_terceiro,
    coalesce(coverage.coverage, FIRST_VALUE(coverage_2.coverage) over (partition by irsc.parent order by coverage_2.coverage)) as coverage
    from stcoop.INSURANCE_DAM_EVENT ide
    left join stcoop.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join stcoop.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join stcoop.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join stcoop.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join stcoop.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join stcoop.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
	left join stcoop.representante r on r.codigo = ide.id_unity
	left join stcoop.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join stcoop.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join stcoop.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join stcoop.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
	left join stcoop.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle    
    left join ( -- tabela para pegar o coverage do benef�cio de terceiros da placa
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from stcoop.INSURANCE_REG_SET_COVERAGE irsc
    	left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join stcoop.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join stcoop.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join stcoop.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join stcoop.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as coverage on coverage.placa = iv.BOARD and coverage.conjunto = irsc.PARENT
    left join ( --tabela para pegar a placa correta
    	select
    	irsc.ID, 
    	b.DESCRIPTION, 
    	iv.BOARD
    	from stcoop.INSURANCE_REG_SET_COVERAGE irsc
    	left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join stcoop.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join stcoop.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join stcoop.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join stcoop.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as terceiro on terceiro.BOARD = iv.board
    left join (-- tabela para fazer a contagem de quantas placas h� no evento
    	select distinct
    	tabela.numero_event,
    	count(tabela.numero_event) as contagem
    	from(
    		select distinct	
    		ide.NUMBER_EVENT as numero_event,
    		irsc.PARENT as conjunto,
    		iv.BOARD as placa
    		from stcoop.INSURANCE_DAM_EVENT ide
    		left join stcoop.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join stcoop.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join stcoop.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    		left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		where 
    		ider.COVERAGE <> 0 
    		and ide.DATE_EVENT >= cast('2020-01-01' as date)
    		and ide.NUMBER_EVENT like ('%TE%')
    		and iv.BOARD is not null
    	) as tabela
    	group by tabela.numero_event
    ) as contagem on contagem.numero_event = ide.NUMBER_EVENT
    left join ( -- tabela para pegar o coverage do conjunto mais pr�ximo (caso n�o ache o coverage principal, usar esta tabela)
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from stcoop.INSURANCE_REG_SET_COVERAGE irsc
    	left join stcoop.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join stcoop.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join stcoop.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join stcoop.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join stcoop.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as coverage_2 on coverage_2.placa = iv.BOARD and coverage_2.conjunto < irsc.PARENT
    where
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like ('%TE%')
    and iv.BOARD is not null
    and coverage.coverage is not null
) as terceiro_st
--------------
UNION ALL
--------------
select
*,
'Viavante' as "cooperativa"
from(
    select distinct	
    ide.NUMBER_EVENT as numero_evento,
    cast(ide.DATE_EVENT as date) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    iv.BOARD as placa,
    ma.DESCRICAO as marca,
    iv.year_model as ano_modelo,    
    ide.AMMOUNT_THIRD as total_terce,
    'REPARAÇÃO A TERCEIROS' as beneficio,
    contagem.contagem,
    ide.AMMOUNT_THIRD / contagem.contagem as valor_terceiro,
    coalesce(coverage.coverage, FIRST_VALUE(coverage_2.coverage) over (partition by irsc.parent order by coverage_2.coverage)) as coverage
    from viavante.INSURANCE_DAM_EVENT ide
    left join viavante.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join viavante.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join viavante.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join viavante.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join viavante.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join viavante.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
	left join viavante.representante r on r.codigo = ide.id_unity
	left join viavante.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join viavante.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join viavante.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join viavante.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
	left join viavante.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle    
    left join ( -- tabela para pegar o coverage do benefício de terceiros da placa
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from viavante.INSURANCE_REG_SET_COVERAGE irsc
    	left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join viavante.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join viavante.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join viavante.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join viavante.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as coverage on coverage.placa = iv.BOARD and coverage.conjunto = irsc.PARENT
    left join ( --tabela para pegar a placa correta
    	select
    	irsc.ID, 
    	b.DESCRIPTION, 
    	iv.BOARD
    	from viavante.INSURANCE_REG_SET_COVERAGE irsc
    	left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join viavante.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join viavante.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join viavante.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join viavante.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as terceiro on terceiro.BOARD = iv.board
    left join (-- tabela para fazer a contagem de quantas placas há no evento
    	select distinct
    	tabela.numero_event,
    	count(tabela.numero_event) as contagem
    	from(
    		select distinct	
    		ide.NUMBER_EVENT as numero_event,
    		irsc.PARENT as conjunto,
    		iv.BOARD as placa
    		from viavante.INSURANCE_DAM_EVENT ide
    		left join viavante.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join viavante.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join viavante.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    		left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		where 
    		ider.COVERAGE <> 0 
    		and ide.DATE_EVENT >= cast('2020-01-01' as date)
    		and ide.NUMBER_EVENT like ('%TE%')
    		and iv.BOARD is not null
    	) as tabela
    	group by tabela.numero_event
    ) as contagem on contagem.numero_event = ide.NUMBER_EVENT
    left join ( -- tabela para pegar o coverage do conjunto mais próximo (caso não ache o coverage principal, usar esta tabela)
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from viavante.INSURANCE_REG_SET_COVERAGE irsc
    	left join viavante.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join viavante.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join viavante.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join viavante.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join viavante.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as coverage_2 on coverage_2.placa = iv.BOARD and coverage_2.conjunto < irsc.PARENT
    where
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like ('%TE%')
    and iv.BOARD is not null
    and coverage.coverage is not null
) as terceiro_viavante    
--------------
UNION ALL
--------------
select
*,
'Tag' as "cooperativa"
from(
    select distinct	
    ide.NUMBER_EVENT as numero_evento,
    cast(ide.DATE_EVENT as date) as data_evento,
    year(ide.date_event) as ano_evento,
    ider.REGISTRATION as matricula,
    cat.NOME as associado,
    irsc.PARENT as conjunto,
    cata.fantasia as unidade,
    v.DESCRICAO as consultor,
    iv.BOARD as placa,
    ma.DESCRICAO as marca,
    iv.year_model as ano_modelo,    
    ide.AMMOUNT_THIRD as total_terce,
    'REPARAÇÃO A TERCEIROS' as beneficio,
    contagem.contagem,
    ide.AMMOUNT_THIRD / contagem.contagem as valor_terceiro,
    coalesce(coverage.coverage, FIRST_VALUE(coverage_2.coverage) over (partition by irsc.parent order by coverage_2.coverage)) as coverage
    from tag.INSURANCE_DAM_EVENT ide
    left join tag.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    left join tag.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    left join tag.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    left join tag.INSURANCE_REGISTRATION ir on ir.ID = ide.ID_REGISTRATION
    left join tag.CLIENTE cli on cli.CODIGO = ir.CUSTOMER_ID
    left join tag.CATALOGO cat on cat.CNPJ_CPF = cli.CNPJ_CPF
	left join tag.representante r on r.codigo = ide.id_unity
	left join tag.CATALOGO cata on cata.CNPJ_CPF = r.CNPJ_CPF
    left join tag.VENDEDOR v on v.CODIGO = irs.ID_CONSULTANT
    left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    left join tag.INSURANCE_REG_SET_COV_TRAILER irsct on irsct.PARENT = irsc.ID
    left join tag.INSURANCE_TRAILER it on it.ID = irsct.ID_TRAILER
	left join tag.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle    
    left join ( -- tabela para pegar o coverage do benefício de terceiros da placa
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from tag.INSURANCE_REG_SET_COVERAGE irsc
    	left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join tag.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join tag.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join tag.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join tag.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as coverage on coverage.placa = iv.BOARD and coverage.conjunto = irsc.PARENT
    left join ( --tabela para pegar a placa correta
    	select
    	irsc.ID, 
    	b.DESCRIPTION, 
    	iv.BOARD
    	from tag.INSURANCE_REG_SET_COVERAGE irsc
    	left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join tag.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join tag.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join tag.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join tag.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as terceiro on terceiro.BOARD = iv.board
    left join (-- tabela para fazer a contagem de quantas placas há no evento
    	select distinct
    	tabela.numero_event,
    	count(tabela.numero_event) as contagem
    	from(
    		select distinct	
    		ide.NUMBER_EVENT as numero_event,
    		irsc.PARENT as conjunto,
    		iv.BOARD as placa
    		from tag.INSURANCE_DAM_EVENT ide
    		left join tag.INSURANCE_DAM_EVENT_REFERENCE ider on ider.PARENT = ide.ID
    		left join tag.INSURANCE_REG_SET_COVERAGE irsc on irsc.ID = ider.COVERAGE
    		left join tag.INSURANCE_REG_SET irs on irs.ID = irsc.PARENT
    		left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    		where 
    		ider.COVERAGE <> 0 
    		and ide.DATE_EVENT >= cast('2020-01-01' as date)
    		and ide.NUMBER_EVENT like ('%TE%')
    		and iv.BOARD is not null
    	) as tabela
    	group by tabela.numero_event
    ) as contagem on contagem.numero_event = ide.NUMBER_EVENT
    left join ( -- tabela para pegar o coverage do conjunto mais próximo (caso não ache o coverage principal, usar esta tabela)
    	select
    	irsc.PARENT as conjunto, 
    	iv.BOARD as placa,
    	irsc.ID as coverage
    	from tag.INSURANCE_REG_SET_COVERAGE irsc
    	left join tag.INSURANCE_VEHICLE iv on iv.ID = irsc.ID_VEHICLE
    	left join tag.PRICE_LIST_BENEFITS plb on plb.ID = irsc.ID_PRICE_LIST
    	left join tag.TYPE_CATEGORY tc on tc.ID = plb.ID_TYPE_CATEGORY
    	left join tag.CATEGORY c on c.ID = tc.ID_CATEGORY
    	left join tag.BENEFITS b on b.ID = c.ID_BENEFITS
    	where b.ID = 29
    ) as coverage_2 on coverage_2.placa = iv.BOARD and coverage_2.conjunto < irsc.PARENT
    where
    ider.COVERAGE <> 0
    and ide.NUMBER_EVENT like ('%TE%')
    and iv.BOARD is not null
    and coverage.coverage is not null
    and cast(ide.DATE_EVENT as date) >= DATE ('2025-08-01')
) as terceiro_tag
    
