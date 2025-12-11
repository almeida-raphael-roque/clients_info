select distinct 
   
    ide.number_event as "numero_evento",
	-- DATA DO EVENTO DANOSO
	cast(ide.date_event as date) as "data_evento",
	-- ID MONTA
	idr.size_mount as "id_monta",
	-- MONTA
	case
		idr.size_mount
		when 0 then 'Sem Monta' -- COLUNA COM DESCRITIVO DE CADA TIPO DE MONTA
		when 1 then 'Sem Monta'
		when 2 then 'Pequena Monta'
		when 3 then 'Média Monta'
		when 4 then 'Grande Monta' else 'Sem Monta'
	end as "monta",
	cat.nome as "cooperado",
	case
		ide.id_kind_event
		when 6 then 'Furto'
		when 7 then 'Roubo'
		when 8 then 'Suspeita de Furto/Roubo'
		when 9 then 'Apropiacao Indebita'
		when 13 then 'Tentativa de Furto/Roubo' else 'SEM TIPO'
	end as "tipo_de_sinistro",
	-- CONJUNTO 
	coalesce(
		cast(idr.regset as varchar),
		cast(ide.id_set as varchar)
	) as "conjunto",
	-- MATRICULA
	ide.id_registration as "matricula",
	-- ABRIU CASCO
	coalesce(ide.open_hull, 'N') as "casco",
	-- ABRIU TERCEIRO 
	coalesce(ide.open_third, 'N') as "terceiro",
	-- ID_TIPO_EVENTO
	ide.id_kind_event as "id_tipo_evento",
	-- VALOR CASC0
	coalesce(ide.ammount_hull, 0) as "valor_casco",
	-- VALOR TERCEIRO
	coalesce(ide.ammount_third, 0) as "valor_terceiro",
	-- VALOR TOTAL
	coalesce(ide.ammount, 0) as "valor_total",
	-- UNIDADE 
	cata.fantasia as "unidade",
	-- ID STATUS
	case
		when ide.id_status = 0 then ide.id_status_third else ide.id_status
	end as "id_status_evento",
	-- STATUS DO EVENTO
	coalesce(iss.description, isss.description) as "status_evento",
    -- CIDADE
	coalesce(cid.nome, 'Sem Cidade') as "cidade_evento",
	-- UF 
	coalesce(cid.uf, 'Sem UF') as "uf_evento",
	-- NOME DO MOTORISTA
	coalesce(ide.driver_name, 'SEM MOTORISTA') as "nome_motorista",
	'segtruck' as "cooperativa"
	
	
from silver.insurance_dam_event ide
	left outer join silver.insurance_dam_event_reference idr on idr.parent = ide.id
	left outer join silver.representante r on r.codigo = ide.id_unity
	left outer join silver.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
	left outer join silver.cliente clie on clie.codigo = ide.customer_id
	left outer join silver.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
	left outer join silver.cidade cid on cid.codigo = ide.code_city_local_event 
	left outer join silver.insurance_status iss on iss.id = ide.id_status
	left outer join silver.insurance_status isss on isss.id = ide.id_status_third

where ide.id_kind_event in (6,7,8,9,13)

--------------------------------------------------------------------------------------
union all
--------------------------------------------------------------------------------------

select distinct 
   
    ide.number_event as "numero_evento",
	-- DATA DO EVENTO DANOSO
	cast(ide.date_event as date) as "data_evento",
	-- ID MONTA
	idr.size_mount as "id_monta",
	-- MONTA
	case
		idr.size_mount
		when 0 then 'Sem Monta' -- COLUNA COM DESCRITIVO DE CADA TIPO DE MONTA
		when 1 then 'Sem Monta'
		when 2 then 'Pequena Monta'
		when 3 then 'Média Monta'
		when 4 then 'Grande Monta' else 'Sem Monta'
	end as "monta",
	cat.nome as "cooperado",
	case
		ide.id_kind_event
		when 6 then 'Furto'
		when 7 then 'Roubo'
		when 8 then 'Suspeita de Furto/Roubo'
		when 9 then 'Apropiacao Indebita'
		when 13 then 'Tentativa de Furto/Roubo' else 'SEM TIPO'
	end as "tipo_de_sinistro",
	-- CONJUNTO 
	coalesce(
		cast(idr.regset as varchar),
		cast(ide.id_set as varchar)
	) as "conjunto",
	-- MATRICULA
	ide.id_registration as "matricula",
	-- ABRIU CASCO
	coalesce(ide.open_hull, 'N') as "casco",
	-- ABRIU TERCEIRO 
	coalesce(ide.open_third, 'N') as "terceiro",
	-- ID_TIPO_EVENTO
	ide.id_kind_event as "id_tipo_evento",
	-- VALOR CASC0
	coalesce(ide.ammount_hull, 0) as "valor_casco",
	-- VALOR TERCEIRO
	coalesce(ide.ammount_third, 0) as "valor_terceiro",
	-- VALOR TOTAL
	coalesce(ide.ammount, 0) as "valor_total",
	-- UNIDADE 
	cata.fantasia as "unidade",
	-- ID STATUS
	case
		when ide.id_status = 0 then ide.id_status_third else ide.id_status
	end as "id_status_evento",
	-- STATUS DO EVENTO
	coalesce(iss.description, isss.description) as "status_evento",
    -- CIDADE
	coalesce(cid.nome, 'Sem Cidade') as "cidade_evento",
	-- UF 
	coalesce(cid.uf, 'Sem UF') as "uf_evento",
	-- NOME DO MOTORISTA
	coalesce(ide.driver_name, 'SEM MOTORISTA') as "nome_motorista",
	'stcoop' as "cooperativa"
	
	
from stcoop.insurance_dam_event ide
	left outer join stcoop.insurance_dam_event_reference idr on idr.parent = ide.id
	left outer join stcoop.representante r on r.codigo = ide.id_unity
	left outer join stcoop.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
	left outer join stcoop.cliente clie on clie.codigo = ide.customer_id
	left outer join stcoop.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
	left outer join stcoop.cidade cid on cid.codigo = ide.code_city_local_event 
	left outer join stcoop.insurance_status iss on iss.id = ide.id_status
	left outer join stcoop.insurance_status isss on isss.id = ide.id_status_third

where ide.id_kind_event in (6,7,8,9,13)

--------------------------------------------------------------------------------------
union all
--------------------------------------------------------------------------------------

select distinct 
   
    ide.number_event as "numero_evento",
	-- DATA DO EVENTO DANOSO
	cast(ide.date_event as date) as "data_evento",
	-- ID MONTA
	idr.size_mount as "id_monta",
	-- MONTA
	case
		idr.size_mount
		when 0 then 'Sem Monta' -- COLUNA COM DESCRITIVO DE CADA TIPO DE MONTA
		when 1 then 'Sem Monta'
		when 2 then 'Pequena Monta'
		when 3 then 'Média Monta'
		when 4 then 'Grande Monta' else 'Sem Monta'
	end as "monta",
	cat.nome as "cooperado",
	case
		ide.id_kind_event
		when 6 then 'Furto'
		when 7 then 'Roubo'
		when 8 then 'Suspeita de Furto/Roubo'
		when 9 then 'Apropiacao Indebita'
		when 13 then 'Tentativa de Furto/Roubo' else 'SEM TIPO'
	end as "tipo_de_sinistro",
	-- CONJUNTO 
	coalesce(
		cast(idr.regset as varchar),
		cast(ide.id_set as varchar)
	) as "conjunto",
	-- MATRICULA
	ide.id_registration as "matricula",
	-- ABRIU CASCO
	coalesce(ide.open_hull, 'N') as "casco",
	-- ABRIU TERCEIRO 
	coalesce(ide.open_third, 'N') as "terceiro",
	-- ID_TIPO_EVENTO
	ide.id_kind_event as "id_tipo_evento",
	-- VALOR CASC0
	coalesce(ide.ammount_hull, 0) as "valor_casco",
	-- VALOR TERCEIRO
	coalesce(ide.ammount_third, 0) as "valor_terceiro",
	-- VALOR TOTAL
	coalesce(ide.ammount, 0) as "valor_total",
	-- UNIDADE 
	cata.fantasia as "unidade",
	-- ID STATUS
	case
		when ide.id_status = 0 then ide.id_status_third else ide.id_status
	end as "id_status_evento",
	-- STATUS DO EVENTO
	coalesce(iss.description, isss.description) as "status_evento",
    -- CIDADE
	coalesce(cid.nome, 'Sem Cidade') as "cidade_evento",
	-- UF 
	coalesce(cid.uf, 'Sem UF') as "uf_evento",
	-- NOME DO MOTORISTA
	coalesce(ide.driver_name, 'SEM MOTORISTA') as "nome_motorista",
	'viavante' as "cooperativa"
	
	
from viavante.insurance_dam_event ide
	left outer join viavante.insurance_dam_event_reference idr on idr.parent = ide.id
	left outer join viavante.representante r on r.codigo = ide.id_unity
	left outer join viavante.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
	left outer join viavante.cliente clie on clie.codigo = ide.customer_id
	left outer join viavante.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
	left outer join viavante.cidade cid on cid.codigo = ide.code_city_local_event 
	left outer join viavante.insurance_status iss on iss.id = ide.id_status
	left outer join viavante.insurance_status isss on isss.id = ide.id_status_third

where ide.id_kind_event in (6,7,8,9,13)

--------------------------------------------------------------------------------------
union all
--------------------------------------------------------------------------------------

select distinct 
   
    ide.number_event as "numero_evento",
	-- DATA DO EVENTO DANOSO
	cast(ide.date_event as date) as "data_evento",
	-- ID MONTA
	idr.size_mount as "id_monta",
	-- MONTA
	case
		idr.size_mount
		when 0 then 'Sem Monta' -- COLUNA COM DESCRITIVO DE CADA TIPO DE MONTA
		when 1 then 'Sem Monta'
		when 2 then 'Pequena Monta'
		when 3 then 'Média Monta'
		when 4 then 'Grande Monta' else 'Sem Monta'
	end as "monta",
	cat.nome as "cooperado",
	case
		ide.id_kind_event
		when 6 then 'Furto'
		when 7 then 'Roubo'
		when 8 then 'Suspeita de Furto/Roubo'
		when 9 then 'Apropiacao Indebita'
		when 13 then 'Tentativa de Furto/Roubo' else 'SEM TIPO'
	end as "tipo_de_sinistro",
	-- CONJUNTO 
	coalesce(
		cast(idr.regset as varchar),
		cast(ide.id_set as varchar)
	) as "conjunto",
	-- MATRICULA
	ide.id_registration as "matricula",
	-- ABRIU CASCO
	coalesce(ide.open_hull, 'N') as "casco",
	-- ABRIU TERCEIRO 
	coalesce(ide.open_third, 'N') as "terceiro",
	-- ID_TIPO_EVENTO
	ide.id_kind_event as "id_tipo_evento",
	-- VALOR CASC0
	coalesce(ide.ammount_hull, 0) as "valor_casco",
	-- VALOR TERCEIRO
	coalesce(ide.ammount_third, 0) as "valor_terceiro",
	-- VALOR TOTAL
	coalesce(ide.ammount, 0) as "valor_total",
	-- UNIDADE 
	cata.fantasia as "unidade",
	-- ID STATUS
	case
		when ide.id_status = 0 then ide.id_status_third else ide.id_status
	end as "id_status_evento",
	-- STATUS DO EVENTO
	coalesce(iss.description, isss.description) as "status_evento",
    -- CIDADE
	coalesce(cid.nome, 'Sem Cidade') as "cidade_evento",
	-- UF 
	coalesce(cid.uf, 'Sem UF') as "uf_evento",
	-- NOME DO MOTORISTA
	coalesce(ide.driver_name, 'SEM MOTORISTA') as "nome_motorista",
	'tag' as "cooperativa"
	
from tag.insurance_dam_event ide
	left outer join tag.insurance_dam_event_reference idr on idr.parent = ide.id
	left outer join tag.representante r on r.codigo = ide.id_unity
	left outer join tag.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
	left outer join tag.cliente clie on clie.codigo = ide.customer_id
	left outer join tag.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
	left outer join tag.cidade cid on cid.codigo = ide.code_city_local_event 
	left outer join tag.insurance_status iss on iss.id = ide.id_status
	left outer join tag.insurance_status isss on isss.id = ide.id_status_third

where ide.id_kind_event in (6,7,8,9,13)
and cast(ide.date_event as date) > date('2025-08-01')