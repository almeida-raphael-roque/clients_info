--- base "sinistros" do google bigquery


-- CONSULTA EVENTOS DANOSOS DW -- SEGTRUCK
select distinct 
    -- NUMERO DO EVENTO (PK)
    ide.number_event as "numero_evento",
	-- DATA DO EVENTO DANOSO
	cast(ide.date_event as date) as "data_evento",
	-- HORA DO EVENTO
	coalesce(ide.hour_event, '00:00') as "hora_evento",
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
	-- COOPERADO
	cat.nome as "cooperado",
	-- PLACA ENVOLVIDA NO EVENTO
	coalesce(idr.board, '0') as "placa",
	-- MARCA
	coalesce(
		placa_cavalo.marca,
		placa_reboque.modelo,
		'SEM MARCA'
	) as "marca",
	-- MODELO
	coalesce(
		placa_cavalo.modelo,
		placa_reboque.modelo,
		'SEM MODELO'
	) as "modelo",
	-- TIPO DO VEICULO
	coalesce(placa_cavalo.tipo_veiculo, 'Não Aplicavél') as "tipo_veiculo",
	-- TIPO DE CARROCERIA
	coalesce(
		placa_cavalo.tipo_carroceria,
		placa_reboque.tipo_carroceria,
		'Não Aplicavél'
	) as "tipo_carroceria",
	
	beneficio.valor_bem as "valor_bem",
	-- TIPO DE EVENTO
	case
		ide.id_kind_event
		when 1 then 'Colisao'
		when 2 then 'Tombamento'
		when 3 then 'Capotamento'
		when 4 then 'Queda'
		when 5 then 'Incendio/Explosao'
		when 6 then 'Furto'
		when 7 then 'Roubo'
		when 8 then 'Suspeita de Furto/Roubo'
		when 9 then 'Apropiacao Indebita'
		when 10 then 'Saida De Pista'
		when 11 then 'Atolamento'
		when 12 then 'Pane'
		when 13 then 'Tentativa de Furto/Roubo' else 'SEM TIPO'
	end as "tipo_de_sinistro",
	-- CONJUNTO 
	coalesce(
		cast(idr.regset as varchar),
		cast(ide.id_set as varchar)
	) as "conjunto",
	-- MATRICULA
	ide.id_registration as "matricula",
	-- BENEFICIO
	coalesce(beneficio.beneficio, 'SEM BENEFICIO') as "beneficio",
	-- CATEGORIA
	coalesce(beneficio.categoria, 'SEM CATEGORIA') as "categoria",
	-- TIPO CATEGORIA
	coalesce(beneficio.tipo_categoria, 'SEM TIPO DE CATEGORIA') as "tipo_categoria",
	-- TABELA
	coalesce(beneficio.tabela, 'SEM TABELA') as "tabela",
	-- ABRIU CASCO
	coalesce(ide.open_hull, 'N') as "casco",
	-- ABRIU TERCEIRO 
	coalesce(ide.open_third, 'N') as "terceiro",
	-- ID_TIPO_EVENTO
	ide.id_kind_event as "id_tipo_evento",
	-- VALOR DA COTA
	idr.quota_value as "valor_da_cota",
    -- VALOR DO ORCAMENTO CASCO
	coalesce(vlorc_cas.valor_orcamento_casco, 0) as "valor_orcamento_casco",
	-- VALOR CASC0
	coalesce(ide.ammount_hull, 0) as "valor_casco",
	-- VALOR ORÇAMENTO TERCEIRO
	coalesce(vlorc_ter.valor_terceiro_orc, 0) as "valor_orcamento_terceiro",
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
	left outer join silver.insurance_status iss on iss.id = ide.id_status
	left outer join silver.insurance_status isss on isss.id = ide.id_status_third
	left outer join silver.cliente clie on clie.codigo = ide.customer_id
	left outer join silver.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
	left outer join silver.insurance_dam_event_budget ideb on ideb.parent = idr.id
	left outer join silver.cidade cid on cid.codigo = ide.code_city_local_event 
    left outer join silver.web_user usu on usu.id = ide.id_user_analyt_hull
        and usu.id = ide.id_user_analyt_third
	-- subquery para coletar a marca e modelo dos cavalos
	left outer join (
		select distinct iv.board as "placa",
			case
				when un.marca = null then mar.descricao else un.marca
			end as "marca",
			case
				when un.modelo = null then mv.descricao else un.modelo
			end as "modelo",
			tv.descricao as tipo_veiculo,
			tca.descricao as "tipo_carroceria"
		from silver.insurance_vehicle iv
			left outer join silver.marca_veiculo mar on mar.codigo = iv.code_brand_vehicle
			left outer join silver.modelo_veiculo mv on mv.codigo = iv.code_model_vehicle
			left outer join silver.tipo_carroceria tca on tca.codigo = iv.body_type
			left outer join silver.tipo_veiculo tv on tv.codigo = iv.code_type_vehicle
			left outer join silver.unusfipe un on un.cod_fipe = iv.code_fipe
		where iv.board <> 'SEM-PLACA'
	) as placa_cavalo on placa_cavalo.placa = idr.board -- subquery para coletar a marca e modelo dos reboques
	left outer join (
		select distinct it.board as placa,
			it.brand as marca,
			it.brand as modelo,
			tca.descricao as tipo_carroceria
		from insurance_trailer it
			left join tipo_carroceria tca on tca.codigo = it.body_type
		where it.board <> 'SEM-PLACA'
	) as placa_reboque on placa_reboque.placa = idr.board -- subquery para somar os valores de valor_orcamento_terceiro
	left outer join (
		select ideb.parent as id_terceiro_orc,
			sum(ideb.amount) as valor_terceiro_orc
		from silver.insurance_dam_event_budget ideb
			left outer join silver.insurance_dam_event_reference idr on idr.id = ideb.parent
			left outer join silver.insurance_dam_event ide on ide.id = idr.parent
		where ide.open_third = 'S'
		group by ideb.parent
	) as vlorc_ter on vlorc_ter.id_terceiro_orc = idr.id -- subquery para coletar os valores de orçamento de casco 
	left outer join (
		select parent,
			case
				when code_material = 466 then sum(amount) else 0
			end as valor_orcamento_casco
		from insurance_dam_event_budget
		group by parent,
			code_material
	) as vlorc_cas on vlorc_cas.parent = idr.id -- subquery para coletar os beneficios que foram acionados
	left outer join (
	    select distinct iab.coverage as coverage,
    	coalesce(it.board, iv.board) as placa,
    	upper(iab.benefit) as beneficio,
    	c.description as categoria,
    	ty.description as tipo_categoria,
    	plb.description as tabela,
        case b.id
        	when 2 then irsc.vehicle_value
        	when 4 then irsct.value_property
        	when 3 then irsc.complement_value
        	else 0
        end as "valor_bem"
    
    from "silver"."insurance_assistance_benefits" iab
    	left outer join silver.insurance_reg_set_coverage irsc on irsc.id = iab.coverage
    	left outer join silver.insurance_vehicle iv on iv.id = irsc.id_vehicle
    	left outer join silver.insurance_reg_set_cov_trailer irsct on irsct.parent = irsc.id
    	left outer join silver.insurance_trailer it on it.ID = irsct.id_trailer
        left outer join silver.price_list_benefits plb on plb.id = irsc.id_price_list
        left outer join silver.type_category ty on ty.id = plb.id_type_category
        left outer join silver.category c on c.id = ty.id_category
        left outer join silver.benefits b on b.id = c.id_benefits
        
    where iab.benefit in (
    		'Reparação ou Reposição do Veículo',
    		'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO',
    		'Reparação ou Reposição do (semi)reboque',
    		'REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE',
    		'REPARAÇÃO A TERCEIROS',
    		'Reparação a Terceiros'
    	)
	) as beneficio on beneficio.placa = idr.board --where ide.number_event = '202302130545/CO.RR.TE'
	--and idr.id = 17953
where ide.number_event = '202301200817/TO.RR.00'
----------------------------------------------------------------------------------------------------------------------------------------
union all 
---------------------------------------------------------------------------------------------------------------------------------------------
select distinct ide.number_event as "numero_evento",
	-- NUMERO DO EVENTO (PK)
	cast(ide.date_event as date) as "data_evento",
	-- DATA DO EVENTO DANOSO
	coalesce(ide.hour_event, '00:00') as "hora_evento",
	idr.size_mount as "id_monta",
	-- ID MONTA 
	case
		idr.size_mount
		when 0 then 'Sem Monta' -- COLUNA COM DESCRITIVO DE CADA TIPO DE MONTA
		when 1 then 'Sem Monta'
		when 2 then 'Pequena Monta'
		when 3 then 'Média Monta'
		when 4 then 'Grande Monta' else 'Sem Monta'
	end as "monta",
	-- MONTA
	cat.nome as "cooperado",
	-- COOPERADO
	coalesce(idr.board, '0') as "placa",
	-- PLACA ENVOLVIDA NO EVENTO
	coalesce(
		placa_cavalo.marca,
		placa_reboque.modelo,
		'SEM MARCA'
	) as "marca",
	-- MARCA DO VEÍCULO ENVOLVIDO
	coalesce(
		placa_cavalo.modelo,
		placa_reboque.modelo,
		'SEM MODELO'
	) as "modelo",
	-- MODELO DO VEÍCULO ENVOLVIDO
	coalesce(placa_cavalo.tipo_veiculo, 'Não Aplicavél') as "tipo_veiculo",
	coalesce(
		placa_cavalo.tipo_carroceria,
		placa_reboque.tipo_carroceria,
		'Não Aplicavél'
	) as "tipo_carroceria",
	-- VALOR DO BEM
	coalesce(beneficio.valor_bem,0) as "valor_bem",
	-- TIPO DE CARROCERIA
	case
		ide.id_kind_event
		when 1 then 'Colisao'
		when 2 then 'Tombamento'
		when 3 then 'Capotamento'
		when 4 then 'Queda'
		when 5 then 'Incendio/Explosao'
		when 6 then 'Furto'
		when 7 then 'Roubo'
		when 8 then 'Suspeita de Furto/Roubo'
		when 9 then 'Apropiacao Indebita'
		when 10 then 'Saida De Pista'
		when 11 then 'Atolamento'
		when 12 then 'Pane'
		when 13 then 'Tentativa de Furto/Roubo' else 'SEM TIPO'
	end as "tipo_de_sinistro",
	coalesce(
		cast(idr.regset as varchar),
		cast(ide.id_set as varchar)
	) as "conjunto",
	-- CONJUNTO
	ide.id_registration as "matricula",
	-- MATRICULA
	coalesce(beneficio.beneficio, 'SEM BENEFICIO') as "beneficio",
	-- CATEGORIA
	coalesce(beneficio.categoria, 'SEM CATEGORIA') as "categoria",
	-- TIPO CATEGORIA
	coalesce(beneficio.tipo_categoria, 'SEM TIPO DE CATEGORIA') as "tipo_categoria",
	-- TABELA
	coalesce(beneficio.tabela, 'SEM TABELA') as "tabela",
	-- BENEFICIO
	coalesce(ide.open_hull, 'N') as "casco",
	-- ABRIU CASCO
	coalesce(ide.open_third, 'N') as "terceiro",
	-- ABRIU TERCEIRO 
	ide.id_kind_event as "id_tipo_evento",
	-- ID_TIPO_EVENTO
	idr.quota_value as "valor_da_cota",
	-- VALOR DA COTA PARTICIPACAO 
	coalesce(vlorc_cas.valor_orcamento_casco, 0) as "valor_orcamento_casco",
	-- VALOR DO ORÇAMENTO CASCO 
	coalesce(ide.ammount_hull, 0) as "valor_casco",
	-- VALOR CASCO
	coalesce(vlorc_ter.valor_terceiro_orc, 0) as "valor_orcamento_terceiro",
	-- VALOR DO ORÇAMENTO TERCEIRO
	coalesce(ide.ammount_third, 0) as "valor_terceiro",
	-- VALOR TERCEIRO 
	coalesce(ide.ammount, 0) as "valor_total",
	-- VALOR TOTAL
	cata.fantasia as "unidade",
	-- UNIDADE 
	case
		when ide.id_status = 0 then ide.id_status_third else ide.id_status
	end as "id_status_evento",
	-- ID STATUS  
	coalesce(iss.description, isss.description) as "status_evento",
	-- STATUS DO EVENTO
	coalesce(cid.nome, 'Sem Cidade') as "cidade_evento",
	-- CIDADE DO EVENTO
	coalesce(cid.uf, 'Sem UF') as "uf_evento",
	-- UF DO EVENTO
	coalesce(ide.driver_name, 'SEM MOTORISTA') as "nome_motorista",
	-- NOME DO MOTORISTA
    'stcoop' as "cooperativa"
    -- COOPERATIVA
    
    
from stcoop.insurance_dam_event ide
	left outer join stcoop.insurance_dam_event_reference idr on idr.parent = ide.id
	left outer join stcoop.representante r on r.codigo = ide.id_unity
	left outer join stcoop.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
	left outer join stcoop.insurance_status iss on iss.id = ide.id_status
	left outer join stcoop.insurance_status isss on isss.id = ide.id_status_third
	left outer join stcoop.cliente clie on clie.codigo = ide.customer_id
	left outer join stcoop.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
	left outer join stcoop.insurance_dam_event_budget ideb on ideb.parent = idr.id
	left outer join stcoop.cidade cid on cid.codigo = ide.code_city_local_event 
	
	-- subquery para coletar a marca e modelo dos cavalos
	left outer join (
		select distinct iv.board as "placa",
		 mar.descricao as "marca",
		 mv.descricao  as "modelo",
		 tv.descricao  as tipo_veiculo,
		 tca.descricao as "tipo_carroceria"
		from stcoop.insurance_vehicle iv
			left outer join stcoop.marca_veiculo mar on mar.codigo = iv.code_brand_vehicle
			left outer join stcoop.modelo_veiculo mv on mv.codigo = iv.code_model_vehicle
			left outer join stcoop.tipo_carroceria tca on tca.codigo = iv.body_type
			left outer join stcoop.tipo_veiculo tv on tv.codigo = iv.code_type_vehicle
			--left outer join unusfipe un on un.cod_fipe = iv.code_fipe
		where iv.board <> 'SEM-PLACA'
	) as placa_cavalo on placa_cavalo.placa = idr.board 
	
	-- subquery para coletar a marca e modelo dos reboques

	left outer join (
		select distinct it.board as placa,
			it.brand as marca,
			-- USAR A MESMA COLUNA PARA MARCA E MODELO, POIS NÃO EXISTE SEPARAÇÃO NO CADASTRO DOS SEMIRREBOQUES
			it.brand as modelo,
			tca.descricao as tipo_carroceria
		from stcoop.insurance_trailer it
			left join stcoop.tipo_carroceria tca on tca.codigo = it.body_type
		where it.board <> 'SEM-PLACA'
	) as placa_reboque on placa_reboque.placa = idr.board 
	
	-- subquery para somar os valores de valor_orcamento_terceiro
	
	left outer join (
		select ideb.parent as id_terceiro_orc,
			sum(ideb.amount) as valor_terceiro_orc
    	    
		from stcoop.insurance_dam_event_budget ideb
			left outer join stcoop.insurance_dam_event_reference idr on idr.id = ideb.parent
			left outer join stcoop.insurance_dam_event ide on ide.id = idr.parent
	
		where ide.open_third = 'S'
		group by ideb.parent
	) as vlorc_ter on vlorc_ter.id_terceiro_orc = idr.id 
	
	-- subquery para coletar os valores de orçamento de casco 
	
	left outer join (
		select parent,
			case
				when code_material = 466 then sum(amount) else 0
			end as valor_orcamento_casco
		from stcoop.insurance_dam_event_budget
		group by parent,
			code_material
	) as vlorc_cas on vlorc_cas.parent = idr.id -- subquery para coletar os beneficios que foram acionados
	left outer join (
		select distinct iab.coverage as coverage,
			coalesce(it.board, iv.board) as placa,
		   case b.ID
    		when 24 then cast((irsc.VEHICLE_VALUE * (plb.PAID_FIPE_PERC / 100)) as decimal(10,2))
    		when 26 then cast((irsct.REPORTED_VEHICLE_VALUE * (plb.PAID_FIPE_PERC / 100)) as decimal(10,2))
    		when 25 then cast((irsc.COMPLEMENT_VALUE * (plb.PAID_FIPE_PERC / 100)) as decimal(10,2))
    		else 0
            end as valor_bem,
			upper(iab.benefit) as beneficio,
		    c.description as categoria,
        	ty.description as tipo_categoria,
    	    plb.description as tabela
    
        
		from stcoop.insurance_assistance_benefits iab
			left outer join stcoop.insurance_reg_set_coverage irsc on irsc.id = iab.coverage
			left outer join stcoop.insurance_vehicle iv on iv.id = irsc.id_vehicle
			left outer join stcoop.insurance_reg_set_cov_trailer irsct on irsct.parent = irsc.id
			left outer join stcoop.insurance_trailer it on it.ID = irsct.id_trailer
            left outer join stcoop.price_list_benefits plb on plb.id = irsc.id_price_list
            left outer join stcoop.type_category ty on ty.id = plb.id_type_category
            left outer join stcoop.category c on c.id = ty.id_category
            left outer join stcoop.benefits b on b.id = c.id_benefits

		where iab.benefit in (
				'Reparação ou Reposição do Veículo',
				'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO',
				'Reparação ou Reposição do (semi)reboque',
				'REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE',
				'REPARAÇÃO A TERCEIROS',
				'Reparação a Terceiros'
			)
	) as beneficio on beneficio.placa = idr.board --where ide.number_event = '202302130545/CO.RR.TE'
	--and idr.id = 17953