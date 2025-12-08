/*despesas_vidros_analitico

atualização feita por mim (wesley) no dia 06/02/24 para colocar a matricula na tabela*/


select *,
'segtruck' as "cooperativa"
from (
    select distinct
    ia.number_event as "numero_evento",
    CAST(ia.date_event as DATE) as "data_evento",
    year(ia.date_event) as "ano_evento",
    iab.parent as "atendimento",
    iab.benefit as "beneficio",
    iap.id as id_despesa,
    ROUND(cast(iap.assistance_total as DOUBLE), 2) as "valor_final_vidr",
    iar.regset as "conjunto",
    cata.fantasia as "unidade",
    v.descricao as "consultor",
    iv.board as "placa",
	ma.descricao as "marca",
	iv.year_model as "ano_modelo",    
    cat.nome as "associado",
    irsc.id as "coverage",
    ir.id as "matricula"
    
    from silver.insurance_assistance_benefits iab
    left join silver.insurance_assistance_providers iap on iap.coverage_id = iab.coverage and iab.parent = iap.parent
    left join silver.insurance_assistance ia on ia.id = iab.parent
    left join silver.insurance_assistance_reference iar on iar.parent = ia.id
    left join silver.insurance_reg_set_coverage irsc on irsc.id = iar.coverage
    left join silver.insurance_reg_set irs on irs.id = irsc.parent
	left join silver.representante r on r.codigo = ia.unity_id
	left join silver.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf    
    left join silver.vendedor v on v.codigo = irs.id_consultant
    left join silver.insurance_vehicle iv on iv.id = irsc.id_vehicle
    left join silver.insurance_registration ir on ir.id = ia.registration_id
    left join silver.cliente cli on cli.codigo = ir.customer_id
    left join silver.catalogo cat on cat.cnpj_cpf = cli.cnpj_cpf
	left join silver.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    where
    iap.cancel <> 'S'
    and CAST(ia.date_event as DATE) >= date('2020-01-01')
    and iab.benefit in ('VIDROS')
    and iar.coverage <> 0
) as seg
------------------
UNION ALL
------------------
select
*,
'stcoop' as cooperativa
from (
    select distinct
    ia.number_event as "numero_evento",
    CAST(ia.date_event as DATE) as "data_evento",
    year(ia.date_event) as "ano_evento",
    iab.parent as "atendimento",
    iab.benefit as "beneficio",
    iap.id as id_despesa,
    ROUND(cast(iap.assistance_total as DOUBLE), 2) as "valor_final_vidr",
    iar.regset as "conjunto",
    cata.fantasia as "unidade",
    v.descricao as "consultor",
    iv.board as "placa",
	ma.descricao as "marca",
	iv.year_model as "ano_modelo",    
    cat.nome as "associado",
    irsc.id as "coverage",
    ir.id as "matricula"
    
    from stcoop.insurance_assistance_benefits iab
    left join stcoop.insurance_assistance_providers iap on iap.coverage_id = iab.coverage and iab.parent = iap.parent
    left join stcoop.insurance_assistance ia on ia.id = iab.parent
    left join stcoop.insurance_assistance_reference iar on iar.parent = ia.id
    left join stcoop.insurance_reg_set_coverage irsc on irsc.id = iar.coverage
    left join stcoop.insurance_reg_set irs on irs.id = irsc.parent
	left join stcoop.representante r on r.codigo = ia.unity_id
	left join stcoop.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf     
    left join stcoop.vendedor v on v.codigo = irs.id_consultant
    left join stcoop.insurance_vehicle iv on iv.id = irsc.id_vehicle
    left join stcoop.insurance_registration ir on ir.id = ia.registration_id
    left join stcoop.cliente cli on cli.codigo = ir.customer_id
    left join stcoop.catalogo cat on cat.cnpj_cpf = cli.cnpj_cpf
	left join stcoop.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    where
    iap.cancel <> 'S'
    and CAST(ia.date_event as DATE) >= date('2020-01-01')
    and iab.benefit in ('VIDROS')
    and iar.coverage <> 0
) as st
------------------
UNION ALL
------------------
select
*,
'viavante' as cooperativa
from (
    select distinct
    ia.number_event as "numero_evento",
    CAST(ia.date_event as DATE) as "data_evento",
    year(ia.date_event) as "ano_evento",
    iab.parent as "atendimento",
    iab.benefit as "beneficio",
    iap.id as id_despesa,
    ROUND(cast(iap.assistance_total as DOUBLE), 2) as "valor_final_vidr",
    iar.regset as "conjunto",
    cata.fantasia as "unidade",
    v.descricao as "consultor",
    iv.board as "placa",
	ma.descricao as "marca",
	iv.year_model as "ano_modelo",    
    cat.nome as "associado",
    irsc.id as "coverage",
    ir.id as "matricula"
    
    from viavante.insurance_assistance_benefits iab
    left join viavante.insurance_assistance_providers iap on iap.coverage_id = iab.coverage and iab.parent = iap.parent
    left join viavante.insurance_assistance ia on ia.id = iab.parent
    left join viavante.insurance_assistance_reference iar on iar.parent = ia.id
    left join viavante.insurance_reg_set_coverage irsc on irsc.id = iar.coverage
    left join viavante.insurance_reg_set irs on irs.id = irsc.parent
	left join viavante.representante r on r.codigo = ia.unity_id
	left join viavante.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf     
    left join viavante.vendedor v on v.codigo = irs.id_consultant
    left join viavante.insurance_vehicle iv on iv.id = irsc.id_vehicle
    left join viavante.insurance_registration ir on ir.id = ia.registration_id
    left join viavante.cliente cli on cli.codigo = ir.customer_id
    left join viavante.catalogo cat on cat.cnpj_cpf = cli.cnpj_cpf
	left join viavante.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    where
    iap.cancel <> 'S'
    and CAST(ia.date_event as DATE) >= date('2020-01-01')
    and iab.benefit in ('VIDROS')
    and iar.coverage <> 0
) as viavante

------------------
UNION ALL
------------------
select
*,
'tag' as cooperativa
from (
    select distinct
    ia.number_event as "numero_evento",
    CAST(ia.date_event as DATE) as "data_evento",
    year(ia.date_event) as "ano_evento",
    iab.parent as "atendimento",
    iab.benefit as "beneficio",
    iap.id as id_despesa,
    ROUND(cast(iap.assistance_total as DOUBLE), 2) as "valor_final_vidr",
    iar.regset as "conjunto",
    cata.fantasia as "unidade",
    v.descricao as "consultor",
    iv.board as "placa",
	ma.descricao as "marca",
	iv.year_model as "ano_modelo",    
    cat.nome as "associado",
    irsc.id as "coverage",
    ir.id as "matricula"
    
    from tag.insurance_assistance_benefits iab
    left join tag.insurance_assistance_providers iap on iap.coverage_id = iab.coverage and iab.parent = iap.parent
    left join tag.insurance_assistance ia on ia.id = iab.parent
    left join tag.insurance_assistance_reference iar on iar.parent = ia.id
    left join tag.insurance_reg_set_coverage irsc on irsc.id = iar.coverage
    left join tag.insurance_reg_set irs on irs.id = irsc.parent
	left join tag.representante r on r.codigo = ia.unity_id
	left join tag.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf     
    left join tag.vendedor v on v.codigo = irs.id_consultant
    left join tag.insurance_vehicle iv on iv.id = irsc.id_vehicle
    left join tag.insurance_registration ir on ir.id = ia.registration_id
    left join tag.cliente cli on cli.codigo = ir.customer_id
    left join tag.catalogo cat on cat.cnpj_cpf = cli.cnpj_cpf
	left join tag.marca_veiculo ma on ma.codigo = iv.code_brand_vehicle
    where
    iap.cancel <> 'S'
    and CAST(ia.date_event as DATE) >= DATE('2025-08-01')
    and iab.benefit in ('VIDROS')
    and iar.coverage <> 0
) as tag