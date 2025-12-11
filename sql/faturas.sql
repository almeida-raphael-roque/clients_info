-- relatório de faturas (17/10/2024)
select distinct
cat.nome as associado,
ir.id as matricula,
irs.id as conjunto,
cata.fantasia as unidade,
v.descricao as consultor,
t.ponteiro as idtitlemoviment,
t.data_vencimento as data_vencimento,
a.descricao as aplicacao_fin,
(t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) as valuetitle,
case 
	when t.ponteiro_consolidado > 0 then 'consolidado'
	when ii.situation = 'C' or (bx.entra_fluxo_caixa <> 'S') then 'cancelado'
	when coalesce(bx.valor_baixa,0) <= 0 then 'aberto'
	when coalesce(bx.valor_baixa,0) >= (t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) then 'pago'
	else 'pago parcialmente' 
end situationdescription,
cast(bx.data_baixa as date) as data_baixa,
bx.valor_baixa as valor_baixa,
t.referencia,
'Segtruck' as cooperativa,
round(((bx.valor_baixa * a.taxa_comissao) / 100),2) as comissao

--------------------------------------------------------------------------------------------------

from silver.invoice i
left join silver.representante r on r.codigo = i.id_unity
left join silver.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
left join silver.vendedor v on v.codigo = i.id_consultant
left join silver.insurance_reg_set irs on irs.id = i.id_set
left join silver.insurance_registration ir on ir.id = irs.parent
left join silver.cliente clie on clie.codigo = ir.customer_id
left join silver.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
inner join silver.invoice_item ii on i.id = ii.parent
inner join silver.titulo_movimento t on ii.id_title_moviment = t.id_titulo_movimento
inner join silver.aplicacao_recurso_financeiro a on a.codigo = t.codigo_aplicacao_recurso_fin and a.codigo_empresa = t.codigo_empresa
inner join silver.grupo_aplic_rec_financeiro g on a.codigo_grupo = g.codigo and a.codigo_empresa = g.codigo_empresa
left outer join (
	select tb.ponteiro,
	coalesce(sum(tb.valor_baixa),0) as valor_baixa,
	max(tb.data_lancamento) as data_baixa,
	min(sd.entra_fluxo_caixa) as entra_fluxo_caixa
	from silver.titulo_movimento tb
	inner join silver.situacao_documento sd on sd.codigo = tb.codigo_situacao_documento
	where tb.historico not in(1,5)
	group by tb.ponteiro
) as bx on bx.ponteiro = t.ponteiro
where cast(bx.data_baixa as date) >= date('2025-01-01')
    
--------------------------------------------------------------------------------------------------
UNION ALL
--------------------------------------------------------------------------------------------------

select distinct
cat.nome as associado,
ir.id as matricula,
irs.id as conjunto,
cata.fantasia as unidade,
v.descricao as consultor,
t.ponteiro as idtitlemoviment,
t.data_vencimento as data_vencimento,
a.descricao as aplicacao_fin,
(t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) as valuetitle,
case 
	when t.ponteiro_consolidado > 0 then 'consolidado'
	when ii.situation = 'C' or (bx.entra_fluxo_caixa <> 'S') then 'cancelado'
	when coalesce(bx.valor_baixa,0) <= 0 then 'aberto'
	when coalesce(bx.valor_baixa,0) >= (t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) then 'pago'
	else 'pago parcialmente' 
end situationdescription,
cast(bx.data_baixa as date) as data_baixa,
bx.valor_baixa as valor_baixa,
t.referencia,
'Stcoop' as cooperativa,
round(((bx.valor_baixa * a.taxa_comissao) / 100),2) as comissao

--------------------------------------------------------------------------------------------------

from stcoop.invoice i
left join stcoop.representante r on r.codigo = i.id_unity
left join stcoop.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
left join stcoop.vendedor v on v.codigo = i.id_consultant
left join stcoop.insurance_reg_set irs on irs.id = i.id_set
left join stcoop.insurance_registration ir on ir.id = irs.parent
left join stcoop.cliente clie on clie.codigo = ir.customer_id
left join stcoop.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
inner join stcoop.invoice_item ii on i.id = ii.parent
inner join stcoop.titulo_movimento t on ii.id_title_moviment = t.id_titulo_movimento
inner join stcoop.aplicacao_recurso_financeiro a on a.codigo = t.codigo_aplicacao_recurso_fin and a.codigo_empresa = t.codigo_empresa
inner join stcoop.grupo_aplic_rec_financeiro g on a.codigo_grupo = g.codigo and a.codigo_empresa = g.codigo_empresa
left outer join (
	select tb.ponteiro,
	coalesce(sum(tb.valor_baixa),0) as valor_baixa,
	max(tb.data_lancamento) as data_baixa,
	min(sd.entra_fluxo_caixa) as entra_fluxo_caixa
	from stcoop.titulo_movimento tb
	inner join stcoop.situacao_documento sd on sd.codigo = tb.codigo_situacao_documento
	where tb.historico not in(1,5)
	group by tb.ponteiro
) as bx on bx.ponteiro = t.ponteiro
where cast(bx.data_baixa as date) >= date('2025-01-01')

--------------------------------------------------------------------------------------------------
UNION ALL
--------------------------------------------------------------------------------------------------

select distinct
cat.nome as associado,
ir.id as matricula,
irs.id as conjunto,
cata.fantasia as unidade,
v.descricao as consultor,
t.ponteiro as idtitlemoviment,
t.data_vencimento as data_vencimento,
a.descricao as aplicacao_fin,
(t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) as valuetitle,
case 
	when t.ponteiro_consolidado > 0 then 'consolidado'
	when ii.situation = 'C' or (bx.entra_fluxo_caixa <> 'S') then 'cancelado'
	when coalesce(bx.valor_baixa,0) <= 0 then 'aberto'
	when coalesce(bx.valor_baixa,0) >= (t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) then 'pago'
	else 'pago parcialmente' 
end situationdescription,
cast(bx.data_baixa as date) as data_baixa,
bx.valor_baixa as valor_baixa,
t.referencia,
'Viavante' as cooperativa,
round(((bx.valor_baixa * a.taxa_comissao) / 100),2) as comissao

--------------------------------------------------------------------------------------------------

from viavante.invoice i
left join viavante.representante r on r.codigo = i.id_unity
left join viavante.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
left join viavante.vendedor v on v.codigo = i.id_consultant
left join viavante.insurance_reg_set irs on irs.id = i.id_set
left join viavante.insurance_registration ir on ir.id = irs.parent
left join viavante.cliente clie on clie.codigo = ir.customer_id
left join viavante.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
inner join viavante.invoice_item ii on i.id = ii.parent
inner join viavante.titulo_movimento t on ii.id_title_moviment = t.id_titulo_movimento
inner join viavante.aplicacao_recurso_financeiro a on a.codigo = t.codigo_aplicacao_recurso_fin and a.codigo_empresa = t.codigo_empresa
inner join viavante.grupo_aplic_rec_financeiro g on a.codigo_grupo = g.codigo and a.codigo_empresa = g.codigo_empresa
left outer join (
	select tb.ponteiro,
	coalesce(sum(tb.valor_baixa),0) as valor_baixa,
	max(tb.data_lancamento) as data_baixa,
	min(sd.entra_fluxo_caixa) as entra_fluxo_caixa
	from viavante.titulo_movimento tb
	inner join viavante.situacao_documento sd on sd.codigo = tb.codigo_situacao_documento
	where tb.historico not in(1,5)
	group by tb.ponteiro
) as bx on bx.ponteiro = t.ponteiro
where cast(bx.data_baixa as date) >= date('2025-01-01')
    	
--------------------------------------------------------------------------------------------------
UNION ALL
--------------------------------------------------------------------------------------------------

select distinct
cat.nome as associado,
ir.id as matricula,
irs.id as conjunto,
cata.fantasia as unidade,
v.descricao as consultor,
t.ponteiro as idtitlemoviment,
t.data_vencimento as data_vencimento,
a.descricao as aplicacao_fin,
(t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) as valuetitle,
case 
	when t.ponteiro_consolidado > 0 then 'consolidado'
	when ii.situation = 'C' or (bx.entra_fluxo_caixa <> 'S') then 'cancelado'
	when coalesce(bx.valor_baixa,0) <= 0 then 'aberto'
	when coalesce(bx.valor_baixa,0) >= (t.valor_titulo_movimento + t.valor_acrescimo - t.valor_desconto) then 'pago'
	else 'pago parcialmente' 
end situationdescription,
cast(bx.data_baixa as date) as data_baixa,
bx.valor_baixa as valor_baixa,
t.referencia,
'Tag' as cooperativa,
round(((bx.valor_baixa * a.taxa_comissao) / 100),2) as comissao

--------------------------------------------------------------------------------------------------

from tag.invoice i
left join tag.representante r on r.codigo = i.id_unity
left join tag.catalogo cata on cata.cnpj_cpf = r.cnpj_cpf
left join tag.vendedor v on v.codigo = i.id_consultant
left join tag.insurance_reg_set irs on irs.id = i.id_set
left join tag.insurance_registration ir on ir.id = irs.parent
left join tag.cliente clie on clie.codigo = ir.customer_id
left join tag.catalogo cat on cat.cnpj_cpf = clie.cnpj_cpf
inner join tag.invoice_item ii on i.id = ii.parent
inner join tag.titulo_movimento t on ii.id_title_moviment = t.id_titulo_movimento
inner join tag.aplicacao_recurso_financeiro a on a.codigo = t.codigo_aplicacao_recurso_fin and a.codigo_empresa = t.codigo_empresa
inner join tag.grupo_aplic_rec_financeiro g on a.codigo_grupo = g.codigo and a.codigo_empresa = g.codigo_empresa
left outer join (
	select tb.ponteiro,
	coalesce(sum(tb.valor_baixa),0) as valor_baixa,
	max(tb.data_lancamento) as data_baixa,
	min(sd.entra_fluxo_caixa) as entra_fluxo_caixa
	from tag.titulo_movimento tb
	inner join tag.situacao_documento sd on sd.codigo = tb.codigo_situacao_documento
	where tb.historico not in(1,5)
	group by tb.ponteiro
) as bx on bx.ponteiro = t.ponteiro
where cast(bx.data_baixa as date) >= date('2025-08-01')
