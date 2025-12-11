/*relatorio_comissao_completo*/

--union all
-------------------------------------
select cast(isete.id as varchar) "conjunto", 
irsc.id as "coverage",
isete.parent as "matricula",
c.fantasia "unidade",
v.descricao "vendedor",
a.descricao "aplicacao_fin",
bx.data_baixa "data_baixa",
bx.valor_baixa "valor_baixa",
iset.date_activation "data_ativacao",
tm.data_vencimento "data_vencimento",
tm.referencia "referencia",
iset.id_renovated_set "conjunto_anterior",
b.description as "beneficio",
isete.monthly_value as "valor_mensalidade",
isete.adhesion_value as "valor_adesao",
ccli.nome as "associado",
(bx.valor_baixa * (a.taxa_comissao / 100)) as "comissao"

from silver.titulo_movimento tm 
	inner join silver.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento
	left join silver.representante r on r.pessoa = tc.pessoa_representante
	    and r.cnpj_cpf = tc.cnpj_cpf_representante
	left join silver.catalogo c on c.pessoa = tc.pessoa_representante 
	    and c.cnpj_cpf = tc.cnpj_cpf_representante
	inner join silver.endereco en on en.pessoa = tc.pessoa_representante
	    and  en.cnpj_cpf = tc.cnpj_cpf_representante
	    and en.sequencia = tc.seq_end_rep
	inner join silver.cliente cl on tm.pessoa = cl.pessoa
	    and tm.cnpj_cpf = cl.cnpj_cpf
	inner join silver.catalogo ccli on tm.pessoa = ccli.pessoa 
	    and tm.cnpj_cpf = ccli.cnpj_cpf
	inner join silver.endereco ecli on tm.pessoa = ecli.pessoa 
	    and tm.cnpj_cpf = ecli.cnpj_cpf
	    and tm.seq_endereco = ecli.sequencia
	left outer join silver.invoice_item ii on ii.id_title_moviment = tm.id_titulo_movimento
	left outer join silver.invoice iff on ii.parent = iff.id
	left outer join silver.insurance_reg_set iset on iset.id = iff.id_set
	left outer join silver.insurance_reg_set_coverage irsc on irsc.parent = iset.id
	left outer join silver.insurance_reg_set isete on isete.id = iff.id_set
	left outer join silver.price_list_benefits plb on plb.id = irsc.id_price_list
	left outer join silver.type_category ty on ty.id = plb.id_type_category
	left outer join silver.category c on c.id = ty.id_category
	left outer join silver.benefits b on b.id = c.id_benefits
	left outer join silver.vendedor v on iset.id_consultant = v.codigo
	inner join silver.aplicacao_recurso_financeiro a on tm.codigo_aplicacao_recurso_fin = a.codigo and a.codigo_empresa = tm.codigo_empresa
	inner join (
				select max(tb.data_lancamento) data_baixa,
				sum(tb.valor_baixa) as valor_baixa,
				tb.ponteiro
				from titulo_movimento tb inner join silver.situacao_documento stb on tb.codigo_situacao_documento = stb.codigo
				where tb.historico not in(1,5)
				and tb.crc_cpg = 'R'
				and stb.entra_fluxo_caixa = 'S'
				and (tb.ponteiro_consolidado is null or tb.ponteiro_consolidado = 0)
				group by tb.ponteiro
				) bx on tm.ponteiro = bx.ponteiro
				and a.taxa_comissao > 0
				and (tm.ponteiro_consolidado is null or tm.ponteiro_consolidado = 0)
				
where a.codigo in(166,1)
    and bx.data_baixa >= date('2025-01-01')
    


-------------------------------------------------------------------------------------------------------------------------------------------
union all
--------------------------------------------------------------------------------------------------------------------------------------------
select cast(isete.id as varchar) "conjunto", 
irsc.id as "coverage",
isete.parent as "matricula",
c.fantasia "unidade",
v.descricao "vendedor",
a.descricao "aplicacao_fin",
bx.data_baixa "data_baixa",
bx.valor_baixa "valor_baixa",
iset.date_activation "data_ativacao",
tm.data_vencimento "data_vencimento",
tm.referencia "referencia",
iset.id_renovated_set "conjunto_anterior",
b.description as "beneficio",
isete.monthly_value as "valor_mensalidade",
isete.adhesion_value as "valor_adesao",
ccli.nome as "associado",
(bx.valor_baixa * (a.taxa_comissao / 100)) as "comissao"

from stcoop.titulo_movimento tm 
	inner join stcoop.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento
	left join stcoop.representante r on r.pessoa = tc.pessoa_representante
	    and r.cnpj_cpf = tc.cnpj_cpf_representante
	left join stcoop.catalogo c on c.pessoa = tc.pessoa_representante 
	    and c.cnpj_cpf = tc.cnpj_cpf_representante
	inner join stcoop.endereco en on en.pessoa = tc.pessoa_representante
	    and  en.cnpj_cpf = tc.cnpj_cpf_representante
	    and en.sequencia = tc.seq_end_rep
	inner join stcoop.cliente cl on tm.pessoa = cl.pessoa
	    and tm.cnpj_cpf = cl.cnpj_cpf
	inner join stcoop.catalogo ccli on tm.pessoa = ccli.pessoa 
	    and tm.cnpj_cpf = ccli.cnpj_cpf
	inner join stcoop.endereco ecli on tm.pessoa = ecli.pessoa 
	    and tm.cnpj_cpf = ecli.cnpj_cpf
	    and tm.seq_endereco = ecli.sequencia
	left outer join stcoop.invoice_item ii on ii.id_title_moviment = tm.id_titulo_movimento
	left outer join stcoop.invoice iff on ii.parent = iff.id
	left outer join stcoop.insurance_reg_set iset on iset.id = iff.id_set
	left outer join stcoop.insurance_reg_set_coverage irsc on irsc.parent = iset.id
	left outer join stcoop.insurance_reg_set isete on isete.id = iff.id_set
	left outer join stcoop.price_list_benefits plb on plb.id = irsc.id_price_list
	left outer join stcoop.type_category ty on ty.id = plb.id_type_category
	left outer join stcoop.category c on c.id = ty.id_category
	left outer join stcoop.benefits b on b.id = c.id_benefits
    left outer join stcoop.vendedor v on iset.id_consultant = v.codigo
	inner join stcoop.aplicacao_recurso_financeiro a on tm.codigo_aplicacao_recurso_fin = a.codigo and a.codigo_empresa = tm.codigo_empresa
	inner join (
				select max(tb.data_lancamento) data_baixa,
				sum(tb.valor_baixa) as valor_baixa,
				tb.ponteiro
				from titulo_movimento tb inner join stcoop.situacao_documento stb on tb.codigo_situacao_documento = stb.codigo
				where tb.historico not in(1,5)
				and tb.crc_cpg = 'R'
				and stb.entra_fluxo_caixa = 'S'
				and (tb.ponteiro_consolidado is null or tb.ponteiro_consolidado = 0)
				group by tb.ponteiro
				) bx on tm.ponteiro = bx.ponteiro
				    and a.taxa_comissao > 0
				    and (tm.ponteiro_consolidado is null or tm.ponteiro_consolidado = 0)
				
where a.codigo in(166,1)
    and bx.data_baixa >= date('2025-01-01')



--order by bx.data_baixa, isete.id


-------------------------------------------------------------------------------------------------------------------------------------------
union all
--------------------------------------------------------------------------------------------------------------------------------------------
select cast(isete.id as varchar) "conjunto", 
irsc.id as "coverage",
isete.parent as "matricula",
c.fantasia "unidade",
v.descricao "vendedor",
a.descricao "aplicacao_fin",
bx.data_baixa "data_baixa",
bx.valor_baixa "valor_baixa",
iset.date_activation "data_ativacao",
tm.data_vencimento "data_vencimento",
tm.referencia "referencia",
iset.id_renovated_set "conjunto_anterior",
b.description as "beneficio",
isete.monthly_value as "valor_mensalidade",
isete.adhesion_value as "valor_adesao",
ccli.nome as "associado",
(bx.valor_baixa * (a.taxa_comissao / 100)) as "comissao"

from viavante.titulo_movimento tm 
	inner join viavante.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento
	left join viavante.representante r on r.pessoa = tc.pessoa_representante
	    and r.cnpj_cpf = tc.cnpj_cpf_representante
	left join viavante.catalogo c on c.pessoa = tc.pessoa_representante 
	    and c.cnpj_cpf = tc.cnpj_cpf_representante
	inner join viavante.endereco en on en.pessoa = tc.pessoa_representante
	    and  en.cnpj_cpf = tc.cnpj_cpf_representante
	    and en.sequencia = tc.seq_end_rep
	inner join viavante.cliente cl on tm.pessoa = cl.pessoa
	    and tm.cnpj_cpf = cl.cnpj_cpf
	inner join viavante.catalogo ccli on tm.pessoa = ccli.pessoa 
	    and tm.cnpj_cpf = ccli.cnpj_cpf
	inner join viavante.endereco ecli on tm.pessoa = ecli.pessoa 
	    and tm.cnpj_cpf = ecli.cnpj_cpf
	    and tm.seq_endereco = ecli.sequencia
	left outer join viavante.invoice_item ii on ii.id_title_moviment = tm.id_titulo_movimento
	left outer join viavante.invoice iff on ii.parent = iff.id
	left outer join viavante.insurance_reg_set iset on iset.id = iff.id_set
	left outer join viavante.insurance_reg_set_coverage irsc on irsc.parent = iset.id
	left outer join viavante.insurance_reg_set isete on isete.id = iff.id_set
	left outer join viavante.price_list_benefits plb on plb.id = irsc.id_price_list
	left outer join viavante.type_category ty on ty.id = plb.id_type_category
	left outer join viavante.category c on c.id = ty.id_category
	left outer join viavante.benefits b on b.id = c.id_benefits
    left outer join viavante.vendedor v on iset.id_consultant = v.codigo
	inner join viavante.aplicacao_recurso_financeiro a on tm.codigo_aplicacao_recurso_fin = a.codigo and a.codigo_empresa = tm.codigo_empresa
	inner join (
				select max(tb.data_lancamento) data_baixa,
				sum(tb.valor_baixa) as valor_baixa,
				tb.ponteiro
				from titulo_movimento tb inner join viavante.situacao_documento stb on tb.codigo_situacao_documento = stb.codigo
				where tb.historico not in(1,5)
				and tb.crc_cpg = 'R'
				and stb.entra_fluxo_caixa = 'S'
				and (tb.ponteiro_consolidado is null or tb.ponteiro_consolidado = 0)
				group by tb.ponteiro
				) bx on tm.ponteiro = bx.ponteiro
				    and a.taxa_comissao > 0
				    and (tm.ponteiro_consolidado is null or tm.ponteiro_consolidado = 0)
				
where a.codigo in(166,1)
    and bx.data_baixa >= date('2025-01-01')



--order by bx.data_baixa, isete.id

-------------------------------------------------------------------------------------------------------------------------------------------
union all
--------------------------------------------------------------------------------------------------------------------------------------------
select cast(isete.id as varchar) "conjunto", 
irsc.id as "coverage",
isete.parent as "matricula",
c.fantasia "unidade",
v.descricao "vendedor",
a.descricao "aplicacao_fin",
bx.data_baixa "data_baixa",
bx.valor_baixa "valor_baixa",
iset.date_activation "data_ativacao",
tm.data_vencimento "data_vencimento",
tm.referencia "referencia",
iset.id_renovated_set "conjunto_anterior",
b.description as "beneficio",
isete.monthly_value as "valor_mensalidade",
isete.adhesion_value as "valor_adesao",
ccli.nome as "associado",
(bx.valor_baixa * (a.taxa_comissao / 100)) as "comissao"

from tag.titulo_movimento tm 
	inner join tag.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento
	left join tag.representante r on r.pessoa = tc.pessoa_representante
	    and r.cnpj_cpf = tc.cnpj_cpf_representante
	left join tag.catalogo c on c.pessoa = tc.pessoa_representante 
	    and c.cnpj_cpf = tc.cnpj_cpf_representante
	inner join tag.endereco en on en.pessoa = tc.pessoa_representante
	    and  en.cnpj_cpf = tc.cnpj_cpf_representante
	    and en.sequencia = tc.seq_end_rep
	inner join tag.cliente cl on tm.pessoa = cl.pessoa
	    and tm.cnpj_cpf = cl.cnpj_cpf
	inner join tag.catalogo ccli on tm.pessoa = ccli.pessoa 
	    and tm.cnpj_cpf = ccli.cnpj_cpf
	inner join tag.endereco ecli on tm.pessoa = ecli.pessoa 
	    and tm.cnpj_cpf = ecli.cnpj_cpf
	    and tm.seq_endereco = ecli.sequencia
	left outer join tag.invoice_item ii on ii.id_title_moviment = tm.id_titulo_movimento
	left outer join tag.invoice iff on ii.parent = iff.id
	left outer join tag.insurance_reg_set iset on iset.id = iff.id_set
	left outer join tag.insurance_reg_set_coverage irsc on irsc.parent = iset.id
	left outer join tag.insurance_reg_set isete on isete.id = iff.id_set
	left outer join tag.price_list_benefits plb on plb.id = irsc.id_price_list
	left outer join tag.type_category ty on ty.id = plb.id_type_category
	left outer join tag.category c on c.id = ty.id_category
	left outer join tag.benefits b on b.id = c.id_benefits
    left outer join tag.vendedor v on iset.id_consultant = v.codigo
	inner join tag.aplicacao_recurso_financeiro a on tm.codigo_aplicacao_recurso_fin = a.codigo and a.codigo_empresa = tm.codigo_empresa
	inner join (
				select max(tb.data_lancamento) data_baixa,
				sum(tb.valor_baixa) as valor_baixa,
				tb.ponteiro
				from titulo_movimento tb inner join tag.situacao_documento stb on tb.codigo_situacao_documento = stb.codigo
				where tb.historico not in(1,5)
				and tb.crc_cpg = 'R'
				and stb.entra_fluxo_caixa = 'S'
				and (tb.ponteiro_consolidado is null or tb.ponteiro_consolidado = 0)
				group by tb.ponteiro
				) bx on tm.ponteiro = bx.ponteiro
				    and a.taxa_comissao > 0
				    and (tm.ponteiro_consolidado is null or tm.ponteiro_consolidado = 0)
				
where a.codigo in(166,1)
    and bx.data_baixa >= date('2025-01-01')



--order by bx.data_baixa, isete.id



