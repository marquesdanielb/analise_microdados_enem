-- 1. Qual a escola com a maior média de notas?
-- (Aproximado pelo município da escola e tipo, já que CO_ESCOLA muitas vezes está ausente)
SELECT 
    e.no_municipio_esc, 
    AVG(f.media_notas) as media_geral_escola
FROM fato_resultados f
JOIN dim_escola e ON f.co_municipio_esc = e.co_municipio_esc
WHERE f.co_municipio_esc IS NOT NULL
GROUP BY e.no_municipio_esc
ORDER BY media_geral_escola DESC
LIMIT 1;

-- 2. Qual o aluno com a maior média de notas e o valor dessa média?
SELECT 
    nu_inscricao, 
    media_notas
FROM fato_resultados
ORDER BY media_notas DESC
LIMIT 1;

-- 3. Qual a média geral?
SELECT 
    AVG(media_notas) as media_geral
FROM fato_resultados;

-- 4. Qual o % de Ausentes?
-- Considera ausente quem faltou em todas as provas (CN, CH, LC, MT) -> TP_PRESENCA = 0
SELECT 
    (SUM(CASE WHEN tp_presenca_cn = 0 AND tp_presenca_ch = 0 AND tp_presenca_lc = 0 AND tp_presenca_mt = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100 as percentual_ausentes
FROM fato_resultados;

-- 5. Qual o número total de Inscritos?
SELECT 
    COUNT(*) as total_inscritos
FROM dim_aluno;

-- 6. Qual a média por disciplina?
SELECT 
    AVG(nota_cn) as media_cn,
    AVG(nota_ch) as media_ch,
    AVG(nota_lc) as media_lc,
    AVG(nota_mt) as media_mt,
    AVG(nota_redacao) as media_redacao
FROM fato_resultados;

-- 7. Qual a média por Sexo?
SELECT 
    a.tp_sexo, 
    AVG(f.media_notas) as media_por_sexo
FROM fato_resultados f
JOIN dim_aluno a ON f.nu_inscricao = a.nu_inscricao
GROUP BY a.tp_sexo;

-- 8. Qual a média por Etnia?
SELECT 
    a.tp_cor_raca, 
    AVG(f.media_notas) as media_por_etnia
FROM fato_resultados f
JOIN dim_aluno a ON f.nu_inscricao = a.nu_inscricao
GROUP BY a.tp_cor_raca;
