SELECT
    p.id,
    p.nombre,
    p.fecha_de_presentacion,
    p.tipologia,
    p.tipo_de_proyecto,
    p.region,
    p.ei_document_communes,
    el.type,
    el.subtype,
    el.file_name            AS link_name,
    el.index                AS link_index,      -- alias para ordenar sin comillas
    df.file_name,
    df.from_compressed_file,
    REPLACE(
      df.s3_url,
      'https://nviro-crawlers.s3.us-west-2.amazonaws.com/',
      ''
    ) AS s3_key,
    REPLACE(
      df.s3_url,
      'https://nviro-crawlers.s3.us-west-2.amazonaws.com/',
      'https://443073691211-rq2lkjfg.us-west-2.console.aws.amazon.com/s3/object/nviro-crawlers?region=us-west-2&bucketType=general&prefix='
    ) AS aws_url
FROM proyecto              p
JOIN extracted_link     el ON p.id = el.fk_project_id
JOIN downloader.file    df ON el.id = df.fk_extracted_link_id
WHERE (
        el.type LIKE 'addendum-physics-files-%'
     OR el.type LIKE 'complementary-addendum-physics-files-%'
     OR el.type =  'ei-document'
      )
  AND el.url  <> 'https://seia.sea.gob.cl/archivos/'
  AND el.used IS TRUE
  AND df.file_format = 'pdf'
  AND p.estado = 'Aprobado'
  AND p.tipo   = 'DIA'
  AND p.fecha_de_presentacion::date >= DATE '2000-01-01'
  AND p.fecha_de_presentacion::date <  DATE '2025-01-01'
--  AND p.region = 'Región de Antofagasta'
  AND (
        df.file_name ~* '.*\bflora\b.*'
     OR df.file_name ~* '.*\bfauna\b.*'
     OR df.file_name ~* '.*fyv.*'
     OR df.file_name ~* '.*vegetación.*'
      )
--  AND df.file_name ~* '.*ldb.*'   --  ⇠ Descomentar si quieres filtrar ldb
ORDER BY
    p.id,
    p.fecha_de_presentacion,
    el.type,
    el.subtype,
    link_index,
    df.file_name;
