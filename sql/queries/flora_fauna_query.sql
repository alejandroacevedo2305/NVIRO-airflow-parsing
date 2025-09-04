select
  -- p.id, p.nombre, p.fecha_de_presentacion, p.estado, p.url,
  -- el.type, el.subtype, el.index, el.file_name, el.used, df.id,
  -- df.file_name, from_compressed_file, df.s3_url, df.size_in_kb, df.file_format, df.created_at,
  p.id,
  p.url,
  p.nombre,
  p.fecha_de_presentacion,
  p.tipologia,
  p.tipo_de_proyecto,
  p.region,
  p.ei_document_communes,
  el.type,
  el.subtype,
  el.file_name as link_name,
  df.file_name,
  df.from_compressed_file,
  concat_ws('-', p.id, el.type, el.subtype, 'file_id_' || df.id::text) as file_identifier,
  replace(df.s3_url, 'https://nviro-crawlers.s3.us-west-2.amazonaws.com/', '') as s3_key,
  replace(df.s3_url, 'https://nviro-crawlers.s3.us-west-2.amazonaws.com/', 'https://443073691211-rq2lkjfg.us-west-2.console.aws.amazon.com/s3/object/nviro-crawlers?region=us-west-2&bucketType=general&prefix=') as aws_url
from proyecto p
  join extracted_link el on p.id = el.fk_project_id
  join downloader.file df on el.id = df.fk_extracted_link_id
where
  p.id in (
      select p.id--, p.nombre, bool_and(el.used) as ready, count(e.*) > 0 as has_errors
      from proyecto p
        join extracted_link el on el.fk_project_id = p.id
        left join downloader.error_log e on el.id = e.fk_extracted_link_id
      where p.estado = 'Aprobado' -- solo proyectos aprobados de momento
        and (
          el.type like 'addendum-physics-files-%' or
          el.type like 'complementary-addendum-physics-files-%' or
          el.type = 'ei-document'
        )
        and el.url != 'https://seia.sea.gob.cl/archivos/'
        and p.fecha_de_presentacion >= '2014-01-01'
      group by p.id, p.nombre
      having bool_and(el.used) = true and count(e.*) = 0
  ) -- filtro para priorizar proyectos con todos los docs descargados
  and (
    el.type like 'addendum-physics-files-%'
    or el.type like 'complementary-addendum-physics-files-%'
    or el.type = 'ei-document'
  )
  and el.url != 'https://seia.sea.gob.cl/archivos/'
  and el.used = true

  -- and el.type = 'ei-document'
  -- and el.subtype = 'article-11-details'
  -- and el.subtype = 'article-11-justification'

  -- solo documentos pdf
  and df.file_format = 'pdf'

  -- solo proyectos aprobados tipo DIA
  and p.estado = 'Aprobado'
  and p.tipo = 'DIA'

  -- filtro fecha
  and p.fecha_de_presentacion >= '2014-01-01'
  and p.fecha_de_presentacion < '2025-01-01'

--   -- filtro region
--   and p.region = 'Región de Antofagasta'
--
--   -- filtro flora o fauna
--   and (
--     df.file_name ~* '.* flora .*'
--     or df.file_name ~* '.* fauna .*'
--     or df.file_name ~* '.*fyv.*'
--     or df.file_name ~* '.*vegetación.*'
--   )
  -- filtro ldb
--   and (
--     df.file_name ~* '.*ldb.*'
--   )
order by p.fecha_de_presentacion desc, p.id, el.type, el.subtype, el.index, df.file_name;
