-- Une ligne par formation et par session (clé : annee, formation_id).
-- Staging : on renomme, on type, on rattache l'académie via le pont des départements.
select
    c.annee,
    cast(c.gta as bigint) as formation_id,
    c.etab_uai as uai,
    c.etab_nom as etablissement_nom,
    c.tf as type_formation,
    regexp_replace(trim(c.nm), '\s+', ' ', 'g') as formation_nom,
    c.nmc as formation_nom_court,
    cast(c.code_formation as bigint) as code_formation,
    c.app is not null as est_apprentissage,
    c.region,
    c.departement as departement_nom,
    c.commune,
    d.academie
from {{ source('brut', 'raw_cartographie') }} as c
left join {{ ref('stg_departements') }} as d
    on lower(c.departement) = d.departement_cle
{% if var('academie') != 'toutes' %}
where d.academie = '{{ var("academie") }}'
{% endif %}
