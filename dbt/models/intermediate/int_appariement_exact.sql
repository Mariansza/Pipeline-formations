-- Phase 1 : jointure exacte sur l'identifiant de formation, pour la même session.
-- Une ligne par formation des vœux retrouvée dans la cartographie.
select
    v.formation_id,
    c.formation_id as cartographie_formation_id,
    'exacte' as methode_appariement,
    1.0 as score
from {{ ref('stg_parcoursup_2025') }} as v
join {{ ref('stg_cartographie') }} as c
    on c.formation_id = v.formation_id
    and c.annee = v.annee
