-- Une ligne par formation des vœux, avec la formation de la cartographie à laquelle elle correspond
-- (vide si aucune), la méthode d'appariement et le score.
select formation_id, cartographie_formation_id, methode_appariement, score
from {{ ref('int_appariement_exact') }}

union all

select formation_id, cartographie_formation_id, methode_appariement, score
from {{ ref('int_appariement_flou') }}

union all

select
    v.formation_id,
    null as cartographie_formation_id,
    'aucune' as methode_appariement,
    null as score
from {{ ref('stg_parcoursup_2025') }} as v
anti join {{ ref('int_appariement_exact') }} as e
    on e.formation_id = v.formation_id
anti join {{ ref('int_appariement_flou') }} as f
    on f.formation_id = v.formation_id
