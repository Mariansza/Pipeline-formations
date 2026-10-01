-- La clé de la cartographie est le couple (annee, formation_id) : une formation par année.
-- Un identifiant seul se répète d'une année à l'autre, c'est normal.
-- Le test échoue s'il retourne des lignes (couples en double).
select
    annee,
    formation_id,
    count(*) as nb_lignes
from {{ ref('stg_cartographie') }}
group by annee, formation_id
having count(*) > 1
