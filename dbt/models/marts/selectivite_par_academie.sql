{{ config(materialized='view') }}

-- Sélectivité par académie, sur les formations qui ont des statistiques de vœux.
-- taux_admission_moyen : moyenne des taux des formations (chaque formation compte pour 1).
-- taux_admission_global : total des propositions / total des vœux (chaque candidat compte pour 1).
select
    academie,
    count(*) as nb_formations,
    sum(nb_voeux) as nb_voeux,
    sum(nb_propositions) as nb_propositions,
    round(avg(taux_admission), 1) as taux_admission_moyen,
    round(100.0 * sum(nb_propositions) / nullif(sum(nb_voeux), 0), 1) as taux_admission_global
from {{ ref('formations') }}
where a_statistiques
group by academie
order by taux_admission_global
