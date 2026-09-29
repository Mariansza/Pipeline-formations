-- Pont département -> académie, dérivé des vœux : un département n'a qu'une académie.
-- Lit la source brute et non stg_parcoursup_2025, qui est filtré sur une académie.
-- La clé est le nom en minuscules : la casse diffère entre les deux sources (Puy-de-Dôme / Puy-de-dôme).
select distinct
    lower(dep_lib) as departement_cle,
    acad_mies as academie
from {{ source('brut', 'raw_parcoursup_2025') }}
where dep_lib is not null
