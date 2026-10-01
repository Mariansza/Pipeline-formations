{{ config(severity='warn') }}

-- Anomalie connue de la source : la capacité n'est pas un plafond, des formations
-- acceptent plus de candidats que leur capacité affichée (1 941 en 2025).
-- On la surveille sans bloquer le pipeline : severity='warn' affiche un avertissement.
select
    formation_id,
    capacite,
    nb_acceptations
from {{ ref('formations') }}
where capacite is not null
    and nb_acceptations > capacite
