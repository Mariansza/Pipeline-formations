-- Phase 2 : repli approximatif sur les formations des vœux sans correspondance exacte.
-- Blocage : on ne compare que des formations du même établissement (UAI) et de la même session.
-- Candidats : formations hors apprentissage de la cartographie, pas déjà prises par la phase 1
-- (les formations en apprentissage n'ont jamais de statistiques de vœux).
with orphelins as (
    select
        v.formation_id,
        v.annee,
        v.uai,
        lower(strip_accents(v.formation_nom)) as nom_normalise
    from {{ ref('stg_parcoursup_2025') }} as v
    anti join {{ ref('int_appariement_exact') }} as e
        on e.formation_id = v.formation_id
),

candidats as (
    select
        c.formation_id,
        c.annee,
        c.uai,
        lower(strip_accents(c.formation_nom)) as nom_normalise
    from {{ ref('stg_cartographie') }} as c
    anti join {{ ref('int_appariement_exact') }} as e
        on e.cartographie_formation_id = c.formation_id
    where not c.est_apprentissage
),

paires as (
    select
        o.formation_id,
        c.formation_id as cartographie_formation_id,
        jaro_winkler_similarity(o.nom_normalise, c.nom_normalise) as score
    from orphelins as o
    join candidats as c
        on c.uai = o.uai
        and c.annee = o.annee
),

classees as (
    select
        *,
        row_number() over (partition by formation_id order by score desc) as rang
    from paires
)

select
    formation_id,
    cartographie_formation_id,
    'floue' as methode_appariement,
    score
from classees
where rang = 1
    and score >= {{ var('seuil_flou') }}
