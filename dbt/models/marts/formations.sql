-- Référentiel des formations : une ligne par formation connue d'au moins une source.
-- = formations de la cartographie (pour la session des vœux) + formations des vœux sans correspondance.
-- Les statistiques et le taux d'admission sont vides pour les formations absentes des vœux.
with cartographie as (
    select *
    from {{ ref('stg_cartographie') }}
    where annee in (select annee from {{ ref('stg_parcoursup_2025') }})
),

depuis_cartographie as (
    select
        c.formation_id,
        c.annee,
        c.uai,
        c.etablissement_nom,
        c.formation_nom,
        c.type_formation,
        c.est_apprentissage,
        c.departement_nom,
        c.region,
        c.commune,
        coalesce(v.academie, c.academie) as academie,
        coalesce(a.methode_appariement, 'aucune') as methode_appariement,
        v.capacite,
        v.nb_voeux,
        v.nb_propositions,
        v.nb_acceptations,
        v.taux_acces_officiel
    from cartographie as c
    left join {{ ref('int_formations_appariees') }} as a
        on a.cartographie_formation_id = c.formation_id
    left join {{ ref('stg_parcoursup_2025') }} as v
        on v.formation_id = a.formation_id
),

-- Formations des vœux absentes de la cartographie : on garde ce que les vœux savent d'elles.
-- Les vœux ne couvrent que des formations hors apprentissage (vérifié sur les 14 214 appariées).
depuis_voeux_seuls as (
    select
        v.formation_id,
        v.annee,
        v.uai,
        v.etablissement_nom,
        v.formation_nom,
        cast(null as varchar) as type_formation,
        false as est_apprentissage,
        v.departement_nom,
        v.region,
        v.commune,
        v.academie,
        a.methode_appariement,
        v.capacite,
        v.nb_voeux,
        v.nb_propositions,
        v.nb_acceptations,
        v.taux_acces_officiel
    from {{ ref('stg_parcoursup_2025') }} as v
    join {{ ref('int_formations_appariees') }} as a
        on a.formation_id = v.formation_id
    where a.methode_appariement = 'aucune'
),

toutes as (
    select * from depuis_cartographie
    union all
    select * from depuis_voeux_seuls
)

select
    *,
    nb_voeux is not null as a_statistiques,
    -- Part des candidats à qui la formation a proposé une place (0 à 100).
    -- Vide sans statistiques ; 0 est un taux valide (aucune proposition).
    round(100.0 * nb_propositions / nullif(nb_voeux, 0), 1) as taux_admission
from toutes
