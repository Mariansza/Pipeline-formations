-- Une ligne par formation (session 2025).
-- Staging : on renomme, on type, on garde les colonnes utiles. Pas de règle métier ici.
select
    cast(session as bigint) as annee,
    cast(cod_aff_form as bigint) as formation_id,
    cod_uai as uai,
    g_ea_lib_vx as etablissement_nom,
    contrat_etab as statut_etablissement,
    dep as departement_code,
    dep_lib as departement_nom,
    region_etab_aff as region,
    acad_mies as academie,
    ville_etab as commune,
    regexp_replace(trim(lib_for_voe_ins), '\s+', ' ', 'g') as formation_nom,
    fili as filiere,
    select_form = 'formation sélective' as est_selective,
    capa_fin as capacite,
    voe_tot as nb_voeux,
    prop_tot as nb_propositions,
    acc_tot as nb_acceptations,
    taux_acces_ens as taux_acces_officiel
from {{ source('brut', 'raw_parcoursup_2025') }}
{% if var('academie') != 'toutes' %}
where acad_mies = '{{ var("academie") }}'
{% endif %}
