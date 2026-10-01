-- Un entonnoir ne peut que se rétrécir : acceptations <= propositions <= vœux.
-- Vrai sur toutes les lignes de la session 2025 ; si une nouvelle livraison de la source
-- casse cette règle, la donnée est suspecte.
select
    formation_id,
    nb_voeux,
    nb_propositions,
    nb_acceptations
from {{ ref('formations') }}
where nb_acceptations > nb_propositions
    or nb_propositions > nb_voeux
