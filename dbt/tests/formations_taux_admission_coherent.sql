-- Le taux d'admission est un pourcentage (0 à 100), et il est vide exactement quand
-- la formation n'a pas de statistiques. Un taux à 0 est valide (aucune proposition).
select
    formation_id,
    a_statistiques,
    taux_admission
from {{ ref('formations') }}
where taux_admission < 0
    or taux_admission > 100
    or a_statistiques <> (taux_admission is not null)
