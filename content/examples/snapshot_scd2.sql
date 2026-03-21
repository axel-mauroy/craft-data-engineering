/*
   ==========================================================================
   CRAFT PATTERN : dbt Snapshots (SCD Type 2)
   ==========================================================================
   Le Slowly Changing Dimension Type 2 (SCD2) permet de conserver
   l'historique complet des changements d'une entité.
   Plutôt que d'écrire du SQL d'insertion/mise à jour complexe, le Craftsman
   délègue cette responsabilité au bloc {% snapshot %} de dbt.
   --------------------------------------------------------------------------
*/

{% snapshot snapshot_clients_scd2 %}

{{
    config(
        target_schema='snapshots',
        unique_key='clientId',
        
        -- ✅ CRAFT PATTERN : Stratégie 'check'
        -- dbt va comparer l'état actuel avec le dernier enregistrement actif.
        -- Si 'segmentFidelite' ou 'adresse' a changé, l'ancienne ligne est
        -- clôturée (dbt_valid_to) et une nouvelle ligne est insérée.
        strategy='check',
        check_cols=['segmentFidelite', 'adresse'],
        
        /* 
        💡 Alternative : Stratégie 'timestamp'
        Si la source maintient toujours une date de mise à jour fiable :
        strategy='timestamp',
        updated_at='derniere_modification_at',
        */
        
        -- Gestion avancée : si la ligne disparaît de la source, on clôture
        -- logiciellement l'enregistrement dans notre Snapshot.
        invalidate_hard_deletes=True
    )
}}

SELECT
    clientId,
    segmentFidelite,
    adresse
FROM {{ ref('stg_clients') }}

{% endsnapshot %}
