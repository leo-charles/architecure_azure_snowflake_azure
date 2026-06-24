# Spécification — Table `silver.trays` (Apache Iceberg)

> Document de référence pour la table Silver Iceberg de Hatchlog.  
> Généré le 2026-06-23 · Format Iceberg v2 · Catalog SQLite (dev) → Polaris (prod)

---

## Localisation

| Propriété | Valeur |
|---|---|
| Nom logique | `silver.trays` |
| Format | Apache Iceberg v2 |
| Location physique | `abfss://silver@dlsecatcandlingfrcedev.dfs.core.windows.net/trays_iceberg` |
| Storage | ADLS Gen2 — Hierarchical Namespace activé |
| Catalog dev | SQLite local — `catalog_dev.db` (non versionné) |
| Catalog prod | Polaris (Snowflake Open Catalog) — à déployer |

---

## Partitionnement

```
machine_id=PMAF-C012501/
    candled_date=2026-05-15/
        part-xxxxx.parquet
```

| Colonne | Transform | Rôle |
|---|---|---|
| `machine_id` | Identity | Isolation tenant — 1 machine = 1 client |
| `candled_date` | Identity | Filtre temporel — pruning journalier |

> **Règle** : toute requête doit filtrer sur `machine_id` en premier.  
> C'est la clé de partition principale — sans elle, un full scan de tous les tenants est déclenché.

---

## Schéma des champs

### Identifiants

| field_id | Nom | Type | Nullable | Description |
|---|---|---|---|---|
| 1 | `tray_id` | `string` | Non nullable | SHA-256 de `machine_id\|candled_at_utc` — clé d'idempotence |
| 2 | `machine_id` | `string` | Non nullable | Identifiant machine — ex: `PMAF-C012501` |

> **`tray_id` est déterministe** : rejouer le même fichier Bronze produit le même `tray_id`.  
> C'est le mécanisme central d'idempotence — validé dans NB 06.

---

### Timestamps

| field_id | Nom | Type | Nullable | Description |
|---|---|---|---|---|
| 3 | `candled_at` | `timestamptz` | Non nullable | Timestamp UTC du mirage — ancré sur le filename IoT Hub |
| 4 | `candled_date` | `date` | Non nullable | Date extraite de `candled_at` — colonne de partition |
| 23 | `processed_at` | `timestamptz` | Non nullable | Timestamp UTC d'ingestion par la Function Azure |

> **`candled_at` est autoritaire** — le timestamp interne de la machine est invalide  
> (exemple connu : `second=64`). On utilise toujours le timestamp du fichier IoT Hub.

> **`timestamptz` vs `timestamp`** : Iceberg stocke explicitement le timezone UTC,  
> contrairement à Delta Lake (`timestamp` sans timezone). Plus précis, plus sûr.

---

### Dimensions flock / trolley

| field_id | Nom | Type | Nullable | Description |
|---|---|---|---|---|
| 5 | `flock` | `int` | Non nullable | Numéro de lot — entier métier (1, 2, 3...) |
| 6 | `trolley` | `string` | Non nullable | Identifiant chariot — alphanumérique ex: `20713024` |
| 7 | `tray_seq` | `int` | Non nullable | Numéro de séquence du plateau dans la journée |
| 8 | `flock_name` | `string` | Nullable | Hash du nom de lot — ex: `43e6ab99aeea0b2d6ca` |
| 9 | `trolley_name` | `string` | Nullable | Nom du trolley (identique à `trolley` en pratique) |
| 10 | `caliber` | `string` | Nullable | Calibre des œufs |
| 11 | `setter_tray_number_flock` | `int` | Nullable | Numéro du plateau dans le lot (depuis les tags Bronze) |

> **`flock` est `int`** dans Iceberg — correction par rapport au schéma Delta  
> où il était `string`. C'est un numéro métier, pas un identifiant textuel.

---

### Comptages œufs

| field_id | Nom | Type | Nullable | Description |
|---|---|---|---|---|
| 12 | `n_total` | `int` | Non nullable | Total œufs — toujours 150 si `is_count_consistent=true` |
| 13 | `n_fertile` | `int` | Non nullable | Œufs fertiles — classe 1 |
| 14 | `n_clear` | `int` | Non nullable | Œufs clairs — classe 3 |
| 15 | `n_early_dead` | `int` | Non nullable | Morts précoces — classe 2 |
| 16 | `n_late_dead` | `int` | Non nullable | Morts tardifs — classe 4 |
| 17 | `n_missing` | `int` | Non nullable | Manquants — classe 0 |
| 18 | `is_count_consistent` | `boolean` | Non nullable | `true` si `n_total == 150` |

> **Règle d'or : ces comptages sont TOUJOURS recomputés depuis `matrix_compact`.**  
> Ne jamais copier les valeurs des tags Bronze (`#_egg_flock`, `#_living_embryo_flock`...).  
> Un bug réel a été identifié en test : `clear_count` et `late_dead_count` étaient inversés  
> dans les tags Bronze — la matrice était correcte.

> **Classes d'œufs** : `0`=manquant · `1`=fertile · `2`=mort précoce · `3`=clair · `4`=mort tardif

---

### Données brutes

| field_id | Nom | Type | Nullable | Description |
|---|---|---|---|---|
| 19 | `matrix_compact` | `string` | Non nullable | Matrice 15×10 encodée en 150 caractères — ordre ligne-major |
| 20 | `light_flat` | `list<int>` | Nullable | Valeurs lumière transmise — 150 entiers, ordre ligne-major |

> **`matrix_compact`** : chaîne de 150 caractères, chaque char = classe de l'œuf.  
> Exemple : `"111113111..."` — position `[i][j]` = `matrix_compact[i*10 + j]` (0-based).

> **`light_flat`** : tableau de 150 entiers correspondant aux tags  
> `laser1_light_transmitted_eggs[i][j]`. Valeurs typiques : 130 000 – 200 000.  
> Une valeur `0` indique une position manquante ou non mesurée.

---

### Alarmes

| field_id | Nom | Type | Nullable | Description |
|---|---|---|---|---|
| 21 | `alarm_emergency_stop` | `int` | Nullable | 0=OK · 1=arrêt d'urgence déclenché |
| 22 | `alarm_air_pressure_fault` | `int` | Nullable | 0=OK · 1=défaut pression air |

> **`int` au lieu de `byte`** : PyIceberg ne supporte pas `ByteType` —  
> les valeurs restent 0 ou 1 en pratique, le type `int` n'a pas d'impact fonctionnel.

---

### Traçabilité

| field_id | Nom | Type | Nullable | Description |
|---|---|---|---|---|
| 24 | `bronze_path` | `string` | Nullable | Nom du fichier NDJSON source — ex: `04_32_0.json` |

---

## Règles d'écriture

### Idempotence

```python
tray_id = SHA-256(f"{machine_id}|{candled_at_utc}")
```

Avant tout `append`, filtrer les `tray_id` déjà présents dans la partition :

```python
existing_ids = set(table.scan(row_filter=...).to_pandas()["tray_id"])
new_rows = df[~df["tray_id"].isin(existing_ids)]
if not new_rows.empty:
    table.append(df_to_arrow(new_rows))
```

### Pattern d'écriture

```
1 fichier NDJSON IoT Hub (batch 60s)
    → parse → DataFrame → PyArrow → table.append()
    = 1 snapshot Iceberg
    = 1 fichier Parquet dans la partition
```

Latence mesurée en dev : **0.55s moyenne · 1.01s max** par batch.  
Budget disponible : 60s. Marge confortable.

---

## Différences avec le schéma Delta Lake

| Champ | Delta Lake | Iceberg | Raison |
|---|---|---|---|
| `flock` | `string` | `int` | Correction de type — c'est un numéro métier |
| `n_total` ... `n_missing` | `short` | `int` | PyIceberg ne supporte pas `ShortType` |
| `alarm_*` | `byte` | `int` | PyIceberg ne supporte pas `ByteType` |
| `candled_at` / `processed_at` | `timestamp` | `timestamptz` | Iceberg stocke explicitement UTC |

---

## Dette technique

| Sujet | Statut | Action |
|---|---|---|
| Catalog SQLite | Dev uniquement | Migrer vers Polaris quand Snowflake onboardé |
| `catalog_dev.db` | Non versionné | Ajouté dans `.gitignore` |
| Compaction des petits fichiers | Non implémenté | À planifier en prod si N snapshots > seuil |
| Lecture depuis Snowflake | Non testé | Dépend du déploiement Polaris |

