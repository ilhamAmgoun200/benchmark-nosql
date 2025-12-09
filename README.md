# 🔥 Benchmark NoSQL Multi-Modèle

Comparaison de performance entre **MongoDB**, **Redis**, **Cassandra** et **Neo4j** sur différents cas d'usage.

---

## 📋 Scénarios testés

| Scénario | Description | Meilleur DB attendu |
| :--- | :--- | :--- |
| 1. CRUD | INSERT/READ/UPDATE/DELETE basiques | MongoDB, Redis |
| 2. IoT/Logs | Données massives haute fréquence | Cassandra |
| 3. Graphes | Relations et traversées | Neo4j |
| 4. Key-Value | GET/SET ultra rapides | Redis |
| 5. Full-Text | Recherche textuelle | MongoDB |
| 6. Scalabilité | Multi-threading | Redis, Cassandra |

Pour chaque scénario, on exécute le même test sur les 4 bases et on mesure :
*   ⏱️ Temps d’exécution total
*   🔄 Latence par opération
*   💻 CPU utilisé
*   🧠 Mémoire utilisée
*   📂 I/O (lecture/écriture)

---

## 🔥 Scénarios détaillés

### 🟩 Scénario 1 — CRUD Simple
**Objectif** : Tester les opérations basiques (INSERT, READ, UPDATE, DELETE).
*   **Données utilisées (JSON simple)** :
    ```json
    {
      "user_id": 1,
      "name": "Test User",
      "age": 25,
      "city": "Paris"
    }
    ```
*   **Tests** : 100k INSERT, 100k READ, 100k UPDATE, 100k DELETE.
*   **Conclusion attendue** :
    *   **MongoDB** : Très bon
    *   **Redis** : Excellent (en mémoire)
    *   **Cassandra** : Lent en UPDATE mais rapide en INSERT
    *   **Neo4j** : Pas adapté

| Champ | Signification |
| :--- | :--- |
| `write_time` | Temps total d’écriture |
| `write_latency_ms` | Latence moyenne d’écriture |
| `read_time` | Temps total de lecture |
| `read_latency_ms` | Latence moyenne de lecture |
| `cpu_usage` | CPU utilisée |
| `ram_usage` | RAM utilisée |

### 🟦 Scénario 2 — IoT / Logs
**Objectif** : Simuler des données massives haute fréquence (ex: capteurs).
*   **Données** :
    ```json
    {
      "sensor_id": "A12",
      "timestamp": "2025-12-03 18:01:00",
      "temperature": 25.4,
      "humidity": 64
    }
    ```
*   **Tests** : Insertion batch, lecture par plage de timestamps, indexation.
*   **Conclusion attendue** : Cassandra > MongoDB (avec index) > Redis > Neo4j

| Champ | Signification |
| :--- | :--- |
| `insert_time` | Temps d’insertion des données |
| `insert_throughput` | Débit d’insertion (records/sec) |
| `insert_cpu` | CPU utilisée pour insertion |
| `insert_mem` | RAM utilisée |
| `index_time` | Temps pour créer index (MongoDB) |
| `range_query_time` | Temps pour requête sur intervalle |
| `records_found` | Nombre d’enregistrements retrouvés |

### 🟥 Scénario 3 — Graph Social
**Objectif** : Tester les relations et requêtes complexes dans un graphe.
*   **Données** : Utilisateurs + relations `friend_of` et `likes`.
*   **Tests** : Amis d’un ami, connexions sur 3 niveaux, communautés.
*   **Conclusion attendue** : Neo4j > RedisGraph (optionnel) > MongoDB/Cassandra

| Champ | Signification |
| :--- | :--- |
| `create_users_time` | Création des utilisateurs |
| `create_friendships_time` | Création des relations |
| `friends_of_friends_time` | Temps requête "amis des amis" |
| `three_level_time` | Temps requête profondeur 3 |
| `records_inserted` | Nombre de nœuds insérés |
| `records_found` | Résultats retournés |

### 🟧 Scénario 4 — Key-Value Haute Performance
**Objectif** : Tester la rapidité pour SET/GET.
*   **Données** : Clés → Valeurs simples (session, token).
*   **Tests** : 100k SET, 100k GET, expiration TTL.
*   **Conclusion attendue** : Redis imbattable.

| Champ | Signification |
| :--- | :--- |
| `set_latency_ms` | Temps pour SET |
| `get_latency_ms` | Temps pour GET |
| `throughput_ops` | Opérations/sec |
| `cpu_usage` | CPU utilisée |
| `ram_usage` | RAM utilisée |
| `memory_used_mb` | RAM totale Redis |

### 🟪 Scénario 5 — Recherche Textuelle
**Objectif** : Tester les recherches full-text.
*   **Données** : 100k articles (titre, contenu, tags).
*   **Tests** : Recherche par mot-clé, multi-mots, tri par pertinence.
*   **Conclusion attendue** : MongoDB > Redis (RedisSearch) > Cassandra/Neo4j.

| Champ | Signification |
| :--- | :--- |
| `insert_time` | Durée d’insertion des documents |
| `index_build_time` | Temps pour construire index texte |
| `search_latency` | Temps pour rechercher |
| `top_k_results` | Documents trouvés |
| `cpu_usage` | CPU pendant index/recherche |
| `ram_usage` | RAM utilisée |

### 🟫 Scénario 6 — Scalabilité Multi-Modèle
**Objectif** : Tester le comportement en montée en charge et opérations combinées.
*   **Tests** : Multi-threading (5,10,50,100), insert + read simultanés.
*   **Conclusion attendue** : Cassandra > Redis > MongoDB > Neo4j.

| Champ | Signification |
| :--- | :--- |
| `create_time` | Temps création données |
| `read_time` | Temps lecture données |
| `update_time` | Temps mise à jour |
| `delete_time` | Temps suppression |
| `complex_query_time` | Temps requête complexe |
| `cpu_total` | CPU totale utilisée |
| `ram_total` | RAM totale utilisée |
| `throughput_ops` | Ops/sec totales |

---

## 🎯 Résumé simple

| Scénario | Description |
| :--- | :--- |
| 1. CRUD Simple | Lecture/écriture basique |
| 2. IoT / Time-Series | Insertion massive + requêtes par intervalle |
| 3. Social Graph | Nœuds + relations + requêtes profondes |
| 4. Key-Value Haute Vitesse | SET/GET grande échelle |
| 5. Recherche Textuelle | Indexation + recherche full-text |
| 6. Benchmark Global | CRUD + requêtes complexes + analyse |

---

## 🚀 Installation rapide

```bash
git clone <url>
cd benchmark-nosql
pip install -r requirements.txt
cd docker
docker-compose up -d
docker ps
```

## ▶️ Exécution

```bash
# Tous les scénarios
python run_all_benchmarks.py

# Scénario spécifique
python scenario1_crud_benchmark.py
python scenario2_iot_logs.py

```

## 📊 Visualisation des résultats

**Grafana** : [http://localhost:3000](http://localhost:3000) (admin / admin)

**InfluxDB** : [http://localhost:8086](http://localhost:8086) (Org: ensa, Bucket: bench)

**Exemple de requête InfluxDB :**

```flux
from(bucket: "bench")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "scenario1_crud")
  |> filter(fn: (r) => r["_field"] == "latency_ms")
  |> group(columns: ["database", "operation"])
```

## 🛑 Arrêter le projet
```bash
docker-compose down       # arrêter les conteneurs
docker-compose down -v    # supprimer tout (volumes inclus)
```
## 📝 Résultats attendus par DB
| DB | Excellent | Moyen |
| :--- | :--- | :--- |
| **MongoDB** 🔵 | CRUD, Full-Text, IoT (index) | Graphes, Scalabilité |
| **Redis** 🔴 | Key-Value, Scalabilité, CRUD lecture | Full-Text (sans RedisSearch), Graphes |
| **Cassandra** 🟣 | IoT/Logs, Scalabilité | UPDATE, Graphes, Full-Text |
| **Neo4j** 🟢 | Graphes complexes | CRUD simples, Key-Value, IoT, Scalabilité |

##  Problèmes courants

- **Cassandra lent au démarrage** → `docker logs cassandra`
- **Erreur connexion InfluxDB** → vérifier le token et `docker ps`
- **Module Python manquant** → `pip install -r requirements.txt`

## 👨‍💻 Auteurs

- **Ahlam** - Configuration Docker
- **Ilham** - Configuration Docker et tests Benchmark
- **Lina Ait Ider** - Analyse de données et Visualisation Grafana

