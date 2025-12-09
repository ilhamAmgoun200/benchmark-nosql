# 🔥 Benchmark NoSQL Multi-Modèle

Comparaison de performance entre MongoDB, Redis, Cassandra et Neo4j sur différents cas d'usage.

---

## 📋 Scénarios testés

| Scénario      | Description                        | Meilleur DB attendu        |
|---------------|------------------------------------|---------------------------|
| 1. CRUD       | INSERT/READ/UPDATE/DELETE basiques | MongoDB, Redis            |
| 2. IoT/Logs   | Données massives haute fréquence    | Cassandra                 |
| 3. Graphes    | Relations et traversées            | Neo4j                     |
| 4. Key-Value  | GET/SET ultra rapides               | Redis                     |
| 5. Full-Text  | Recherche textuelle                 | MongoDB                   |
| 6. Scalabilité| Multi-threading                     | Redis, Cassandra          |

Pour chaque scénario, on exécute le même test sur les 4 bases et on mesure :  

- ⏱️ Temps d’exécution total  
- 🔄 Latence par opération  
- 💻 CPU utilisé  
- 🧠 Mémoire utilisée  
- 📂 I/O (lecture/écriture)  

---

## 🔥 Scénarios détaillés

### 🟩 Scénario 1 — CRUD Simple
**Objectif :** tester les opérations basiques (INSERT, READ, UPDATE, DELETE).  

**Données utilisées (JSON simple) :**
```json
{
  "user_id": 1,
  "name": "Test User",
  "age": 25,
  "city": "Paris"
}
