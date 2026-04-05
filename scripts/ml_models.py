"""
Machine Learning models for:
1. Vote Simulator: Predict how a deputy would vote on a given text
2. Anomaly Detection: Identify deputies with atypical voting patterns per theme
"""

import json
import os
import numpy as np
from collections import defaultdict, Counter

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(data, filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved {filename}")

# ─── Theme classification for scrutins ──────────────────────────────────────

THEMES = {
    "Economie & Finance": ["budget", "financ", "fiscal", "impôt", "taxe", "économi", "dette", "déficit", "banque", "commerce", "entreprise"],
    "Sécurité & Justice": ["sécurité", "justice", "pénal", "police", "prison", "délinquanc", "terroris", "gendarm", "criminel"],
    "Santé": ["santé", "médic", "hôpital", "soins", "pandémi", "vaccin", "pharma", "sécurité sociale", "maladie"],
    "Education": ["éducation", "école", "université", "enseignement", "étudiant", "formation", "recherche", "scolaire"],
    "Ecologie & Environnement": ["écologi", "environnement", "climat", "énergi", "carbone", "biodiversité", "pollution", "renouvelable", "nucléaire", "eau"],
    "Social & Travail": ["social", "travail", "emploi", "retraite", "chômage", "salaire", "RSA", "solidarité", "assurance", "protection sociale"],
    "Immigration": ["immigration", "migrat", "asile", "étranger", "intégration", "naturalisation", "frontière", "titre de séjour"],
    "Agriculture": ["agricul", "agricole", "paysan", "alimenta", "rural", "pêche", "PAC", "élevage", "pesticide"],
    "Défense": ["défense", "armée", "militaire", "OTAN", "opération extérieure", "ancien combattant"],
    "Numérique & Technologies": ["numérique", "technolog", "intelligence artificielle", "données", "cyber", "digital", "telecom"],
    "Logement & Urbanisme": ["logement", "immobilier", "urbanis", "habitat", "HLM", "loyer", "copropriété"],
    "Transport": ["transport", "mobilité", "ferroviaire", "SNCF", "autoroute", "aérien", "routier"],
    "Culture": ["culture", "patrimoine", "audiovisuel", "média", "art", "musée", "spectacle", "sport"],
    "Outre-mer": ["outre-mer", "ultramarin", "DOM-TOM", "Polynésie", "Nouvelle-Calédonie", "Réunion", "Guadeloupe", "Martinique", "Guyane", "Mayotte"],
    "Institutions": ["constitution", "institution", "élection", "scrutin", "referendum", "décentralis", "commune", "région", "sénat"],
}

def classify_scrutin_theme(title):
    """Classify a scrutin by theme based on its title."""
    title_lower = title.lower()
    matched = []
    for theme, keywords in THEMES.items():
        for kw in keywords:
            if kw.lower() in title_lower:
                matched.append(theme)
                break
    return matched if matched else ["Autre"]


# ═══════════════════════════════════════════════════════════════════════════
# 1. VOTE SIMULATOR
# ═══════════════════════════════════════════════════════════════════════════

def build_vote_simulator():
    """
    Build a vote prediction model.
    For each deputy, we build a profile:
    - Overall loyalty score
    - Per-theme voting tendencies (pour/contre/abstention ratios)
    - Group alignment per theme

    Prediction logic:
    1. Look at the text's themes
    2. Check deputy's historical voting on those themes
    3. Check group's typical position on those themes
    4. Weight by loyalty score
    """
    print("\n" + "=" * 60)
    print("  BUILDING VOTE SIMULATOR")
    print("=" * 60)

    deputy_votes = load_json("deputy_votes.json")
    scrutin_info = load_json("scrutins_info.json")
    deputes = load_json("deputes_enriched.json")

    # Build per-deputy, per-theme voting profiles
    deputy_profiles = {}

    for dep in deputes:
        uid = dep["uid"]
        votes = deputy_votes.get(uid, [])

        if not votes:
            continue

        # Overall stats
        vote_counts = Counter(v["vote"] for v in votes if v["vote"] != "nonVotant")
        total = sum(vote_counts.values())

        # Per-theme stats
        theme_stats = defaultdict(lambda: {"pour": 0, "contre": 0, "abstention": 0, "total": 0,
                                            "loyal": 0, "rebel": 0})

        for v in votes:
            if v["vote"] == "nonVotant":
                continue

            sc_info = scrutin_info.get(str(v["scrutin"]), {})
            themes = classify_scrutin_theme(sc_info.get("titre", ""))

            # Check loyalty for this vote
            pos = v.get("group_position", "").lower()
            vote = v["vote"].lower()
            is_loyal = (pos == vote) or (pos == "liberté de vote")

            for theme in themes:
                theme_stats[theme][vote] += 1
                theme_stats[theme]["total"] += 1
                if is_loyal:
                    theme_stats[theme]["loyal"] += 1
                else:
                    theme_stats[theme]["rebel"] += 1

        # Compute per-theme tendencies
        theme_profiles = {}
        for theme, stats in theme_stats.items():
            t = stats["total"]
            if t > 0:
                theme_profiles[theme] = {
                    "pour_rate": round(stats["pour"] / t, 4),
                    "contre_rate": round(stats["contre"] / t, 4),
                    "abstention_rate": round(stats["abstention"] / t, 4),
                    "loyalty_rate": round(stats["loyal"] / t, 4),
                    "rebel_rate": round(stats["rebel"] / t, 4),
                    "total_votes": t,
                }

        deputy_profiles[uid] = {
            "uid": uid,
            "nom": dep["nom"],
            "prenom": dep["prenom"],
            "groupe": dep["groupe_sigle"],
            "loyalty_global": dep.get("score_loyaute"),
            "overall_pour_rate": round(vote_counts.get("pour", 0) / total, 4) if total > 0 else 0,
            "overall_contre_rate": round(vote_counts.get("contre", 0) / total, 4) if total > 0 else 0,
            "overall_abstention_rate": round(vote_counts.get("abstention", 0) / total, 4) if total > 0 else 0,
            "total_votes": total,
            "theme_profiles": theme_profiles,
        }

    save_json(deputy_profiles, "vote_simulator_profiles.json")

    # Build group-level profiles for fallback
    group_profiles = defaultdict(lambda: defaultdict(lambda: {"pour": 0, "contre": 0, "abstention": 0, "total": 0}))

    for uid, profile in deputy_profiles.items():
        group = profile["groupe"]
        for theme, stats in profile["theme_profiles"].items():
            gp = group_profiles[group][theme]
            gp["pour"] += int(stats["pour_rate"] * stats["total_votes"])
            gp["contre"] += int(stats["contre_rate"] * stats["total_votes"])
            gp["abstention"] += int(stats["abstention_rate"] * stats["total_votes"])
            gp["total"] += stats["total_votes"]

    group_profiles_final = {}
    for group, themes in group_profiles.items():
        group_profiles_final[group] = {}
        for theme, stats in themes.items():
            t = stats["total"]
            if t > 0:
                group_profiles_final[group][theme] = {
                    "pour_rate": round(stats["pour"] / t, 4),
                    "contre_rate": round(stats["contre"] / t, 4),
                    "abstention_rate": round(stats["abstention"] / t, 4),
                }

    save_json(group_profiles_final, "group_profiles.json")

    print(f"  Built profiles for {len(deputy_profiles)} deputies")
    print(f"  Built profiles for {len(group_profiles_final)} groups")

    # Demo predictions
    print("\n  === DEMO: Simulating votes ===")
    demo_texts = [
        {"title": "Projet de loi immigration et intégration", "themes": ["Immigration"]},
        {"title": "Budget 2026 de la sécurité sociale", "themes": ["Santé", "Social & Travail", "Economie & Finance"]},
        {"title": "Loi climat et résilience écologique", "themes": ["Ecologie & Environnement"]},
        {"title": "Réforme des retraites", "themes": ["Social & Travail", "Economie & Finance"]},
    ]

    for text in demo_texts:
        print(f"\n  > '{text['title']}' (themes: {text['themes']})")
        # Predict for a few deputies
        sample_deps = list(deputy_profiles.values())[:5]
        for dp in sample_deps:
            prediction = predict_vote(dp, text["themes"], group_profiles_final)
            print(f"    {dp['prenom']} {dp['nom']} ({dp['groupe']}): "
                  f"Pour={prediction['pour']:.0%} Contre={prediction['contre']:.0%} "
                  f"Abst={prediction['abstention']:.0%} → {prediction['predicted_vote']}")

    return deputy_profiles, group_profiles_final


def predict_vote(deputy_profile, themes, group_profiles):
    """
    Predict a deputy's vote given themes.

    Algorithm:
    1. For each theme, get the deputy's historical voting tendency
    2. If no history for a theme, use group profile
    3. Weight by loyalty score (high loyalty = follows group)
    4. Average across themes
    """
    loyalty = (deputy_profile.get("loyalty_global") or 85) / 100.0
    group = deputy_profile["groupe"]

    pour_score = 0
    contre_score = 0
    abstention_score = 0
    weight_sum = 0

    for theme in themes:
        tp = deputy_profile.get("theme_profiles", {}).get(theme)
        gp = group_profiles.get(group, {}).get(theme)

        if tp and tp["total_votes"] >= 5:
            # Deputy has enough history on this theme
            # Blend personal tendency with group tendency
            personal_weight = 1 - loyalty * 0.3  # High loyalty = less personal deviation
            group_weight = loyalty * 0.3

            if gp:
                pour = tp["pour_rate"] * personal_weight + gp["pour_rate"] * group_weight
                contre = tp["contre_rate"] * personal_weight + gp["contre_rate"] * group_weight
                abstention = tp["abstention_rate"] * personal_weight + gp["abstention_rate"] * group_weight
            else:
                pour = tp["pour_rate"]
                contre = tp["contre_rate"]
                abstention = tp["abstention_rate"]

            w = min(tp["total_votes"] / 20, 1.0)  # Weight by data confidence
        elif gp:
            # No personal history, use group
            pour = gp["pour_rate"]
            contre = gp["contre_rate"]
            abstention = gp["abstention_rate"]
            w = 0.5
        else:
            # No data at all, use overall
            pour = deputy_profile["overall_pour_rate"]
            contre = deputy_profile["overall_contre_rate"]
            abstention = deputy_profile["overall_abstention_rate"]
            w = 0.3

        pour_score += pour * w
        contre_score += contre * w
        abstention_score += abstention * w
        weight_sum += w

    if weight_sum > 0:
        pour_score /= weight_sum
        contre_score /= weight_sum
        abstention_score /= weight_sum

    # Normalize
    total = pour_score + contre_score + abstention_score
    if total > 0:
        pour_score /= total
        contre_score /= total
        abstention_score /= total

    predicted = "pour" if pour_score >= contre_score and pour_score >= abstention_score else \
                "contre" if contre_score >= abstention_score else "abstention"

    return {
        "pour": pour_score,
        "contre": contre_score,
        "abstention": abstention_score,
        "predicted_vote": predicted,
        "confidence": max(pour_score, contre_score, abstention_score),
    }


# ═══════════════════════════════════════════════════════════════════════════
# 2. ANOMALY DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def detect_anomalies():
    """
    Detect deputies with atypical voting patterns within their group.

    Method:
    1. For each group, compute the average voting profile per theme
    2. For each deputy, compute distance from their group's profile
    3. Flag deputies who deviate significantly on specific themes
    4. Use clustering (K-Means) to find voting blocs that cross party lines
    """
    print("\n" + "=" * 60)
    print("  ANOMALY DETECTION")
    print("=" * 60)

    profiles = load_json("vote_simulator_profiles.json")
    deputes = load_json("deputes_enriched.json")

    # Step 1: Compute per-group, per-theme average
    print("\n  Step 1: Computing group averages per theme...")
    group_avgs = defaultdict(lambda: defaultdict(lambda: {"pour_rates": [], "contre_rates": [], "abstention_rates": []}))

    for uid, p in profiles.items():
        group = p["groupe"]
        for theme, tp in p["theme_profiles"].items():
            if tp["total_votes"] >= 3:
                group_avgs[group][theme]["pour_rates"].append(tp["pour_rate"])
                group_avgs[group][theme]["contre_rates"].append(tp["contre_rate"])
                group_avgs[group][theme]["abstention_rates"].append(tp["abstention_rate"])

    # Compute means and stds
    group_stats = {}
    for group, themes in group_avgs.items():
        group_stats[group] = {}
        for theme, data in themes.items():
            if len(data["pour_rates"]) >= 3:
                group_stats[group][theme] = {
                    "pour_mean": np.mean(data["pour_rates"]),
                    "pour_std": np.std(data["pour_rates"]),
                    "contre_mean": np.mean(data["contre_rates"]),
                    "contre_std": np.std(data["contre_rates"]),
                    "n_deputies": len(data["pour_rates"]),
                }

    # Step 2: Find anomalies
    print("  Step 2: Detecting anomalies...")
    anomalies = []

    for uid, p in profiles.items():
        group = p["groupe"]
        gs = group_stats.get(group, {})

        deputy_anomalies = []
        for theme, tp in p["theme_profiles"].items():
            if tp["total_votes"] < 5:
                continue

            ts = gs.get(theme)
            if not ts or ts["pour_std"] == 0:
                continue

            # Z-score for deviation from group
            z_pour = abs(tp["pour_rate"] - ts["pour_mean"]) / max(ts["pour_std"], 0.01)
            z_contre = abs(tp["contre_rate"] - ts["contre_mean"]) / max(ts["contre_std"], 0.01)

            max_z = max(z_pour, z_contre)

            if max_z > 1.5:  # Significant deviation
                direction = ""
                if tp["pour_rate"] > ts["pour_mean"] + ts["pour_std"]:
                    direction = f"vote POUR plus que son groupe ({tp['pour_rate']:.0%} vs {ts['pour_mean']:.0%})"
                elif tp["contre_rate"] > ts["contre_mean"] + ts["contre_std"]:
                    direction = f"vote CONTRE plus que son groupe ({tp['contre_rate']:.0%} vs {ts['contre_mean']:.0%})"
                elif tp["pour_rate"] < ts["pour_mean"] - ts["pour_std"]:
                    direction = f"vote POUR moins que son groupe ({tp['pour_rate']:.0%} vs {ts['pour_mean']:.0%})"

                if direction:
                    deputy_anomalies.append({
                        "theme": theme,
                        "z_score": round(max_z, 2),
                        "direction": direction,
                        "deputy_pour_rate": round(tp["pour_rate"], 4),
                        "group_pour_mean": round(ts["pour_mean"], 4),
                        "deputy_contre_rate": round(tp["contre_rate"], 4),
                        "group_contre_mean": round(ts["contre_mean"], 4),
                        "votes_on_theme": tp["total_votes"],
                    })

        if deputy_anomalies:
            deputy_anomalies.sort(key=lambda x: x["z_score"], reverse=True)
            anomalies.append({
                "uid": uid,
                "nom": p["nom"],
                "prenom": p["prenom"],
                "groupe": group,
                "loyalty_global": p.get("loyalty_global"),
                "anomalies": deputy_anomalies,
                "max_z_score": deputy_anomalies[0]["z_score"],
                "nb_themes_atypiques": len(deputy_anomalies),
            })

    anomalies.sort(key=lambda x: x["max_z_score"], reverse=True)
    save_json(anomalies, "anomalies.json")

    # Step 3: Cross-party clustering
    print("  Step 3: Cross-party clustering...")
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        # Build feature matrix: per-theme pour_rate for each deputy
        all_themes = sorted(set(t for g in group_stats.values() for t in g.keys()))
        feature_names = all_themes

        uid_list = []
        feature_matrix = []

        for uid, p in profiles.items():
            if p["total_votes"] < 50:
                continue
            features = []
            for theme in all_themes:
                tp = p["theme_profiles"].get(theme, {})
                features.append(tp.get("pour_rate", 0.5))
            uid_list.append(uid)
            feature_matrix.append(features)

        if len(feature_matrix) > 10:
            X = np.array(feature_matrix)
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            # Find optimal k (try 3-8)
            best_k = 5
            kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(X_scaled)

            # Analyze clusters
            clusters = defaultdict(list)
            for i, uid in enumerate(uid_list):
                p = profiles[uid]
                clusters[int(labels[i])].append({
                    "uid": uid,
                    "nom": p["nom"],
                    "prenom": p["prenom"],
                    "groupe": p["groupe"],
                })

            cluster_analysis = {}
            for cluster_id, members in clusters.items():
                group_dist = Counter(m["groupe"] for m in members)
                cluster_analysis[cluster_id] = {
                    "size": len(members),
                    "group_distribution": dict(group_dist),
                    "members_sample": members[:10],
                    "is_cross_party": len(group_dist) > 1,
                }

            save_json(cluster_analysis, "voting_clusters.json")
            print(f"  Found {best_k} clusters:")
            for cid, info in cluster_analysis.items():
                print(f"    Cluster {cid}: {info['size']} députés - "
                      f"Groups: {info['group_distribution']}")

    except ImportError:
        print("  scikit-learn not available, skipping clustering")

    # Print top anomalies
    print(f"\n  Top 15 anomalies détectées:")
    for a in anomalies[:15]:
        top_anomaly = a["anomalies"][0]
        print(f"    {a['prenom']} {a['nom']} ({a['groupe']}, loyauté: {a['loyalty_global']}%)")
        print(f"      → {top_anomaly['theme']}: {top_anomaly['direction']} (z={top_anomaly['z_score']})")

    # Interesting patterns: loyal deputies who rebel on specific themes
    print(f"\n  Députés loyaux (>90%) mais atypiques sur un thème:")
    loyal_rebels = [a for a in anomalies if a["loyalty_global"] and a["loyalty_global"] > 90 and a["max_z_score"] > 2]
    for a in loyal_rebels[:10]:
        top = a["anomalies"][0]
        print(f"    {a['prenom']} {a['nom']} ({a['groupe']}, loyauté: {a['loyalty_global']}%)")
        print(f"      → Atypique sur '{top['theme']}': {top['direction']}")

    return anomalies


# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    profiles, group_profiles = build_vote_simulator()
    anomalies = detect_anomalies()

    print(f"\n{'=' * 60}")
    print(f"  ML MODELS COMPLETE")
    print(f"  - Vote simulator profiles: {len(profiles)}")
    print(f"  - Anomalies detected: {len(anomalies)}")
    print(f"{'=' * 60}")
