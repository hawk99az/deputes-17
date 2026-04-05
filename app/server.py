"""
Flask web application for Députés 17e Législature Analysis
"""

import json
import os
import sys
from flask import Flask, render_template, jsonify, request

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True

# ─── Load data once at startup ──────────────────────────────────────────────

def load_data():
    data = {}
    files = [
        "deputes_enriched_v2.json",
        "vote_simulator_profiles.json",
        "group_profiles.json",
        "anomalies_improved.json",
        "voting_clusters.json",
        "scrutins_info.json",
    ]
    for f in files:
        path = os.path.join(DATA_DIR, f)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fp:
                data[f.replace(".json", "")] = json.load(fp)
    return data

DATA = load_data()

# ─── Theme classification (same as ml_models.py) ───────────────────────────

THEMES = {
    "Economie & Finance": ["budget", "financ", "fiscal", "impot", "taxe", "economi", "dette", "deficit", "banque", "commerce", "entreprise"],
    "Securite & Justice": ["securite", "justice", "penal", "police", "prison", "delinquanc", "terroris", "gendarm", "criminel"],
    "Sante": ["sante", "medic", "hopital", "soins", "pandemi", "vaccin", "pharma", "securite sociale", "maladie"],
    "Education": ["education", "ecole", "universite", "enseignement", "etudiant", "formation", "recherche", "scolaire"],
    "Ecologie & Environnement": ["ecologi", "environnement", "climat", "energi", "carbone", "biodiversite", "pollution", "renouvelable", "nucleaire", "eau"],
    "Social & Travail": ["social", "travail", "emploi", "retraite", "chomage", "salaire", "rsa", "solidarite", "assurance", "protection sociale"],
    "Immigration": ["immigration", "migrat", "asile", "etranger", "integration", "naturalisation", "frontiere", "titre de sejour"],
    "Agriculture": ["agricul", "agricole", "paysan", "alimenta", "rural", "peche", "pac", "elevage", "pesticide"],
    "Defense": ["defense", "armee", "militaire", "otan", "operation exterieure", "ancien combattant"],
    "Numerique & Technologies": ["numerique", "technolog", "intelligence artificielle", "donnees", "cyber", "digital", "telecom"],
    "Logement & Urbanisme": ["logement", "immobilier", "urbanis", "habitat", "hlm", "loyer", "copropriete"],
    "Transport": ["transport", "mobilite", "ferroviaire", "sncf", "autoroute", "aerien", "routier"],
    "Culture": ["culture", "patrimoine", "audiovisuel", "media", "art", "musee", "spectacle", "sport"],
    "Outre-mer": ["outre-mer", "ultramarin", "dom-tom", "polynesie", "nouvelle-caledonie", "reunion", "guadeloupe", "martinique", "guyane", "mayotte"],
    "Institutions": ["constitution", "institution", "election", "scrutin", "referendum", "decentralis", "commune", "region", "senat"],
}

def classify_themes(text):
    import unicodedata
    text_clean = unicodedata.normalize('NFD', text.lower())
    text_clean = ''.join(c for c in text_clean if unicodedata.category(c) != 'Mn')
    matched = []
    for theme, keywords in THEMES.items():
        for kw in keywords:
            if kw in text_clean:
                matched.append(theme)
                break
    return matched if matched else ["Autre"]

def predict_vote(deputy_profile, themes, group_profiles):
    loyalty = (deputy_profile.get("loyalty_global") or 85) / 100.0
    group = deputy_profile["groupe"]
    pour_score = contre_score = abstention_score = weight_sum = 0

    for theme in themes:
        tp = deputy_profile.get("theme_profiles", {}).get(theme)
        gp = group_profiles.get(group, {}).get(theme)

        if tp and tp["total_votes"] >= 5:
            personal_weight = 1 - loyalty * 0.3
            group_weight = loyalty * 0.3
            if gp:
                pour = tp["pour_rate"] * personal_weight + gp["pour_rate"] * group_weight
                contre = tp["contre_rate"] * personal_weight + gp["contre_rate"] * group_weight
                abstention = tp["abstention_rate"] * personal_weight + gp["abstention_rate"] * group_weight
            else:
                pour, contre, abstention = tp["pour_rate"], tp["contre_rate"], tp["abstention_rate"]
            w = min(tp["total_votes"] / 20, 1.0)
        elif gp:
            pour, contre, abstention = gp["pour_rate"], gp["contre_rate"], gp["abstention_rate"]
            w = 0.5
        else:
            pour = deputy_profile.get("overall_pour_rate", 0.33)
            contre = deputy_profile.get("overall_contre_rate", 0.33)
            abstention = deputy_profile.get("overall_abstention_rate", 0.33)
            w = 0.3

        pour_score += pour * w
        contre_score += contre * w
        abstention_score += abstention * w
        weight_sum += w

    if weight_sum > 0:
        pour_score /= weight_sum
        contre_score /= weight_sum
        abstention_score /= weight_sum

    total = pour_score + contre_score + abstention_score
    if total > 0:
        pour_score /= total
        contre_score /= total
        abstention_score /= total

    predicted = "pour" if pour_score >= contre_score and pour_score >= abstention_score else \
                "contre" if contre_score >= abstention_score else "abstention"

    return {
        "pour": round(pour_score, 4),
        "contre": round(contre_score, 4),
        "abstention": round(abstention_score, 4),
        "predicted_vote": predicted,
        "confidence": round(max(pour_score, contre_score, abstention_score), 4),
    }

# ─── Routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    # Read template directly to avoid Jinja caching issues
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()

@app.route("/api/deputes")
def api_deputes():
    deputes = DATA.get("deputes_enriched_v2", [])
    # Filter params
    groupe = request.args.get("groupe", "")
    search = request.args.get("search", "").lower()
    sort_by = request.args.get("sort", "nom")
    order = request.args.get("order", "asc")

    filtered = deputes
    if groupe:
        filtered = [d for d in filtered if d.get("groupe_sigle") == groupe]
    if search:
        filtered = [d for d in filtered if search in d.get("nom", "").lower() or
                    search in d.get("prenom", "").lower() or
                    search in d.get("departement", "").lower()]

    # Sort
    reverse = order == "desc"
    if sort_by == "loyaute":
        filtered.sort(key=lambda x: x.get("score_loyaute") or 0, reverse=reverse)
    elif sort_by == "participation":
        filtered.sort(key=lambda x: x.get("taux_participation") or 0, reverse=reverse)
    elif sort_by == "rebelles":
        filtered.sort(key=lambda x: x.get("votes_rebelles") or 0, reverse=reverse)
    else:
        filtered.sort(key=lambda x: x.get("nom", ""), reverse=reverse)

    return jsonify(filtered)

@app.route("/api/depute/<uid>")
def api_depute(uid):
    deputes = DATA.get("deputes_enriched_v2", [])
    dep = next((d for d in deputes if d["uid"] == uid), None)
    if not dep:
        return jsonify({"error": "Deputy not found"}), 404

    profile = DATA.get("vote_simulator_profiles", {}).get(uid, {})
    anomalies = [a for a in DATA.get("anomalies_improved", []) if a["uid"] == uid]

    return jsonify({
        "depute": dep,
        "profile": profile,
        "anomalies": anomalies[0] if anomalies else None,
    })

@app.route("/api/groupes")
def api_groupes():
    deputes = DATA.get("deputes_enriched_v2", [])
    from collections import Counter
    groups = Counter(d["groupe_sigle"] for d in deputes if d.get("groupe_sigle"))

    group_stats = {}
    for g, count in groups.items():
        members = [d for d in deputes if d.get("groupe_sigle") == g]
        loyautes = [d["score_loyaute"] for d in members if d.get("score_loyaute") is not None]
        participations = [d["taux_participation"] for d in members if d.get("taux_participation")]

        group_stats[g] = {
            "sigle": g,
            "nom": members[0].get("groupe_nom", g) if members else g,
            "nb_deputes": count,
            "loyaute_moyenne": round(sum(loyautes) / len(loyautes), 2) if loyautes else 0,
            "participation_moyenne": round(sum(participations) / len(participations), 2) if participations else 0,
        }

    return jsonify(group_stats)

@app.route("/api/simulate", methods=["POST"])
def api_simulate():
    data = request.json
    title = data.get("title", "")
    themes = data.get("themes", [])
    deputy_uid = data.get("deputy_uid", "")

    if not themes and title:
        themes = classify_themes(title)

    profiles = DATA.get("vote_simulator_profiles", {})
    group_profiles = DATA.get("group_profiles", {})

    if deputy_uid:
        profile = profiles.get(deputy_uid)
        if profile:
            result = predict_vote(profile, themes, group_profiles)
            result["deputy"] = {"nom": profile["nom"], "prenom": profile["prenom"], "groupe": profile["groupe"]}
            result["themes_detected"] = themes
            return jsonify(result)
        return jsonify({"error": "Deputy not found"}), 404

    # Simulate for all deputies
    results = []
    for uid, profile in profiles.items():
        pred = predict_vote(profile, themes, group_profiles)
        pred["uid"] = uid
        pred["nom"] = profile["nom"]
        pred["prenom"] = profile["prenom"]
        pred["groupe"] = profile["groupe"]
        results.append(pred)

    # Group summary
    from collections import Counter
    group_summary = {}
    for r in results:
        g = r["groupe"]
        if g not in group_summary:
            group_summary[g] = {"pour": 0, "contre": 0, "abstention": 0, "total": 0}
        group_summary[g][r["predicted_vote"]] += 1
        group_summary[g]["total"] += 1

    return jsonify({
        "themes_detected": themes,
        "predictions": results,
        "group_summary": group_summary,
        "total_pour": sum(1 for r in results if r["predicted_vote"] == "pour"),
        "total_contre": sum(1 for r in results if r["predicted_vote"] == "contre"),
        "total_abstention": sum(1 for r in results if r["predicted_vote"] == "abstention"),
    })

@app.route("/api/anomalies")
def api_anomalies():
    anomalies = DATA.get("anomalies_improved", [])
    groupe = request.args.get("groupe", "")
    theme = request.args.get("theme", "")
    min_z = float(request.args.get("min_z", 1.5))

    filtered = anomalies
    if groupe:
        filtered = [a for a in filtered if a["groupe"] == groupe]
    if theme:
        filtered = [a for a in filtered if any(an["theme"] == theme for an in a["anomalies"])]
    filtered = [a for a in filtered if a["max_z_score"] >= min_z]

    return jsonify(filtered[:100])

@app.route("/api/clusters")
def api_clusters():
    return jsonify(DATA.get("voting_clusters", {}))

@app.route("/api/themes")
def api_themes():
    return jsonify(list(THEMES.keys()))

@app.route("/api/stats")
def api_stats():
    deputes = DATA.get("deputes_enriched_v2", [])
    scrutins = DATA.get("scrutins_info", {})
    return jsonify({
        "total_deputes": len(deputes),
        "total_scrutins": len(scrutins),
        "groupes": len(set(d["groupe_sigle"] for d in deputes if d.get("groupe_sigle"))),
    })

if __name__ == "__main__":
    print("Starting server at http://localhost:5000")
    app.run(debug=False, host="0.0.0.0", port=5000, use_reloader=False)
