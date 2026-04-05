"""
Recalcule TOUS les CSV a partir des donnees JSON deja telechargees.
Ne retelecharge rien. Corrige les bugs de classification thematique.
"""

import json
import os
import csv
import re
import unicodedata
import numpy as np
from collections import defaultdict, Counter

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
CSV_DIR = os.path.join(DATA_DIR, "csv_export")
os.makedirs(CSV_DIR, exist_ok=True)


def save_csv(rows, filename, fieldnames):
    path = os.path.join(CSV_DIR, filename)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  CSV: {filename} ({len(rows)} rows, {os.path.getsize(path) // 1024}KB)")


def save_json(data, filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Theme classification (FIXED - no more "art" matching "article") ────────

THEMES = {
    "Economie & Finance": [r"\bbudget", r"\bfinanc", r"\bfiscal", r"\bimpot", r"\btaxe", r"\beconom", r"\bdette", r"\bdeficit", r"\bbanque", r"\bcommerce\b", r"\bentreprise", r"\bnationalisation", r"\bcomptes?\s+bancaire"],
    "Securite & Justice": [r"\bjustice\b", r"\bpenal", r"\bpolice\b", r"\bprison", r"\bdelinquanc", r"\bterroris", r"\bgendarm", r"\bcriminel", r"\bnarco", r"\bdetention", r"\bemprisonnement", r"\bpeine", r"\binfraction", r"\bcondamn"],
    "Sante": [r"\bsante\b", r"\bmedic", r"\bhopital", r"\bsoins\b", r"\bpandemi", r"\bvaccin", r"\bpharma", r"\bmaladie", r"\baidant", r"\baide a mourir", r"\bfin de vie", r"\beuthanasi", r"\bbioethique"],
    "Education": [r"\beducation\b", r"\becole\b", r"\buniversite", r"\benseignement", r"\betudiant", r"\bformation\b", r"\brecherche\b", r"\bscolaire", r"\bmineur"],
    "Ecologie & Environnement": [r"\becolog", r"\benvironnement", r"\bclimat", r"\benergi", r"\bcarbone", r"\bbiodiversite", r"\bpollution", r"\brenouvelable", r"\bnucleaire", r"\beau\b"],
    "Social & Travail": [r"\btravail", r"\bemploi", r"\bretraite", r"\bchomage", r"\bsalaire", r"\b[r]sa\b", r"\bsolidarite", r"\bprotection sociale", r"\bhandicap"],
    "Immigration": [r"\bimmigration", r"\bmigrat", r"\basile\b", r"\betranger", r"\bintegration\b", r"\bnaturalisation", r"\bfrontiere", r"\bretention"],
    "Agriculture": [r"\bagricul", r"\bagricole", r"\bpaysan", r"\balimenta", r"\brural", r"\belevage", r"\bpesticide", r"\bfoncier", r"\bmercosur"],
    "Defense": [r"\bdefense\b", r"\barmee", r"\bmilitaire", r"\botan\b", r"\barmement"],
    "Numerique & Technologies": [r"\bnumerique", r"\btechnolog", r"\bintelligence artificielle", r"\bdonnees personnelles", r"\bcyber", r"\bdigital"],
    "Logement & Urbanisme": [r"\blogement", r"\bimmobilier", r"\burbanis", r"\bhabitat", r"\bhlm\b", r"\bloyer", r"\blocatif"],
    "Transport": [r"\btransport", r"\bmobilite", r"\bferroviaire", r"\bsncf\b", r"\bautoroute", r"\baerien"],
    "Culture": [r"\bculture\b", r"\bculturel", r"\bpatrimoine", r"\baudiovisuel", r"\bmusee", r"\bspectacle", r"\bsport", r"\bartist", r"\bcinema", r"\blivre\b", r"\bolympi"],
    "Outre-mer": [r"\boutre-mer", r"\bultramarin", r"\bpolynesie", r"\bnouvelle-caledonie", r"\bguadeloupe", r"\bmartinique", r"\bguyane", r"\bmayotte"],
    "Institutions": [r"\bconstitution", r"\binstitution", r"\belection", r"\breferendum", r"\bdecentralis", r"\bsenat", r"\belu local", r"\belu.e.s?\s+loc", r"\bmotion de censure", r"\bscrutin", r"\bassemblee nationale"],
    "Securite sociale & Protection": [r"\bsecurite sociale", r"\bsecu\b", r"\bcotisation", r"\bplfss"],
}


def classify_theme(title):
    """Classify a scrutin title into themes using regex word-boundary matching."""
    t = unicodedata.normalize('NFD', title.lower())
    t = ''.join(c for c in t if unicodedata.category(c) != 'Mn')
    matched = []
    for theme, patterns in THEMES.items():
        for pat in patterns:
            if re.search(pat, t):
                matched.append(theme)
                break
    return matched


def main():
    print("=" * 60)
    print("  RECALCUL DES CSV (sans re-telechargement)")
    print("=" * 60)

    # ─── Load cached data ──────────────────────────────────────────────────
    print("\n> Chargement des donnees en cache...")
    deputies = json.load(open(os.path.join(DATA_DIR, "all_deputes_17_parsed.json"), "r", encoding="utf-8"))
    print(f"  {len(deputies)} deputes")

    scrutins_raw = json.load(open(os.path.join(DATA_DIR, "scrutins_raw.json"), "r", encoding="utf-8"))
    print(f"  {len(scrutins_raw)} scrutins bruts")

    deputy_votes = json.load(open(os.path.join(DATA_DIR, "all_deputy_votes.json"), "r", encoding="utf-8"))
    print(f"  {len(deputy_votes)} deputes avec votes")

    # ─── 01: Profils (inchange) ────────────────────────────────────────────
    print("\n> 01_deputes_profils.csv (inchange)")
    save_csv(deputies, "01_deputes_profils.csv", [
        "uid", "nom", "prenom", "civilite", "date_naissance", "lieu_naissance",
        "departement", "num_circonscription", "region",
        "groupe_sigle", "groupe_nom",
        "profession_avant_mandat", "categorie_socpro", "famille_socpro",
        "nb_mandats_depute", "commission_permanente",
        "date_debut_mandat", "date_fin_mandat", "en_cours"
    ])

    # ─── 02: Scrutins (FIXED: avec colonne theme) ─────────────────────────
    print("\n> 02_scrutins.csv (CORRIGE: ajout colonne theme)")
    scrutins_export = []
    for sc in scrutins_raw:
        sc_num = sc.get("numero", "")
        titre = sc.get("titre", sc.get("sort", {}).get("libelle", ""))
        date = sc.get("dateScrutin", "")
        type_vote = sc.get("typeVote", {}).get("codeTypeVote", "")
        type_lib = sc.get("typeVote", {}).get("libelleTypeVote", "")
        synthese = sc.get("syntheseVote", {})
        nb_pour = synthese.get("depisteVotant", {}).get("pour", "")
        nb_contre = synthese.get("depisteVotant", {}).get("contre", "")
        nb_abstention = synthese.get("depisteVotant", {}).get("abstention", "")
        nb_votants = synthese.get("nombreVotants", "")
        nb_suffrages = synthese.get("suffragesExprimes", "")
        sort_code = sc.get("sort", {}).get("code", "")
        if not nb_pour:
            decomp = synthese.get("decompte", {})
            nb_pour = decomp.get("pour", "")
            nb_contre = decomp.get("contre", "")
            nb_abstention = decomp.get("abstentions", "")
            nb_votants = synthese.get("nombreVotants", "")
            nb_suffrages = synthese.get("suffragesExprimes", "")

        themes_list = classify_theme(titre)
        theme_str = " | ".join(themes_list) if themes_list else "Autre"

        scrutins_export.append({
            "numero": sc_num,
            "date": date,
            "titre": titre,
            "theme": theme_str,
            "type_code": type_vote,
            "type_libelle": type_lib,
            "resultat": sort_code,
            "nb_votants": nb_votants,
            "nb_suffrages_exprimes": nb_suffrages,
            "nb_pour": nb_pour,
            "nb_contre": nb_contre,
            "nb_abstentions": nb_abstention,
        })

    scrutins_export.sort(key=lambda x: x.get("date", ""))
    save_csv(scrutins_export, "02_scrutins.csv", [
        "numero", "date", "titre", "theme", "type_code", "type_libelle", "resultat",
        "nb_votants", "nb_suffrages_exprimes", "nb_pour", "nb_contre", "nb_abstentions"
    ])

    # Build title lookup for themes
    scrut_titles = {}
    for sc in scrutins_raw:
        scrut_titles[str(sc.get("numero", ""))] = sc.get("titre", "")

    total_scrutins = len(scrutins_raw)

    # ─── 03: Votes individuels (inchange) ──────────────────────────────────
    print("\n> 03_votes_individuels.csv (skip - inchange, 45MB)")

    # ─── 04: Participation ─────────────────────────────────────────────────
    print("\n> 04_participation.csv")
    participation = []
    for dep in deputies:
        uid = dep["uid"]
        votes = deputy_votes.get(uid, [])
        actual_votes = sum(1 for v in votes if v["vote"] != "nonVotant")
        total_present = len(votes)
        participation.append({
            "uid": uid,
            "nom": dep["nom"],
            "prenom": dep["prenom"],
            "groupe_sigle": dep["groupe_sigle"],
            "scrutins_votes": actual_votes,
            "scrutins_present": total_present,
            "total_scrutins_legislature": total_scrutins,
            "taux_participation_pct": round(actual_votes / total_scrutins * 100, 2) if total_scrutins > 0 else 0,
            "taux_presence_pct": round(total_present / total_scrutins * 100, 2) if total_scrutins > 0 else 0,
            "en_cours": dep["en_cours"],
        })
    participation.sort(key=lambda x: x["taux_participation_pct"], reverse=True)
    save_csv(participation, "04_participation.csv", [
        "uid", "nom", "prenom", "groupe_sigle",
        "scrutins_votes", "scrutins_present", "total_scrutins_legislature",
        "taux_participation_pct", "taux_presence_pct", "en_cours"
    ])
    participation_map = {p["uid"]: p for p in participation}

    # ─── 05: Loyaute ──────────────────────────────────────────────────────
    print("\n> 05_loyaute.csv")
    loyalty = []
    for dep in deputies:
        uid = dep["uid"]
        votes = deputy_votes.get(uid, [])
        total = 0
        loyal = 0
        rebel_count = 0
        for v in votes:
            if v["vote"] == "nonVotant":
                continue
            total += 1
            pos = v["position_groupe"].lower()
            vote = v["vote"].lower()
            is_loyal = (pos == vote) or ("liberte" in pos)
            if is_loyal:
                loyal += 1
            else:
                rebel_count += 1
        score = round(loyal / total * 100, 2) if total > 0 else None
        loyalty.append({
            "uid": uid,
            "nom": dep["nom"],
            "prenom": dep["prenom"],
            "groupe_sigle": dep["groupe_sigle"],
            "total_votes_exprimes": total,
            "votes_loyaux": loyal,
            "votes_rebelles": rebel_count,
            "score_loyaute_pct": score if score is not None else "",
            "en_cours": dep["en_cours"],
        })
    loyalty.sort(key=lambda x: float(x["score_loyaute_pct"]) if x["score_loyaute_pct"] != "" else 0, reverse=True)
    save_csv(loyalty, "05_loyaute.csv", [
        "uid", "nom", "prenom", "groupe_sigle",
        "total_votes_exprimes", "votes_loyaux", "votes_rebelles",
        "score_loyaute_pct", "en_cours"
    ])
    loyalty_map = {l["uid"]: l for l in loyalty}

    # ─── 06: Specialites thematiques (FIXED - TF-IDF weighting) ─────────
    print("\n> 06_specialites_thematiques.csv (CORRIGE: TF-IDF pour specialite)")
    import math

    # First pass: compute raw theme counts per deputy
    deputy_theme_counts = {}  # uid -> Counter of themes
    global_theme_counts = Counter()  # total votes per theme across all deputies
    deputy_total_votes = {}  # uid -> total expressed votes

    for dep in deputies:
        uid = dep["uid"]
        votes = deputy_votes.get(uid, [])
        theme_count = Counter()
        total = 0
        for v in votes:
            if v["vote"] == "nonVotant":
                continue
            total += 1
            title = scrut_titles.get(str(v["scrutin_numero"]), "")
            matched_themes = classify_theme(title)
            for theme in matched_themes:
                theme_count[theme] += 1
        deputy_theme_counts[uid] = theme_count
        deputy_total_votes[uid] = total
        for t, c in theme_count.items():
            global_theme_counts[t] += c

    # Compute global proportions (how much each theme represents overall)
    total_global = sum(global_theme_counts.values())
    global_proportions = {t: c / total_global for t, c in global_theme_counts.items()} if total_global > 0 else {}

    # Second pass: compute TF-IDF specialties
    themes_data = []
    for dep in deputies:
        uid = dep["uid"]
        tc = deputy_theme_counts.get(uid, Counter())
        dep_total = deputy_total_votes.get(uid, 0)

        if dep_total == 0:
            themes_data.append({
                "uid": uid, "nom": dep["nom"], "prenom": dep["prenom"],
                "groupe_sigle": dep["groupe_sigle"],
                "specialite_principale": "", "top_5_themes": "", "nb_themes_votes": 0,
            })
            continue

        # TF-IDF: (deputy_proportion / global_proportion)
        # A deputy specialized in Defense votes proportionally MORE on Defense than the average
        tfidf_scores = {}
        for theme, count in tc.items():
            dep_prop = count / dep_total  # proportion of this deputy's votes on this theme
            global_prop = global_proportions.get(theme, 0.001)
            tfidf_scores[theme] = dep_prop / global_prop  # overrepresentation ratio

        # Sort by TF-IDF score (highest overrepresentation first)
        sorted_themes = sorted(tfidf_scores.items(), key=lambda x: -x[1])
        top5_tfidf = sorted_themes[:5]

        # Specialty = theme with highest overrepresentation (min 10 votes on that theme)
        specialite = ""
        for theme, score in sorted_themes:
            if tc[theme] >= 10:
                specialite = theme
                break

        # top_5_themes still shows raw counts for readability
        top5_raw = tc.most_common(5)
        themes_data.append({
            "uid": uid,
            "nom": dep["nom"],
            "prenom": dep["prenom"],
            "groupe_sigle": dep["groupe_sigle"],
            "specialite_principale": specialite,
            "top_5_themes": " | ".join(f"{t}({c})" for t, c in top5_raw),
            "nb_themes_votes": len(tc),
        })

    save_csv(themes_data, "06_specialites_thematiques.csv", [
        "uid", "nom", "prenom", "groupe_sigle",
        "specialite_principale", "top_5_themes", "nb_themes_votes"
    ])

    # ─── 07: Anomalies (FIXED) ────────────────────────────────────────────
    print("\n> 07_profils_atypiques.csv (CORRIGE: themes corriges)")

    dep_theme_profiles = {}
    for dep in deputies:
        uid = dep["uid"]
        votes = deputy_votes.get(uid, [])
        if not votes:
            continue
        theme_stats = defaultdict(lambda: {"pour": 0, "contre": 0, "abstention": 0, "total": 0})
        for v in votes:
            if v["vote"] == "nonVotant":
                continue
            title = scrut_titles.get(str(v["scrutin_numero"]), "")
            for theme in classify_theme(title):
                theme_stats[theme][v["vote"]] += 1
                theme_stats[theme]["total"] += 1
        profiles = {}
        for theme, s in theme_stats.items():
            if s["total"] >= 20:
                profiles[theme] = {
                    "pour_rate": s["pour"] / s["total"],
                    "contre_rate": s["contre"] / s["total"],
                    "total": s["total"],
                }
        dep_theme_profiles[uid] = {"dep": dep, "profiles": profiles}

    # Group averages
    group_avgs = defaultdict(lambda: defaultdict(lambda: {"pour_rates": [], "contre_rates": []}))
    for uid, data in dep_theme_profiles.items():
        group = data["dep"]["groupe_sigle"]
        for theme, p in data["profiles"].items():
            group_avgs[group][theme]["pour_rates"].append(p["pour_rate"])
            group_avgs[group][theme]["contre_rates"].append(p["contre_rate"])

    group_stats = {}
    for group, themes in group_avgs.items():
        group_stats[group] = {}
        for theme, data in themes.items():
            if len(data["pour_rates"]) >= 5:
                group_stats[group][theme] = {
                    "pour_mean": np.mean(data["pour_rates"]),
                    "pour_std": max(np.std(data["pour_rates"]), 0.01),
                    "contre_mean": np.mean(data["contre_rates"]),
                    "contre_std": max(np.std(data["contre_rates"]), 0.01),
                    "n": len(data["pour_rates"]),
                }

    anomalies = []
    for uid, data in dep_theme_profiles.items():
        dep = data["dep"]
        group = dep["groupe_sigle"]
        gs = group_stats.get(group, {})
        dep_anomalies = []
        for theme, p in data["profiles"].items():
            ts = gs.get(theme)
            if not ts:
                continue
            z_pour = abs(p["pour_rate"] - ts["pour_mean"]) / ts["pour_std"]
            z_contre = abs(p["contre_rate"] - ts["contre_mean"]) / ts["contre_std"]
            max_z = max(z_pour, z_contre)
            if max_z > 2.0:
                direction = ""
                if p["pour_rate"] > ts["pour_mean"] + ts["pour_std"]:
                    direction = f"vote POUR plus que son groupe ({p['pour_rate']:.0%} vs {ts['pour_mean']:.0%})"
                elif p["contre_rate"] > ts["contre_mean"] + ts["contre_std"]:
                    direction = f"vote CONTRE plus que son groupe ({p['contre_rate']:.0%} vs {ts['contre_mean']:.0%})"
                elif p["pour_rate"] < ts["pour_mean"] - ts["pour_std"]:
                    direction = f"vote POUR moins que son groupe ({p['pour_rate']:.0%} vs {ts['pour_mean']:.0%})"
                if direction:
                    dep_anomalies.append({
                        "theme": theme,
                        "z_score": round(max_z, 2),
                        "direction": direction,
                        "deputy_pour_rate": round(p["pour_rate"], 4),
                        "group_pour_mean": round(ts["pour_mean"], 4),
                        "deputy_contre_rate": round(p["contre_rate"], 4),
                        "group_contre_mean": round(ts["contre_mean"], 4),
                        "votes_on_theme": p["total"],
                    })
        if dep_anomalies:
            dep_anomalies.sort(key=lambda x: x["z_score"], reverse=True)
            loy = loyalty_map.get(uid, {})
            anomalies.append({
                "uid": uid,
                "nom": dep["nom"],
                "prenom": dep["prenom"],
                "groupe": dep["groupe_sigle"],
                "loyalty_global": loy.get("score_loyaute_pct", ""),
                "anomalies": dep_anomalies,
                "max_z_score": dep_anomalies[0]["z_score"],
                "nb_themes_atypiques": len(dep_anomalies),
            })

    anomalies.sort(key=lambda x: x["max_z_score"], reverse=True)

    csv_rows = []
    for a in anomalies:
        for an in a["anomalies"]:
            csv_rows.append({
                "uid": a["uid"],
                "nom": a["nom"],
                "prenom": a["prenom"],
                "groupe": a["groupe"],
                "theme": an["theme"],
                "z_score": an["z_score"],
                "direction": an["direction"],
                "taux_pour_depute": an["deputy_pour_rate"],
                "taux_pour_groupe": an["group_pour_mean"],
                "taux_contre_depute": an["deputy_contre_rate"],
                "taux_contre_groupe": an["group_contre_mean"],
                "nb_votes_theme": an["votes_on_theme"],
            })

    save_json(anomalies, "anomalies_improved.json")
    save_csv(csv_rows, "07_profils_atypiques.csv", [
        "uid", "nom", "prenom", "groupe", "theme", "z_score",
        "direction", "taux_pour_depute", "taux_pour_groupe",
        "taux_contre_depute", "taux_contre_groupe", "nb_votes_theme"
    ])

    print(f"  {len(anomalies)} deputes avec anomalies, {len(csv_rows)} entrees")

    # ─── 00: Dataset complet (FIXED) ──────────────────────────────────────
    print("\n> 00_dataset_complet.csv (CORRIGE)")
    themes_map = {t["uid"]: t for t in themes_data}
    final = []
    for dep in deputies:
        uid = dep["uid"]
        part = participation_map.get(uid, {})
        loy = loyalty_map.get(uid, {})
        thm = themes_map.get(uid, {})
        entry = {
            **dep,
            "scrutins_votes": part.get("scrutins_votes", 0),
            "total_scrutins": total_scrutins,
            "taux_participation": part.get("taux_participation_pct", 0),
            "score_loyaute": loy.get("score_loyaute_pct", ""),
            "votes_loyaux": loy.get("votes_loyaux", 0),
            "votes_rebelles": loy.get("votes_rebelles", 0),
            "total_votes_exprimes": loy.get("total_votes_exprimes", 0),
            "specialite_principale": thm.get("specialite_principale", ""),
            "top_5_themes": thm.get("top_5_themes", ""),
        }
        final.append(entry)
    save_csv(final, "00_dataset_complet.csv", [
        "uid", "nom", "prenom", "civilite", "date_naissance", "lieu_naissance",
        "departement", "num_circonscription", "region",
        "groupe_sigle", "groupe_nom",
        "profession_avant_mandat", "categorie_socpro", "famille_socpro",
        "nb_mandats_depute", "commission_permanente",
        "date_debut_mandat", "date_fin_mandat", "en_cours",
        "scrutins_votes", "total_scrutins", "taux_participation",
        "score_loyaute", "votes_loyaux", "votes_rebelles", "total_votes_exprimes",
        "specialite_principale", "top_5_themes"
    ])

    # ─── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  RECALCUL TERMINE")
    print(f"{'=' * 60}")
    for f in sorted(os.listdir(CSV_DIR)):
        size = os.path.getsize(os.path.join(CSV_DIR, f))
        print(f"  {f} ({size // 1024}KB)")


if __name__ == "__main__":
    main()
