"""
Collecte COMPLETE des données de la 17e législature.
- Tous les députés (actifs + anciens)
- Tous les scrutins depuis juillet 2024
- Export en CSV séparés pour le rapport
"""

import json
import os
import io
import csv
import zipfile
import time
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from collections import defaultdict, Counter
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
CSV_DIR = os.path.join(DATA_DIR, "csv_export")
os.makedirs(CSV_DIR, exist_ok=True)

HEADERS = {"User-Agent": "DepAnalyzer/1.0 (academic research project)"}

def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = Request(url, headers=HEADERS)
            with urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  [Attempt {attempt+1}] Error: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None

def fetch_zip_json(url):
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=180) as resp:
            zip_data = io.BytesIO(resp.read())
        with zipfile.ZipFile(zip_data) as zf:
            results = []
            for name in zf.namelist():
                if name.endswith(".json"):
                    with zf.open(name) as f:
                        data = json.loads(f.read().decode("utf-8"))
                        results.append((name, data))
            return results
    except Exception as e:
        print(f"  Error fetching ZIP {url}: {e}")
        return []

def save_json(data, filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    size = os.path.getsize(path)
    print(f"  Saved {filename} ({size // 1024}KB)")

def save_csv(rows, filename, fieldnames):
    path = os.path.join(CSV_DIR, filename)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  CSV: {filename} ({len(rows)} rows)")

# ─── Known political groups for 17th legislature ───────────────────────────

ORGANE_TO_GROUPE = {}  # Will be populated from data

GROUPE_NAMES = {
    "PO845401": ("RN", "Rassemblement National"),
    "PO845407": ("EPR", "Ensemble pour la Republique"),
    "PO845413": ("LFI-NFP", "La France insoumise - Nouveau Front Populaire"),
    "PO845419": ("SOC", "Socialistes et apparentes"),
    "PO845425": ("DR", "Droite Republicaine"),
    "PO845439": ("EcoS", "Ecologiste et Social"),
    "PO845454": ("Dem", "Les Democrates"),
    "PO845470": ("HOR", "Horizons & Independants"),
    "PO845485": ("LIOT", "Libertes, Independants, Outre-mer et Territoires"),
    "PO845514": ("GDR", "Gauche Democrate et Republicaine"),
    "PO840056": ("NI", "Non inscrits"),
    "PO872880": ("UDR", "Union des droites pour la Republique"),
    "PO847173": ("NI", "Non inscrits"),
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Collect ALL deputies (historical)
# ═══════════════════════════════════════════════════════════════════════════

def collect_all_deputies():
    print("\n" + "=" * 60)
    print("  STEP 1: Collecting ALL deputies (historical + active)")
    print("=" * 60)

    # Active deputies
    print("\n  Fetching active deputies...")
    url_active = "https://data.assemblee-nationale.fr/static/openData/repository/17/amo/deputes_actifs_mandats_actifs_organes/AMO10_deputes_actifs_mandats_actifs_organes.json.zip"
    active_files = fetch_zip_json(url_active)

    # Historical (all deputies since 11th legislature)
    print("  Fetching historical deputies...")
    url_hist = "https://data.assemblee-nationale.fr/static/openData/repository/17/amo/tous_acteurs_mandats_organes_xi_legislature/AMO30_tous_acteurs_tous_mandats_tous_organes_historique.json.zip"
    hist_files = fetch_zip_json(url_hist)

    all_actors = {}

    def extract_actors(files):
        for name, data in files:
            if isinstance(data, dict):
                if "acteur" in data:
                    uid = data["acteur"].get("uid", {})
                    if isinstance(uid, dict):
                        uid = uid.get("#text", "")
                    all_actors[uid] = data["acteur"]
                elif "export" in data:
                    acteurs = data.get("export", {}).get("acteurs", {})
                    if isinstance(acteurs, dict) and "acteur" in acteurs:
                        al = acteurs["acteur"]
                        if isinstance(al, list):
                            for a in al:
                                uid = a.get("uid", {})
                                if isinstance(uid, dict):
                                    uid = uid.get("#text", "")
                                all_actors[uid] = a
                        else:
                            uid = al.get("uid", {})
                            if isinstance(uid, dict):
                                uid = uid.get("#text", "")
                            all_actors[uid] = al

    extract_actors(active_files)
    print(f"  Active: {len(all_actors)} actors")
    extract_actors(hist_files)
    print(f"  Total after historical: {len(all_actors)} actors")

    # Filter to only deputies who served in 17th legislature
    deputes_17 = {}
    for uid, actor in all_actors.items():
        mandats = actor.get("mandats", {}).get("mandat", [])
        if isinstance(mandats, dict):
            mandats = [mandats]

        for m in mandats:
            if m.get("typeOrgane") == "ASSEMBLEE" and str(m.get("legislature")) == "17":
                deputes_17[uid] = actor
                break

    print(f"  Deputies in 17th legislature: {len(deputes_17)}")
    save_json(deputes_17, "all_deputes_17_raw.json")
    return deputes_17

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: Parse deputy profiles
# ═══════════════════════════════════════════════════════════════════════════

def parse_all_deputies(raw_deputies):
    print("\n" + "=" * 60)
    print("  STEP 2: Parsing deputy profiles")
    print("=" * 60)

    import re
    deputies = []

    for uid, actor in raw_deputies.items():
        try:
            ec = actor.get("etatCivil", {})
            ident = ec.get("ident", {})
            naissance = ec.get("infoNaissance", {})

            nom = ident.get("nom", "")
            prenom = ident.get("prenom", "")
            civ = ident.get("civ", "")
            date_naissance = naissance.get("dateNais", "")
            ville_naissance = naissance.get("villeNais", "")
            if isinstance(ville_naissance, dict):
                ville_naissance = ""
            dep_naissance = naissance.get("depNais", "")
            if isinstance(dep_naissance, dict):
                dep_naissance = ""

            prof = actor.get("profession", {})
            profession = prof.get("libelleCourant", "")
            profession = re.sub(r'^\(\d+\)\s*-\s*', '', profession).strip()
            cat_socpro = prof.get("socProcINSEE", {}).get("catSocPro", "")
            fam_socpro = prof.get("socProcINSEE", {}).get("famSocPro", "")

            mandats = actor.get("mandats", {}).get("mandat", [])
            if isinstance(mandats, dict):
                mandats = [mandats]

            # Extract 17th legislature info
            departement = ""
            num_circo = ""
            region = ""
            date_debut = ""
            date_fin = ""
            groupe_ref = ""
            groupe_sigle = ""
            groupe_nom = ""
            commission = ""
            nb_mandats_depute = 0
            is_current = False

            for m in mandats:
                type_org = m.get("typeOrgane", "")
                leg = str(m.get("legislature", ""))

                if type_org == "ASSEMBLEE" and leg == "17":
                    date_debut = m.get("dateDebut", "")
                    date_fin = m.get("dateFin", "") or ""
                    if not date_fin:
                        is_current = True

                    election = m.get("election", {})
                    if election:
                        lieu = election.get("lieu", {})
                        if lieu:
                            departement = lieu.get("departement", "")
                            num_circo = lieu.get("numCirco", "")
                            region = lieu.get("region", "")

                elif type_org == "ASSEMBLEE":
                    nb_mandats_depute += 1

                elif type_org == "GP" and leg == "17":
                    org_ref = m.get("organes", {}).get("organeRef", "")
                    df = m.get("dateFin", "")
                    dd = m.get("dateDebut", "")
                    # Take current group (no dateFin) or most recent
                    if org_ref:
                        if not df or df == "None":
                            groupe_ref = org_ref
                            ginfo = GROUPE_NAMES.get(org_ref, ("", ""))
                            groupe_sigle = ginfo[0]
                            groupe_nom = ginfo[1]
                        elif not groupe_sigle:
                            # Fallback: take most recent ended group
                            groupe_ref = org_ref
                            ginfo = GROUPE_NAMES.get(org_ref, ("", ""))
                            groupe_sigle = ginfo[0]
                            groupe_nom = ginfo[1]

                elif type_org == "COMPER" and leg == "17":
                    df = m.get("dateFin", "")
                    if not df or df == "None":
                        commission = m.get("organes", {}).get("organeRef", "")

            nb_mandats_depute += 1  # Current

            deputies.append({
                "uid": uid,
                "nom": nom,
                "prenom": prenom,
                "civilite": civ,
                "date_naissance": date_naissance,
                "lieu_naissance": f"{ville_naissance}, {dep_naissance}".strip(", "),
                "departement": departement,
                "num_circonscription": num_circo,
                "region": region,
                "groupe_sigle": groupe_sigle,
                "groupe_nom": groupe_nom,
                "profession_avant_mandat": profession,
                "categorie_socpro": cat_socpro,
                "famille_socpro": fam_socpro,
                "nb_mandats_depute": nb_mandats_depute,
                "commission_permanente": commission,
                "date_debut_mandat": date_debut,
                "date_fin_mandat": date_fin,
                "en_cours": is_current,
            })
        except Exception as e:
            print(f"  Error parsing {uid}: {e}")

    deputies.sort(key=lambda x: (x["nom"], x["prenom"]))
    print(f"  Parsed {len(deputies)} deputies")
    print(f"  Currently active: {sum(1 for d in deputies if d['en_cours'])}")
    print(f"  Former: {sum(1 for d in deputies if not d['en_cours'])}")

    save_json(deputies, "all_deputes_17_parsed.json")

    # CSV export
    save_csv(deputies, "01_deputes_profils.csv", [
        "uid", "nom", "prenom", "civilite", "date_naissance", "lieu_naissance",
        "departement", "num_circonscription", "region",
        "groupe_sigle", "groupe_nom",
        "profession_avant_mandat", "categorie_socpro", "famille_socpro",
        "nb_mandats_depute", "commission_permanente",
        "date_debut_mandat", "date_fin_mandat", "en_cours"
    ])

    return deputies

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: Load scrutins (already collected)
# ═══════════════════════════════════════════════════════════════════════════

def load_and_export_scrutins():
    print("\n" + "=" * 60)
    print("  STEP 3: Loading and exporting scrutins")
    print("=" * 60)

    scrutins_raw = json.load(open(os.path.join(DATA_DIR, "scrutins_raw.json"), "r", encoding="utf-8"))
    print(f"  Loaded {len(scrutins_raw)} scrutins")

    # Parse and export
    scrutins_export = []
    for sc in scrutins_raw:
        sc_num = sc.get("numero", "")
        titre = sc.get("titre", sc.get("sort", {}).get("libelle", ""))
        date = sc.get("dateScrutin", "")
        type_vote = sc.get("typeVote", {}).get("codeTypeVote", "")
        type_lib = sc.get("typeVote", {}).get("libelleTypeVote", "")

        # Get result
        synthese = sc.get("syntheseVote", {})
        nb_pour = synthese.get("depisteVotant", {}).get("pour", "")
        nb_contre = synthese.get("depisteVotant", {}).get("contre", "")
        nb_abstention = synthese.get("depisteVotant", {}).get("abstention", "")
        nb_votants = synthese.get("nombreVotants", "")
        nb_suffrages = synthese.get("suffragesExprimes", "")

        sort_code = sc.get("sort", {}).get("code", "")

        # Also try the synthese format
        if not nb_pour:
            synth = sc.get("syntheseVote", {})
            decomp = synth.get("decompte", {})
            nb_pour = decomp.get("pour", "")
            nb_contre = decomp.get("contre", "")
            nb_abstention = decomp.get("abstentions", "")
            nb_votants = synth.get("nombreVotants", "")
            nb_suffrages = synth.get("suffragesExprimes", "")

        # Classify themes
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

    save_json(scrutins_export, "scrutins_parsed.json")
    save_csv(scrutins_export, "02_scrutins.csv", [
        "numero", "date", "titre", "theme", "type_code", "type_libelle", "resultat",
        "nb_votants", "nb_suffrages_exprimes", "nb_pour", "nb_contre", "nb_abstentions"
    ])

    # Date range
    dates = [s["date"] for s in scrutins_export if s["date"]]
    if dates:
        print(f"  Date range: {min(dates)} -> {max(dates)}")

    return scrutins_export

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: Parse ALL individual votes and export
# ═══════════════════════════════════════════════════════════════════════════

def parse_and_export_votes():
    print("\n" + "=" * 60)
    print("  STEP 4: Parsing and exporting ALL individual votes")
    print("=" * 60)

    scrutins_raw = json.load(open(os.path.join(DATA_DIR, "scrutins_raw.json"), "r", encoding="utf-8"))

    all_votes = []
    deputy_votes = defaultdict(list)

    for sc in scrutins_raw:
        sc_num = sc.get("numero", "")
        sc_date = sc.get("dateScrutin", "")
        sc_titre = sc.get("titre", "")

        groups_data = sc.get("ventilationVotes", {}).get("organe", {}).get("groupes", {}).get("groupe", [])
        if isinstance(groups_data, dict):
            groups_data = [groups_data]

        for group in groups_data:
            group_ref = group.get("organeRef", "")
            group_position = group.get("vote", {}).get("positionMajoritaire", "")

            vote_detail = group.get("vote", {}).get("decompteNominatif", {})
            if not vote_detail:
                continue

            for vote_type in ["pours", "contres", "abstentions", "nonVotants"]:
                voters = vote_detail.get(vote_type, {})
                if not voters:
                    continue
                votant_list = voters.get("votant", [])
                if isinstance(votant_list, dict):
                    votant_list = [votant_list]

                vote_label = {
                    "pours": "pour", "contres": "contre",
                    "abstentions": "abstention", "nonVotants": "nonVotant"
                }[vote_type]

                for votant in votant_list:
                    dep_ref = votant.get("acteurRef", "")
                    if dep_ref:
                        record = {
                            "depute_uid": dep_ref,
                            "scrutin_numero": sc_num,
                            "date_scrutin": sc_date,
                            "vote": vote_label,
                            "groupe_organe_ref": group_ref,
                            "position_groupe": group_position,
                        }
                        all_votes.append(record)
                        deputy_votes[dep_ref].append(record)

    print(f"  Total individual votes: {len(all_votes)}")
    print(f"  Deputies with votes: {len(deputy_votes)}")

    save_json(dict(deputy_votes), "all_deputy_votes.json")

    # Export CSV (this will be large)
    save_csv(all_votes, "03_votes_individuels.csv", [
        "depute_uid", "scrutin_numero", "date_scrutin",
        "vote", "groupe_organe_ref", "position_groupe"
    ])

    return deputy_votes

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: Compute participation
# ═══════════════════════════════════════════════════════════════════════════

def compute_participation(deputies, deputy_votes, total_scrutins):
    print("\n" + "=" * 60)
    print("  STEP 5: Computing participation rates")
    print("=" * 60)

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
    save_json(participation, "participation_complete.json")
    save_csv(participation, "04_participation.csv", [
        "uid", "nom", "prenom", "groupe_sigle",
        "scrutins_votes", "scrutins_present", "total_scrutins_legislature",
        "taux_participation_pct", "taux_presence_pct", "en_cours"
    ])

    return {p["uid"]: p for p in participation}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6: Compute loyalty scores
# ═══════════════════════════════════════════════════════════════════════════

def compute_loyalty(deputies, deputy_votes):
    print("\n" + "=" * 60)
    print("  STEP 6: Computing loyalty scores")
    print("=" * 60)

    loyalty = []
    for dep in deputies:
        uid = dep["uid"]
        votes = deputy_votes.get(uid, [])

        total = 0
        loyal = 0
        rebel_count = 0
        rebel_examples = []

        for v in votes:
            if v["vote"] == "nonVotant":
                continue
            total += 1
            pos = v["position_groupe"].lower()
            vote = v["vote"].lower()

            is_loyal = (pos == vote) or (pos == "liberte de vote") or (pos == "liberté de vote")

            if is_loyal:
                loyal += 1
            else:
                rebel_count += 1
                if len(rebel_examples) < 5:
                    rebel_examples.append({
                        "scrutin": v["scrutin_numero"],
                        "date": v["date_scrutin"],
                        "vote_depute": v["vote"],
                        "position_groupe": v["position_groupe"],
                    })

        score = round(loyal / total * 100, 2) if total > 0 else None

        loyalty.append({
            "uid": uid,
            "nom": dep["nom"],
            "prenom": dep["prenom"],
            "groupe_sigle": dep["groupe_sigle"],
            "total_votes_exprimes": total,
            "votes_loyaux": loyal,
            "votes_rebelles": rebel_count,
            "score_loyaute_pct": score,
            "en_cours": dep["en_cours"],
        })

    loyalty.sort(key=lambda x: x["score_loyaute_pct"] or 0, reverse=True)
    save_json(loyalty, "loyalty_complete.json")
    save_csv(loyalty, "05_loyaute.csv", [
        "uid", "nom", "prenom", "groupe_sigle",
        "total_votes_exprimes", "votes_loyaux", "votes_rebelles",
        "score_loyaute_pct", "en_cours"
    ])

    return {l["uid"]: l for l in loyalty}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7: Compute thematic specialties
# ═══════════════════════════════════════════════════════════════════════════

import re as _re

# Keywords use REGEX patterns with word boundaries (\b) to avoid false positives
# e.g. "art" no longer matches "article", "peche" no longer matches "empeche"
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
    import unicodedata
    t = unicodedata.normalize('NFD', title.lower())
    t = ''.join(c for c in t if unicodedata.category(c) != 'Mn')
    matched = []
    for theme, patterns in THEMES.items():
        for pat in patterns:
            if _re.search(pat, t):
                matched.append(theme)
                break
    return matched

def compute_themes(deputies, deputy_votes, scrutins_info):
    print("\n" + "=" * 60)
    print("  STEP 7: Computing thematic specialties")
    print("=" * 60)

    # Build scrutin title lookup
    scrut_titles = {}
    for sc in scrutins_info:
        scrut_titles[str(sc.get("numero", ""))] = sc.get("titre", "")

    themes_data = []
    for dep in deputies:
        uid = dep["uid"]
        votes = deputy_votes.get(uid, [])

        theme_count = Counter()
        theme_votes = defaultdict(lambda: {"pour": 0, "contre": 0, "abstention": 0, "total": 0})

        for v in votes:
            if v["vote"] == "nonVotant":
                continue
            title = scrut_titles.get(str(v["scrutin_numero"]), "")
            matched_themes = classify_theme(title)

            for theme in matched_themes:
                theme_count[theme] += 1
                theme_votes[theme][v["vote"]] += 1
                theme_votes[theme]["total"] += 1

        top5 = theme_count.most_common(5)
        specialite = top5[0][0] if top5 else ""

        themes_data.append({
            "uid": uid,
            "nom": dep["nom"],
            "prenom": dep["prenom"],
            "groupe_sigle": dep["groupe_sigle"],
            "specialite_principale": specialite,
            "top_5_themes": " | ".join(f"{t}({c})" for t, c in top5),
            "nb_themes_votes": len(theme_count),
        })

    save_json(themes_data, "themes_complete.json")
    save_csv(themes_data, "06_specialites_thematiques.csv", [
        "uid", "nom", "prenom", "groupe_sigle",
        "specialite_principale", "top_5_themes", "nb_themes_votes"
    ])

    return themes_data

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8: Anomaly detection (improved)
# ═══════════════════════════════════════════════════════════════════════════

def detect_anomalies_improved(deputies, deputy_votes, scrutins_info):
    print("\n" + "=" * 60)
    print("  STEP 8: Detecting thematic anomalies (improved)")
    print("=" * 60)

    import numpy as np

    scrut_titles = {}
    for sc in scrutins_info:
        scrut_titles[str(sc.get("numero", ""))] = sc.get("titre", "")

    # Build per-deputy, per-theme vote profiles
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
            if s["total"] >= 20:  # Minimum 20 votes (improved threshold)
                profiles[theme] = {
                    "pour_rate": s["pour"] / s["total"],
                    "contre_rate": s["contre"] / s["total"],
                    "total": s["total"],
                }
        dep_theme_profiles[uid] = {
            "dep": dep,
            "profiles": profiles,
        }

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

    # Detect anomalies
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

            if max_z > 2.0:  # Increased threshold
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
            anomalies.append({
                "uid": uid,
                "nom": dep["nom"],
                "prenom": dep["prenom"],
                "groupe": dep["groupe_sigle"],
                "loyalty_global": None,  # Will be filled later
                "anomalies": dep_anomalies,
                "max_z_score": dep_anomalies[0]["z_score"],
                "nb_themes_atypiques": len(dep_anomalies),
            })

    anomalies.sort(key=lambda x: x["max_z_score"], reverse=True)

    # Export CSV (flattened)
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

    print(f"  Anomalies detected: {len(anomalies)} deputies")
    print(f"  Total atypical theme entries: {len(csv_rows)}")

    return anomalies

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9: Build final enriched dataset
# ═══════════════════════════════════════════════════════════════════════════

def build_final(deputies, participation_map, loyalty_map, themes_data, scrutins_info):
    print("\n" + "=" * 60)
    print("  STEP 9: Building final enriched dataset")
    print("=" * 60)

    themes_map = {t["uid"]: t for t in themes_data}
    total_scrutins = len(scrutins_info)

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
            "score_loyaute": loy.get("score_loyaute_pct"),
            "votes_loyaux": loy.get("votes_loyaux", 0),
            "votes_rebelles": loy.get("votes_rebelles", 0),
            "total_votes_exprimes": loy.get("total_votes_exprimes", 0),
            "specialite_principale": thm.get("specialite_principale", ""),
            "top_5_themes": thm.get("top_5_themes", ""),
        }
        final.append(entry)

    save_json(final, "deputes_enriched_v2.json")
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

    # Stats
    print(f"\n  FINAL STATS:")
    print(f"  Total deputes: {len(final)}")
    print(f"  En cours: {sum(1 for d in final if d['en_cours'])}")
    print(f"  Anciens: {sum(1 for d in final if not d['en_cours'])}")
    print(f"  Avec votes: {sum(1 for d in final if d['scrutins_votes'] > 0)}")
    print(f"  Total scrutins: {total_scrutins}")

    return final

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  COLLECTE COMPLETE - 17e LEGISLATURE")
    print("  Depuis juillet 2024 jusqu'a aujourd'hui")
    print("=" * 60)

    # Step 1: All deputies
    raw_deps = collect_all_deputies()

    # Step 2: Parse
    deputies = parse_all_deputies(raw_deps)

    # Step 3: Scrutins
    scrutins_info = load_and_export_scrutins()

    # Step 4: Individual votes
    deputy_votes = parse_and_export_votes()

    # Step 5: Participation
    participation_map = compute_participation(deputies, deputy_votes, len(scrutins_info))

    # Step 6: Loyalty
    loyalty_map = compute_loyalty(deputies, deputy_votes)

    # Step 7: Themes
    themes_data = compute_themes(deputies, deputy_votes, scrutins_info)

    # Step 8: Anomalies (improved)
    anomalies = detect_anomalies_improved(deputies, deputy_votes, scrutins_info)

    # Fill loyalty in anomalies
    for a in anomalies:
        loy = loyalty_map.get(a["uid"], {})
        a["loyalty_global"] = loy.get("score_loyaute_pct")
    save_json(anomalies, "anomalies_improved.json")

    # Step 9: Final dataset
    final = build_final(deputies, participation_map, loyalty_map, themes_data, scrutins_info)

    print(f"\n{'=' * 60}")
    print(f"  ALL CSV FILES EXPORTED TO: {CSV_DIR}")
    print(f"{'=' * 60}")
    for f in sorted(os.listdir(CSV_DIR)):
        size = os.path.getsize(os.path.join(CSV_DIR, f))
        print(f"  {f} ({size // 1024}KB)")

if __name__ == "__main__":
    main()
