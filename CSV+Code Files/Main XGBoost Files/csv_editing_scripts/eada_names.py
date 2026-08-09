"""Team-identity mapping between the combined dataset and the EADA survey files.

The combined dataset identifies a program three ways: `team` (the FanGraphs acronym),
`team_old` (the NCAA-ish short name) and `Full Name_team` (the institutional name).
EADA identifies an institution by IPEDS `unitid` / `institution_name`.

The join key here is the **acronym**, not `team_old`, for two reasons:
  1. It is 1:1 with the program (all 311 acronyms map to exactly one team_old / full name).
  2. `team_old` had swapped labels for Indiana/Indiana State and Appalachian State/Coppin
     State until fix_swapped_team_identities.py corrected them. The acronym was right in
     every case, so keying on it is the more robust choice regardless.

Resolution order for each acronym:
  EADA_NAME_OVERRIDES  ->  normalized `Full Name_team`  ->  IPEDS unitid

Once an acronym resolves to a `unitid`, every year joins on that id, so mid-period renames
are free (Dixie State -> Utah Tech, Houston Baptist -> Houston Christian, Iona College ->
Iona University, Canisius College -> Canisius University, St. Peter's College -> University).
"""

# Acronyms with no EADA record at all. The service academies do not participate in
# Title IV federal student aid, so they are not required to file an EADA survey and
# never appear in the data. Their EADA columns stay NaN, which XGBoost handles natively.
MISSING_FROM_EADA = {
    "AFA": "United States Air Force Academy - no Title IV participation, never files EADA",
    "ARMY": "United States Military Academy - no Title IV participation, never files EADA",
    "NAVY": "United States Naval Academy - no Title IV participation, never files EADA",
}

# Acronym -> exact EADA `institution_name`. Only programs whose normalized `Full Name_team`
# does not land on the right IPEDS record need an entry here; the other ~270 match directly.
#
# Categories:
#   - IPEDS campus qualifiers ("-Main Campus", "-Twin Cities", "-Norman Campus")
#   - IPEDS legal names ("Louisiana State University and Agricultural & Mechanical College")
#   - system schools where the short name is ambiguous ("Illinois" -> Urbana-Champaign)
#   - Cal State / UC naming ("Long Beach State" -> "California State University-Long Beach")
#   - two outright errors in the project's own team table (see IU / PORT below)
EADA_NAME_OVERRIDES = {
    # --- IPEDS campus qualifiers on flagship names ---
    "ILL": "University of Illinois Urbana-Champaign",
    "MICH": "University of Michigan-Ann Arbor",
    "MINN": "University of Minnesota-Twin Cities",
    "MIZ": "University of Missouri-Columbia",
    "MD": "University of Maryland-College Park",
    "IU": "Indiana University-Bloomington",
    "OU": "University of Oklahoma-Norman Campus",
    "PITT": "University of Pittsburgh-Pittsburgh Campus",
    "PSU": "Pennsylvania State University-Main Campus",
    "RUTG": "Rutgers University-New Brunswick",
    "SC": "University of South Carolina-Columbia",
    "WASH": "University of Washington-Seattle Campus",
    "KENT": "Kent State University at Kent",
    "SIU": "Southern Illinois University-Carbondale",
    "M-OH": "Miami University-Oxford",
    "FDU": "Fairleigh Dickinson University-Metropolitan Campus",
    # --- IPEDS legal names ---
    "LSU": "Louisiana State University and Agricultural & Mechanical College",
    "TA&M": "Texas A & M University-College Station",
    "FAMU": "Florida Agricultural and Mechanical University",
    "VT": "Virginia Polytechnic Institute and State University",
    "TNTC": "Tennessee Technological University",
    "TULN": "Tulane University of Louisiana",
    "NWST": "Northwestern State University of Louisiana",
    "COLU": "Columbia University in the City of New York",
    "SJU": "St. John's University-New York",
    "SPU": "Saint Peter's University",
    "W&M": "William & Mary",
    "UALB": "SUNY at Albany",
    "NCSU": "North Carolina State University at Raleigh",
    # --- Cal State / Cal Poly / UC naming ---
    "CP": "California Polytechnic State University-San Luis Obispo",
    "LBSU": "California State University-Long Beach",
    "FRES": "California State University-Fresno",
    "SAC": "California State University-Sacramento",
    # --- other short-name expansions ---
    "KSU": "Kansas State University",
    "ASU": "Arizona State University Campus Immersion",
    "MOST": "Missouri State University-Springfield",
    "LR": "University of Arkansas at Little Rock",
    # Pinned rather than left to the automatic match: PORT pointed at "Portland State
    # University" until fix_swapped_team_identities.py corrected it, and Portland State has
    # not sponsored baseball since 1993. Being explicit keeps a past bug from resurfacing.
    "PORT": "University of Portland",
}

# Acronym -> IPEDS unitid, for the cases where the EADA `institution_name` is not unique.
# Checked against `state_cd` in the source files.
EADA_UNITID_OVERRIDES = {
    # Two "University of St Thomas" records: 174914 (MN, D-I since 2021-22) and
    # 227863 (TX, D-III). The FanGraphs STMN program is the Minnesota one.
    "STMN": 174914,
    # "University of the Pacific" (120883, CA, D-I) vs "Pacific University" (209612, OR,
    # D-III) both fold to "pacific" once the stopwords are dropped.
    "PAC": 120883,
}
