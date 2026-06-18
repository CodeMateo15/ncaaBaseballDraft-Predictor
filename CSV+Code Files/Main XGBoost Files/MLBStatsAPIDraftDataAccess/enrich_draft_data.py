import os
import json
import time
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

FINAL_INPUT   = "./data cleaning/final_converted_schema.csv"
CLEANED_1216  = "./data cleaning/cleaned_df.csv"
CLEANED_SIGN  = "./data cleaning/mlbdraft2012_2016.csv"   # has Signed (character) for 2012–2016
OUTPUT_CSV    = "./data cleaning/final_converted_schema_enriched.csv"
CACHE_DIR     = "./data cleaning/cache"

STATSAPI_DRAFT_URL = "https://statsapi.mlb.com/api/v1/draft/{year}"
SLEEP_SEC = 0.10

BONUS_POOL_ERA_START = 2012
FORCE_SLOT_NA_YEARS = {2010, 2011}

# Post-round-10 "slot" convention (your paper-style)
def post10_threshold_millions(year: int) -> Optional[float]:
    if year < BONUS_POOL_ERA_START:
        return None
    if 2012 <= year <= 2016:
        return 0.100
    if 2017 <= year <= 2021:
        return 0.125
    return 0.150

# -----------------------------
# HTTP + caching
# -----------------------------
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def cache_path(kind: str, name: str) -> str:
    ensure_dir(CACHE_DIR)
    return os.path.join(CACHE_DIR, f"{kind}_{name}.json")

class HTTPError(Exception):
    pass

session = requests.Session()
session.headers.update({"User-Agent": "MLB-draft-research/1.0 (academic use)"})

@retry(
    retry=retry_if_exception_type((requests.RequestException, HTTPError)),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=1, max=20),
)
def fetch_json(url: str, timeout: int = 30) -> dict:
    r = session.get(url, timeout=timeout)
    if r.status_code >= 400:
        raise HTTPError(f"HTTP {r.status_code} for {url}")
    return r.json()

def fetch_json_cached(kind: str, name: str, url: str) -> dict:
    path = cache_path(kind, name)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    data = fetch_json(url)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    time.sleep(SLEEP_SEC)
    return data

# -----------------------------
# Position normalization + buckets
# -----------------------------
def normalize_pos(pos_raw: Optional[str]) -> Optional[str]:
    if pos_raw is None or (isinstance(pos_raw, float) and pd.isna(pos_raw)):
        return None
    p = str(pos_raw).strip().upper()
    if p == "":
        return None

    # already-abbrev
    if p in {"RHP", "LHP", "P", "C", "1B", "2B", "3B", "SS", "OF", "DH"}:
        return p
    if p in {"LF", "CF", "RF"}:
        return "OF"

    # full names
    if "RIGHT" in p and "PITCH" in p: return "RHP"
    if "LEFT" in p and "PITCH" in p:  return "LHP"
    if "PITCH" in p: return "P"
    if "CATCH" in p: return "C"
    if "FIRST" in p and "BASE" in p:  return "1B"
    if "SECOND" in p and "BASE" in p: return "2B"
    if "THIRD" in p and "BASE" in p:  return "3B"
    if "SHORT" in p and "STOP" in p:  return "SS"
    if "OUTFIELD" in p: return "OF"
    if "DESIGNATED" in p and "HITTER" in p: return "DH"

    return p

def derive_pitch_newpos(pos: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    p = normalize_pos(pos)
    if p is None:
        return None, None
    if p in {"RHP", "LHP", "P"}:
        return "Pitch", p
    if p == "C":
        return "Bat", "C"
    if p == "OF":
        return "Bat", "OF"
    if p in {"1B", "2B", "3B", "SS"}:
        return "Bat", "IF"
    if p == "DH":
        return "Bat", "DH"
    return "Bat", None

# -----------------------------
# Signed normalization
# -----------------------------
def normalize_signed(x: Any) -> Optional[bool]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "signed"}:
        return True
    if s in {"0", "false", "f", "no", "n", "unsigned", "did not sign", "didnt sign", "didn't sign"}:
        return False
    return None

# -----------------------------
# StatsAPI parser (uses person.primaryPosition.abbreviation, and TEAM NAME)
# -----------------------------
def parse_statsapi_draft(year: int) -> pd.DataFrame:
    data = fetch_json_cached("statsapi_draft", str(year), STATSAPI_DRAFT_URL.format(year=year))

    drafts = data.get("drafts", {})
    rounds = drafts.get("rounds", [])
    rows = []

    for r in rounds:
        for pick in r.get("picks", []):
            ov = pick.get("pickOverall")
            if ov is None:
                ov = pick.get("displayPickNumber") or pick.get("pickNumber")
            if ov is None:
                continue

            team = pick.get("team", {}) or {}
            team_name = team.get("name")

            person = pick.get("person", {}) or {}
            person_id = person.get("id")  # <-- NEW

            prim = person.get("primaryPosition", {}) or {}
            pos_abbrev = prim.get("abbreviation") or prim.get("name")
            pos_abbrev = normalize_pos(pos_abbrev)

            slot_dollars = pick.get("pickValue")
            slot_mil = None
            if slot_dollars is not None:
                try:
                    slot_mil = float(str(slot_dollars).replace(",", "")) / 1e6
                except ValueError:
                    slot_mil = None

            rows.append({
                "Year": int(year),
                "OvPck": int(ov),
                "Tm_src": team_name,
                "Pos_src": pos_abbrev,
                "Slot_src": slot_mil,
                "person_id": person_id,   # <-- NEW
            })

    return pd.DataFrame(rows).drop_duplicates(subset=["Year", "OvPck"])

# -----------------------------
# Fill missing Tm/Slot/Pos for 2012–2016 from cleaned_df using (Year, OvPck)
# -----------------------------
def fill_from_cleaned_2012_2016(df: pd.DataFrame, cleaned: pd.DataFrame) -> pd.DataFrame:
    for c in ["Year", "OvPck"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        cleaned[c] = pd.to_numeric(cleaned[c], errors="coerce").astype("Int64")

    for c in ["Tm", "Pos", "Slot"]:
        if c in cleaned.columns:
            cleaned[c] = cleaned[c].replace(r"^\s*$", pd.NA, regex=True)
    if "Slot" in cleaned.columns:
        cleaned["Slot"] = pd.to_numeric(cleaned["Slot"], errors="coerce")

    cleaned_1216 = cleaned.loc[
        cleaned["Year"].between(2012, 2016),
        ["Year", "OvPck", "Tm", "Slot", "Pos"]
    ].copy()

    out = df.merge(cleaned_1216, on=["Year", "OvPck"], how="left", suffixes=("", "_c"))
    mask = out["Year"].between(2012, 2016)

    for col in ["Tm", "Slot", "Pos"]:
        out.loc[mask & out[col].isna(), col] = out.loc[mask & out[col].isna(), f"{col}_c"]

    return out.drop(columns=["Tm_c", "Slot_c", "Pos_c"])

# -----------------------------
# Fill Signed from cleaned_mlbdraft.csv for 2012–2016 (Year, OvPck)
# -----------------------------
def fill_signed_from_cleaned_mlbdraft(df: pd.DataFrame, signed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill Signed for 2012–2016 from a raw draft file that includes BOTH signed and unsigned picks.
    Joins on (Year, OvPck). Does NOT overwrite Signed outside 2012–2016.
    """
    # Find required columns
    if "Signed" not in signed_df.columns:
        raise ValueError("Signed column not found in mlbdraft2012_2016.csv")

    # Ensure key types
    for c in ["Year", "OvPck"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    signed_df["Year"] = pd.to_numeric(signed_df["Year"], errors="coerce").astype("Int64")
    signed_df["OvPck"] = pd.to_numeric(signed_df["OvPck"], errors="coerce").astype("Int64")

    signed_df["Signed"] = signed_df["Signed"].apply(normalize_signed).astype("boolean")

    signed_1216 = signed_df.loc[
        signed_df["Year"].between(2012, 2016),
        ["Year", "OvPck", "Signed"]
    ].copy()

    out = df.merge(signed_1216, on=["Year", "OvPck"], how="left", suffixes=("", "_s"))
    mask = out["Year"].between(2012, 2016)

    # IMPORTANT: fill from raw for 2012–2016 (raw is ground truth)
    out.loc[mask, "Signed"] = out.loc[mask, "Signed_s"].combine_first(out.loc[mask, "Signed"])

    return out.drop(columns=["Signed_s"])

# -----------------------------
# Main
# -----------------------------
def main():
    ensure_dir(CACHE_DIR)

    df = pd.read_csv(FINAL_INPUT)
    cleaned = pd.read_csv(CLEANED_1216)

    # Optional Signed source file
    signed_df = None
    if os.path.exists(CLEANED_SIGN):
        signed_df = pd.read_csv(CLEANED_SIGN)

    # Treat blanks as missing
    for col in ["Tm", "Pos", "Slot", "Pitch", "newPOS", "BSp", "BmS", "Signed"]:
        if col in df.columns:
            df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)

    # Key types
    for col in ["Year", "Rnd", "OvPck"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Ensure dtypes so filling behaves + no warnings
    for col in ["Tm", "Pos", "Pitch", "newPOS"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype("string")

    for col in ["Bonus", "Slot", "BSp", "BmS"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Signed column (nullable boolean)
    if "Signed" not in df.columns:
        df["Signed"] = pd.NA
    df["Signed"] = df["Signed"].apply(normalize_signed).astype("boolean")

    # 1) Fill missing Tm/Slot/Pos from cleaned_df for 2012–2016 (Year, OvPck)
    df = fill_from_cleaned_2012_2016(df, cleaned)

    # 2) StatsAPI: build per-year tables and merge
    years = sorted(df["Year"].dropna().unique().astype(int).tolist())

    print("Fetching Team/Position/Slot via StatsAPI (only filling missing)...")
    frames: List[pd.DataFrame] = []
    for y in tqdm(years):
        try:
            api_df = parse_statsapi_draft(y)
            if not api_df.empty:
                frames.append(api_df)
        except Exception as e:
            print(f"[WARN] StatsAPI failed for {y}: {e}")

    if frames:
        tp = pd.concat(frames, ignore_index=True)

        # DIAGNOSTIC: match rate by year BEFORE filling
        tmp = df[["Year", "OvPck"]].merge(tp[["Year", "OvPck"]], on=["Year", "OvPck"], how="left", indicator=True)
        match_rate = tmp.groupby("Year")["_merge"].apply(lambda s: (s == "both").mean()).reset_index(name="match_rate")
        print("\nStatsAPI join match_rate by Year (should be ~1.0):")
        print(match_rate.to_string(index=False))

        df = df.merge(tp, on=["Year", "OvPck"], how="left")

        # Fill only missing
        df.loc[df["Tm"].isna(), "Tm"] = df.loc[df["Tm"].isna(), "Tm_src"]
        df.loc[df["Pos"].isna(), "Pos"] = df.loc[df["Pos"].isna(), "Pos_src"]

        can_fill_slot = (
            df["Slot"].isna()
            & df["Year"].notna()
            & (df["Year"] >= BONUS_POOL_ERA_START)
            & (~df["Year"].isin(FORCE_SLOT_NA_YEARS))
        )
        df.loc[can_fill_slot, "Slot"] = df.loc[can_fill_slot, "Slot_src"]

        df = df.drop(columns=[c for c in ["Tm_src", "Pos_src", "Slot_src"] if c in df.columns])

        # -----------------------------
        # NEW: Signed logic using person_id + redraft + bonus + (optional) pro-activity
        # -----------------------------

        # Ensure person_id numeric
        df["person_id"] = pd.to_numeric(df.get("person_id"), errors="coerce").astype("Int64")
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")

        # has_later_draft: drafted again in a later year => earlier pick unsigned
        max_year_by_pid = df.groupby("person_id")["Year"].transform("max")
        df["has_later_draft"] = df["Year"].notna() & max_year_by_pid.notna() & (df["Year"] < max_year_by_pid)

        # 1) Redrafted later => Signed = False for earlier pick
        df.loc[df["has_later_draft"] == True, "Signed"] = False

        # 2) Keep your gold Signed (2012–2016 from cleaned_mlbdraft) if already filled
        # (Your existing code fills those before this point, so we don't overwrite True/False.)

        # 3) Bonus > 0 => Signed = True (for remaining unknowns)
        bonus = pd.to_numeric(df["Bonus"], errors="coerce")
        df.loc[df["Signed"].isna() & bonus.notna() & (bonus > 0), "Signed"] = True

        # Ensure dtype is nullable boolean
        df["Signed"] = df["Signed"].astype("boolean")

    # Fallback if person_id never got merged in:
    if "person_id" not in df.columns:
        bonus = pd.to_numeric(df["Bonus"], errors="coerce")
        df.loc[df["Signed"].isna() & (bonus > 0), "Signed"] = True
        df.loc[df["Signed"].isna() & (bonus == 0), "Signed"] = False
        df["Signed"] = df["Signed"].astype("boolean")

    # Force Slot NA for 2010–2011
    df.loc[df["Year"].isin(FORCE_SLOT_NA_YEARS), "Slot"] = pd.NA

    # 3) Slot for rounds 11+ threshold (fill-only)
    mask_post10 = (
        df["Year"].notna() & df["Rnd"].notna() & (df["Rnd"] > 10)
        & df["Slot"].isna()
        & (df["Year"] >= BONUS_POOL_ERA_START)
    )
    df.loc[mask_post10, "Slot"] = df.loc[mask_post10, "Year"].astype(int).apply(post10_threshold_millions)
    df.loc[df["Year"].isin(FORCE_SLOT_NA_YEARS), "Slot"] = pd.NA

    # 4) Signed: fill from cleaned_mlbdraft.csv (2012–2016) if available
    if signed_df is not None:
        df = fill_signed_from_cleaned_mlbdraft(df, signed_df)

    mask_1216 = df["Year"].between(2012, 2016)
    print("2012–2016 Signed coverage:", df.loc[mask_1216, "Signed"].notna().mean())
    print(df.loc[mask_1216, "Signed"].value_counts(dropna=False))

    bonus = pd.to_numeric(df["Bonus"], errors="coerce")
    df.loc[df["Signed"].isna() & bonus.notna() & (bonus > 0), "Signed"] = True
    # leave remaining as NA
    df["Signed"] = df["Signed"].astype("boolean")

    # 5) Derive Pitch/newPOS (fill-only)
    df["Pos"] = df["Pos"].apply(normalize_pos)
    need = df["Pitch"].isna() | df["newPOS"].isna()
    derived = df.loc[need, "Pos"].apply(lambda x: derive_pitch_newpos(x))
    df.loc[need, "Pitch"] = derived.apply(lambda t: t[0])
    df.loc[need, "newPOS"] = derived.apply(lambda t: t[1])

    # 6) Compute BSp/BmS (fill-only)
    df["Slot"] = pd.to_numeric(df["Slot"], errors="coerce")
    if "BSp" not in df.columns:
        df["BSp"] = pd.NA
    if "BmS" not in df.columns:
        df["BmS"] = pd.NA

    mask_bsp = df["BSp"].isna() & df["Slot"].notna() & (df["Slot"] != 0)
    df.loc[mask_bsp, "BSp"] = df.loc[mask_bsp, "Bonus"] / df.loc[mask_bsp, "Slot"]

    mask_bms = df["BmS"].isna() & df["Slot"].notna()
    df.loc[mask_bms, "BmS"] = df.loc[mask_bms, "Bonus"] - df.loc[mask_bms, "Slot"]

    # Output ordering (preserve your extras like COVID_era)
    canonical = [
        "Year","Rnd","OvPck","Tm","Bonus","Slot","Name","Pos","Type","Bats","Throws",
        "Age","Pitch","newPOS","BSp","BmS","times","status","Signed"
    ]
    if "COVID_era" in df.columns:
        canonical = [
            "Year","Rnd","OvPck","Tm","Bonus","Slot","Name","Pos","Type","COVID_era","Bats","Throws",
            "Age","Pitch","newPOS","BSp","BmS","times","status","Signed"
        ]

    out_cols = [c for c in canonical if c in df.columns] + [c for c in df.columns if c not in canonical]
    df[out_cols].to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Wrote: {OUTPUT_CSV}")

    print("\nCoverage (% non-missing):")
    for col in ["Tm","Pos","Slot","Pitch","newPOS","BSp","BmS","Signed"]:
        pct = 100 * df[col].notna().mean()
        print(f"  {col:7s}: {pct:6.2f}%")

    print("\nTop Pos values:")
    print(df["Pos"].value_counts(dropna=False).head(15).to_string())

if __name__ == "__main__":
    main()

output_data = pd.read_csv(OUTPUT_CSV)
output_data.head(10)
from collections import Counter
Counter(output_data.Signed)