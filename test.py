"""test.py

Connect to the project's database (reads credentials from `.env`) and run
several focused queries to support research questions for the following
metrics:

- Jump Height ("Jump Height(m)")
- Peak Propulsive Force ("Peak Propulsive Force(N)")
- Total Distance ("distance_total")
- Accumulated Acceleration Load ("accel_load_accum")
- Left/Right Max Force ("leftMaxForce", "rightMaxForce")

The script prints summary tables and identifies candidate research
questions that the data can answer, together with the query results that
support each question.
"""
from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE", "research_experiment_refactor_test")

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    raise SystemExit("Missing DB credentials in .env — please set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME")

URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"
ENGINE = create_engine(URL)

# Metric names expected in the DB 'metric' column
METRICS = [
    'Jump Height(m)',
    'Peak Propulsive Force(N)',
    'distance_total',
    'accel_load_accum',
    'leftMaxForce',
    'rightMaxForce',
]


def fetch_metrics_table(table: str, metrics: list[str]) -> pd.DataFrame:
    """Fetch records for the requested metrics (non-null values).

    Returns a long-form DataFrame with columns: playername, team, metric, value, timestamp
    """
    metrics_str = "','".join(metrics)
    sql = f"""
SELECT playername, team, metric, value, timestamp
FROM {table}
WHERE metric IN ('{metrics_str}')
  AND value IS NOT NULL
"""
    df = pd.read_sql(sql, ENGINE)
    # Ensure timestamp is datetime where present
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df


def research_team_means(df: pd.DataFrame) -> pd.DataFrame:
    """Compute team means for each metric and print results."""
    team_means = df.groupby(['team', 'metric'])['value'].mean().reset_index()
    pivot = team_means.pivot(index='team', columns='metric', values='value')
    print('\n== Team means (wide format) ==')
    print(pivot.round(4).to_string())
    return pivot


def research_correlations(player_means: pd.DataFrame):
    """Compute correlations of interest on per-player mean values.

    We compute Pearson correlations between:
    - Jump Height vs Peak Propulsive Force
    - distance_total vs accel_load_accum
    """
    print('\n== Correlations (per-player means) ==')
    pairs = [
        ('Jump Height(m)', 'Peak Propulsive Force(N)'),
        ('distance_total', 'accel_load_accum'),
    ]
    for a, b in pairs:
        if a in player_means.columns and b in player_means.columns:
            tmp = player_means[[a, b]].dropna()
            if len(tmp) >= 3:
                corr = tmp[a].corr(tmp[b])
                print(f"Correlation {a} vs {b}: {corr:.3f} (N={len(tmp)})")
            else:
                print(f"Not enough paired data for correlation {a} vs {b} (N={len(tmp)})")
        else:
            print(f"Columns missing for correlation: {a} or {b}")


def research_left_right_asymmetry(player_means: pd.DataFrame, threshold_pct: float = 10.0) -> pd.DataFrame:
    """Compute left/right asymmetry per player and return those above threshold_pct."""
    if 'leftMaxForce' not in player_means.columns or 'rightMaxForce' not in player_means.columns:
        print('\nLeft/right force columns not available in player-level data')
        return pd.DataFrame()

    df_lr = player_means[['leftMaxForce', 'rightMaxForce']].dropna().copy()
    df_lr['asym_pct'] = (df_lr['leftMaxForce'] - df_lr['rightMaxForce']).abs() / df_lr[['leftMaxForce', 'rightMaxForce']].max(axis=1) * 100
    df_lr = df_lr.reset_index()
    high_asym = df_lr[df_lr['asym_pct'] >= threshold_pct].sort_values('asym_pct', ascending=False)
    print(f"\n== Players with >= {threshold_pct}% left/right asymmetry ==")
    if high_asym.empty:
        print('None found')
    else:
        print(high_asym[['playername', 'team', 'leftMaxForce', 'rightMaxForce', 'asym_pct']].to_string(index=False))
    return high_asym


def research_top_loaders(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Identify players with highest accumulated acceleration load and total distance."""
    # Player-level mean values per metric
    pm = df.groupby(['playername', 'team', 'metric'])['value'].mean().unstack()
    results = {}
    for metric in ['accel_load_accum', 'distance_total']:
        if metric in pm.columns:
            top = pm[metric].dropna().sort_values(ascending=False).head(top_n)
            results[metric] = top
            print(f"\nTop {top_n} players by {metric}:")
            print(top.to_string())
        else:
            print(f"Metric {metric} not available in player means")
    return results


def per_player_means(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-player mean values (wide) for the metrics of interest."""
    pm = df.groupby(['playername', 'team', 'metric'])['value'].mean().unstack()
    pm = pm.rename_axis(('playername', 'team')).reset_index()
    return pm.set_index(['playername', 'team'])


def yearly_trends(df: pd.DataFrame):
    """Compute yearly averages for each metric (using timestamp).

    Useful for research questions about trends over time.
    """
    if 'timestamp' not in df.columns:
        print('\nNo timestamp column available to compute trends')
        return pd.DataFrame()
    df_ts = df.dropna(subset=['timestamp']).copy()
    df_ts['year'] = df_ts['timestamp'].dt.year
    yearly = df_ts.groupby(['year', 'metric'])['value'].mean().unstack()
    print('\n== Yearly metric means ==')
    print(yearly.round(3).to_string())
    return yearly


def suggest_research_questions():
    # Five focused research questions (branching flow will use these)
    questions = [
        ('Q1', 'Are there systematic differences in Jump Height and Peak Propulsive Force between men and women within Basketball?'),
        ('Q2', 'Which gender/sport groups show higher left/right asymmetry prevalence (>=10%)?'),
        ('Q3', 'Do high-asymmetry players show higher accumulated acceleration load than low-asymmetry players?'),
        ('Q4', 'Which Basketball players exceed combined risk thresholds (asymmetry >=10% AND accel_load_accum >= 90th pct)?'),
        ('Q5', 'Recommend target players/cohorts for monitoring or intervention (if combined-risk players found) or monitoring strategy otherwise.'),
    ]
    print('\n== Planned 5 research questions (gender, risk/injury, basketball) ==')
    for id_, q in questions:
        print(f"{id_}: {q}")
    return questions


def run_question_flow(df: pd.DataFrame, asym_threshold: float = 10.0):
    """Run a 5-question branching flow where each answer directs the next question.

    Q1 -> Q2 -> Q3 -> Q4 -> Q5 (branching rules choose relevant sub-questions)
    """
    print('\n== Running 5-question branching flow ==')

    # Prepare player-level means
    pm = per_player_means(df).reset_index()

    # Infer gender/sport using same heuristics as gender summary
    def infer_gender(team: str) -> str:
        if not isinstance(team, str):
            return 'Unknown'
        t = team.lower()
        if "women" in t or "women's" in t or "womens" in t:
            return 'Female'
        if "men" in t or "men's" in t or "mens" in t:
            return 'Male'
        return 'Unknown'

    def infer_sport(team: str) -> str:
        if not isinstance(team, str):
            return 'Unknown'
        t = team.lower()
        if 'basketball' in t:
            return 'Basketball'
        if 'football' in t:
            return 'Football'
        return 'Other'

    pm['gender'] = pm['team'].apply(infer_gender)
    pm['sport'] = pm['team'].apply(infer_sport)

    # Q1: Check mean differences (Jump Height and Peak Force) by gender within Basketball
    print('\nQ1: Gender differences in Jump Height and Peak Propulsive Force (Basketball only)')
    sports = ['Basketball']
    difference_found = {}
    for sport in sports:
        sub_m = pm[(pm['sport'] == sport) & (pm['gender'] == 'Male')]
        sub_f = pm[(pm['sport'] == sport) & (pm['gender'] == 'Female')]
        if sub_m.empty or sub_f.empty:
            print(f"  {sport}: insufficient male/female data (Male={len(sub_m)}, Female={len(sub_f)})")
            difference_found[sport] = False
            continue
        # metrics to compare
        for metric in ['Jump Height(m)', 'Peak Propulsive Force(N)']:
            if metric in sub_m.columns and metric in sub_f.columns:
                m_mean = sub_m[metric].mean()
                f_mean = sub_f[metric].mean()
                # pooled std for effect size
                pooled_sd = (((sub_m[metric].var(ddof=1) * (len(sub_m)-1)) + (sub_f[metric].var(ddof=1) * (len(sub_f)-1))) / (len(sub_m)+len(sub_f)-2))**0.5 if (len(sub_m)+len(sub_f)-2)>0 else 0
                effect = abs(m_mean - f_mean) / pooled_sd if pooled_sd > 0 else 0
                print(f"  {sport} | {metric}: Male_mean={m_mean:.3f}, Female_mean={f_mean:.3f}, effect_size={effect:.3f} (nM={len(sub_m)}, nF={len(sub_f)})")
                # decision rule: effect_size >= 0.2 and both groups have >=30 -> treat as 'difference'
                if effect >= 0.2 and len(sub_m) >= 30 and len(sub_f) >= 30:
                    difference_found[sport] = True
                    break
        else:
            difference_found[sport] = False
        
        # Print sport-level correlation for Jump vs Peak
        if 'Jump Height(m)' in pm.columns and 'Peak Propulsive Force(N)' in pm.columns:
            sub_sport = pm[pm['sport'] == sport][['Jump Height(m)', 'Peak Propulsive Force(N)']].dropna()
            if len(sub_sport) >= 3:
                corr = sub_sport['Jump Height(m)'].corr(sub_sport['Peak Propulsive Force(N)'])
                print(f"  {sport} — Jump vs Peak correlation: N={len(sub_sport)}, r = {corr:.3f}")
            else:
                print(f"  {sport} — Jump vs Peak: insufficient paired data (n={len(sub_sport)})")
        
        
        # Also ensure gender-specific Peak Propulsive Force means for Basketball
        if sport == 'Basketball' and 'Peak Propulsive Force(N)' in pm.columns:
            sub_m = pm[(pm['sport'] == sport) & (pm['gender'] == 'Male')]
            sub_f = pm[(pm['sport'] == sport) & (pm['gender'] == 'Female')]
            if not sub_m.empty and not sub_f.empty:
                p_m = sub_m['Peak Propulsive Force(N)'].mean()
                p_f = sub_f['Peak Propulsive Force(N)'].mean()
                print(f"  {sport} | Peak Propulsive Force(N) by gender: Male_mean={p_m:.3f}, Female_mean={p_f:.3f} (nM={len(sub_m)}, nF={len(sub_f)})")
            else:
                print(f"  {sport} | Peak Propulsive Force(N) by gender: insufficient male/female data (nM={len(sub_m)}, nF={len(sub_f)})")

    # Q2: If differences found in a sport -> compare asymmetry prevalence by gender for that sport
    # Otherwise compute Jump vs Peak correlation within the sport
    for sport in sports:
        if difference_found.get(sport):
            print(f"\nQ2 (branch): {sport} shows gender differences — compute asymmetry prevalence by gender")
            sub = pm[pm['sport'] == sport]
            if 'leftMaxForce' in sub.columns and 'rightMaxForce' in sub.columns:
                sub_lr = sub[['gender', 'leftMaxForce', 'rightMaxForce']].dropna()
                sub_lr = sub_lr.assign(asym=lambda d: (d['leftMaxForce'] - d['rightMaxForce']).abs() / d[['leftMaxForce', 'rightMaxForce']].max(axis=1) * 100)
                preval = sub_lr.groupby('gender')['asym'].apply(lambda x: (x >= asym_threshold).mean() * 100)
                print(preval.to_string())
                # pick gender with higher prevalence
                higher = preval.idxmax() if not preval.empty else None
                print(f"  Higher asymmetry prevalence: {higher}")
            else:
                print('  Asymmetry metrics not available for this sport')
        else:
            print(f"\nQ2 (alt): {sport} shows no clear gender mean differences — check Jump vs Peak correlation by gender")
            if 'Jump Height(m)' in pm.columns and 'Peak Propulsive Force(N)' in pm.columns:
                for gender in ['Male', 'Female']:
                    subg = pm[(pm['sport'] == sport) & (pm['gender'] == gender)][['Jump Height(m)', 'Peak Propulsive Force(N)']].dropna()
                    if len(subg) >= 5:
                        corr = subg['Jump Height(m)'].corr(subg['Peak Propulsive Force(N)'])
                        print(f"  {sport} {gender} correlation Jump vs Peak: {corr:.3f} (n={len(subg)})")
                    else:
                        print(f"  {sport} {gender}: insufficient paired data (n={len(subg)})")

    # Q3: Do high-asymmetry players have higher accel_load_accum?
    print('\nQ3: Compare accel_load_accum between high-asymmetry and low-asymmetry players')
    if {'leftMaxForce', 'rightMaxForce', 'accel_load_accum'}.issubset(pm.columns):
        tmp = pm.dropna(subset=['leftMaxForce', 'rightMaxForce', 'accel_load_accum']).copy()
        tmp['asym_pct'] = (tmp['leftMaxForce'] - tmp['rightMaxForce']).abs() / tmp[['leftMaxForce', 'rightMaxForce']].max(axis=1) * 100
        high = tmp[tmp['asym_pct'] >= asym_threshold]['accel_load_accum']
        low = tmp[tmp['asym_pct'] < asym_threshold]['accel_load_accum']
        if not high.empty and not low.empty:
            print(f"  high-asymmetry N={len(high)}, mean accel_load_accum={high.mean():.2f}")
            print(f"  low-asymmetry N={len(low)}, mean accel_load_accum={low.mean():.2f}")
            diff_pct = (high.mean() - low.mean()) / low.mean() * 100 if low.mean() != 0 else float('nan')
            print(f"  mean difference = {diff_pct:.1f}%")
        else:
            print('  Not enough data to compare high vs low asymmetry groups')
    else:
        print('  Required metrics not available to compare asymmetry vs load')

    # Q4: Identify combined-risk players (asymmetry >= threshold AND accel_load_accum >= 90th pct)
    print('\nQ4: Identify combined-risk players (asym >= {0}% AND accel_load_accum >= 90th pct)'.format(asym_threshold))
    combined = pd.DataFrame()
    if 'accel_load_accum' in pm.columns and 'leftMaxForce' in pm.columns and 'rightMaxForce' in pm.columns:
        accel_thresh = pm['accel_load_accum'].dropna().quantile(0.90)
        tmp = pm.dropna(subset=['leftMaxForce', 'rightMaxForce', 'accel_load_accum']).copy()
        tmp['asym_pct'] = (tmp['leftMaxForce'] - tmp['rightMaxForce']).abs() / tmp[['leftMaxForce', 'rightMaxForce']].max(axis=1) * 100
        combined = tmp[(tmp['asym_pct'] >= asym_threshold) & (tmp['accel_load_accum'] >= accel_thresh)]
        print(f"  accel_load_accum 90th pct = {accel_thresh:.2f}; combined-risk N={len(combined)}")
        if not combined.empty:
            print(combined[['playername', 'team', 'asym_pct', 'accel_load_accum']].sort_values(['asym_pct','accel_load_accum'], ascending=False).head(20).to_string(index=False))
    else:
        print('  Missing metrics for combined-risk identification')

    # Q5: Recommend actions or list target players
    print('\nQ5: Recommendation based on Q4 results')
    if not combined.empty:
        print('  Found combined-risk players — recommend targeted monitoring/intervention for these top candidates:')
        print(combined[['playername','team','asym_pct','accel_load_accum']].sort_values(['asym_pct','accel_load_accum'], ascending=False).head(10).to_string(index=False))
        print("  Suggested actions: movement screen, bilateral strength testing, load-reduction trial, targeted neuromuscular training.")
    else:
        print('  No combined-risk players found. Suggest: continue routine monitoring, set sport-specific thresholds, and collect more longitudinal jump/load data to detect changes.')

    print('\nBranching flow complete')
    return


def research_gender_sport_summary(df: pd.DataFrame, asym_threshold: float = 10.0):
    """Produce short summaries comparing male vs female players for Basketball.

    Outputs:
    - Mean metric values by (sport, gender)
    - Asymmetry prevalence (pct of players >= asym_threshold)
    - Fraction of players above high-load threshold (90th percentile)
    """
    print('\n== Gender & sport focused summary (Basketball) ==')
    # Prepare player-level means
    pm = per_player_means(df).reset_index()

    # derive simple gender and sport tags from team string
    def infer_gender(team: str) -> str:
        if not isinstance(team, str):
            return 'Unknown'
        t = team.lower()
        if "women" in t or "women's" in t or "womens" in t:
            return 'Female'
        if "men" in t or "men's" in t or "mens" in t:
            return 'Male'
        return 'Unknown'

    def infer_sport(team: str) -> str:
        if not isinstance(team, str):
            return 'Unknown'
        t = team.lower()
        if 'basketball' in t:
            return 'Basketball'
        if 'football' in t:
            return 'Football'
        return 'Other'

    pm['gender'] = pm['team'].apply(infer_gender)
    pm['sport'] = pm['team'].apply(infer_sport)

    metrics_of_interest = [m for m in METRICS if m in pm.columns]
    if not metrics_of_interest:
        print('No matching metrics found in player-level data for gender/sport summary')
        return

    # high-load threshold (90th percentile) computed on player means for accel_load_accum and distance_total
    high_loads = {}
    for load_metric in ['accel_load_accum', 'distance_total']:
        if load_metric in pm.columns:
            high = pm[load_metric].dropna().quantile(0.90)
            high_loads[load_metric] = high

    groups = [('Basketball', 'Male'), ('Basketball', 'Female')]
    for sport, gender in groups:
        sub = pm[(pm['sport'] == sport) & (pm['gender'] == gender)]
        label = f"{sport} ({gender})"
        if sub.empty:
            print(f"\n{label}: no players found")
            continue
        print(f"\n{label}: N_players={len(sub)}")
        # mean values
        means = sub[metrics_of_interest].mean()
        print('  Mean values:')
        for m in metrics_of_interest:
            val = means.get(m, float('nan'))
            print(f"    - {m}: {val:.3f}" if pd.notna(val) else f"    - {m}: NA")

        # asymmetry prevalence
        if 'leftMaxForce' in sub.columns and 'rightMaxForce' in sub.columns:
            asym_pct = (sub[['leftMaxForce', 'rightMaxForce']].dropna()
                       .assign(asym=lambda d: (d['leftMaxForce'] - d['rightMaxForce']).abs() / d[['leftMaxForce', 'rightMaxForce']].max(axis=1) * 100))
            prevalence = (asym_pct['asym'] >= asym_threshold).mean() * 100 if not asym_pct.empty else 0.0
            print(f"  Asymmetry >= {asym_threshold}% prevalence: {prevalence:.1f}%")
        else:
            print('  Asymmetry metrics not available')

        # high-load prevalence
        for load_metric, thresh in high_loads.items():
            sub_load = sub[load_metric].dropna()
            if sub_load.empty:
                print(f"  {load_metric}: no data")
                continue
            high_frac = (sub_load >= thresh).mean() * 100
            print(f"  {load_metric} >= 90th pct ({thresh:.2f}): {high_frac:.1f}%")

    # Identify combined-risk players (asymmetry >= threshold AND high accel_load_accum)
    if 'accel_load_accum' in pm.columns and 'leftMaxForce' in pm.columns and 'rightMaxForce' in pm.columns:
        accel_thresh = high_loads.get('accel_load_accum')
        if accel_thresh is not None:
            pm_lr = pm.dropna(subset=['leftMaxForce', 'rightMaxForce', 'accel_load_accum']).copy()
            pm_lr['asym_pct'] = (pm_lr['leftMaxForce'] - pm_lr['rightMaxForce']).abs() / pm_lr[['leftMaxForce', 'rightMaxForce']].max(axis=1) * 100
            combined = pm_lr[(pm_lr['asym_pct'] >= asym_threshold) & (pm_lr['accel_load_accum'] >= accel_thresh)]
            print(f"\nCombined-risk players (asym >= {asym_threshold}% AND accel_load_accum >= 90th pct): N={len(combined)}")
            if not combined.empty:
                print(combined[['playername', 'team', 'asym_pct', 'accel_load_accum']].sort_values(['asym_pct', 'accel_load_accum'], ascending=False).head(20).to_string(index=False))
    return


def main():
    print('Connecting to DB and fetching requested metrics...')
    df = fetch_metrics_table(DB_TABLE, METRICS)
    print(f'Fetched {len(df)} rows; metrics present: {sorted(df.metric.unique())}')

    # 1) Team means
    pivot = research_team_means(df)

    # 2) Per-player mean table
    player_means = per_player_means(df)

    # 3) Correlations
    research_correlations(player_means)

    # 4) Left/right asymmetry
    _ = research_left_right_asymmetry(player_means, threshold_pct=10.0)

    # 5) Top loaders and high-distance players
    _ = research_top_loaders(df, top_n=10)

    # 6) Yearly trends
    _ = yearly_trends(df)

    # 7) Suggest research questions (planned list)
    suggest_research_questions()

    # 8) Run the 5-question branching flow (answers guide next questions)
    run_question_flow(df, asym_threshold=10.0)


if __name__ == '__main__':
    main()
