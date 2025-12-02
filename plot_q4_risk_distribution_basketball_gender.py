"""Plot Q4 for Basketball, separated by gender.

Classifies players into:
 - Low Risk
 - High Asymmetry (asym_pct >= 10%)
 - High Load (accel_load_accum >= 90th pct)
 - Combined Risk (both conditions)

Produces `q4_basketball_risk_by_gender.png` and prints counts/percentages.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent / '.env'
load_dotenv(ENV_PATH)

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE = os.getenv('DB_TABLE', 'research_experiment_refactor_test')

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    raise SystemExit('Missing DB credentials in .env')

URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"
engine = create_engine(URL)

METRICS = ['leftMaxForce', 'rightMaxForce', 'accel_load_accum']

SQL = f"""
SELECT playername, team, metric, value
FROM {DB_TABLE}
WHERE metric IN ({','.join([f"'{m}'" for m in METRICS])})
  AND value IS NOT NULL
"""

def fetch_df():
    df = pd.read_sql(SQL, engine)
    return df


def per_player_wide(df):
    pm = df.groupby(['playername', 'team', 'metric'])['value'].mean().unstack()
    pm = pm.rename_axis(('playername', 'team')).reset_index()
    return pm


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


def compute_asym_pct_series(left, right):
    left = left.fillna(0)
    right = right.fillna(0)
    maxv = np.maximum(left, right)
    asym = np.zeros(len(left))
    mask = maxv > 0
    asym[mask] = np.abs(left[mask] - right[mask]) / maxv[mask] * 100.0
    return asym


def classify_risk(pm: pd.DataFrame, accel_thresh: float, asym_threshold: float = 10.0):
    df = pm.copy()
    if 'leftMaxForce' in df.columns and 'rightMaxForce' in df.columns:
        df['asym_pct'] = compute_asym_pct_series(df['leftMaxForce'], df['rightMaxForce'])
    else:
        df['asym_pct'] = np.nan
    df['high_asym'] = df['asym_pct'] >= asym_threshold
    df['high_load'] = df['accel_load_accum'] >= accel_thresh

    def label_row(r):
        if r['high_asym'] and r['high_load']:
            return 'Combined Risk'
        if r['high_asym']:
            return 'High Asymmetry'
        if r['high_load']:
            return 'High Load'
        return 'Low Risk'

    df['risk_category'] = df.apply(label_row, axis=1)
    return df


def plot_by_gender(df_risk: pd.DataFrame, out_path: Path):
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set(style='whitegrid')
    # Keep only Male/Female
    df_risk = df_risk[df_risk['gender'].isin(['Male','Female'])].copy()
    order = ['Low Risk', 'High Asymmetry', 'High Load', 'Combined Risk']

    summary = df_risk.groupby(['gender','risk_category']).size().unstack(fill_value=0).reindex(columns=order).fillna(0)
    # compute percentages per gender
    pct = summary.div(summary.sum(axis=1), axis=0) * 100.0

    fig, ax = plt.subplots(figsize=(9,5))
    # side-by-side bars: for each category plot Male and Female
    x = np.arange(len(order))
    width = 0.35

    male_counts = summary.loc['Male'].values if 'Male' in summary.index else np.zeros(len(order))
    female_counts = summary.loc['Female'].values if 'Female' in summary.index else np.zeros(len(order))

    bars1 = ax.bar(x - width/2, male_counts, width, label='Male', color='#1f77b4')
    bars2 = ax.bar(x + width/2, female_counts, width, label='Female', color='#ff7f0e')

    ax.set_xticks(x)
    ax.set_xticklabels(order)
    ax.set_ylabel('Number of players')
    ax.set_xlabel('Risk category')
    ax.set_title('Basketball — Risk category distribution by gender')
    ax.legend()

    # annotate counts + percentages
    for i in range(len(order)):
        m = male_counts[i]
        f = female_counts[i]
        pm = pct.loc['Male'].values[i] if 'Male' in pct.index else 0.0
        pf = pct.loc['Female'].values[i] if 'Female' in pct.index else 0.0
        ax.text(x[i] - width/2, m + max(male_counts.max(), female_counts.max())*0.01, f'{int(m)} ({pm:.1f}%)', ha='center', fontsize=9)
        ax.text(x[i] + width/2, f + max(male_counts.max(), female_counts.max())*0.01, f'{int(f)} ({pf:.1f}%)', ha='center', fontsize=9)

    plt.xticks(rotation=10)
    plt.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main():
    print('Fetching left/right forces and accel loads...')
    df = fetch_df()
    if df.empty:
        print('No data returned for required metrics')
        return
    pm = per_player_wide(df)
    pm['gender'] = pm['team'].apply(infer_gender)
    pm['sport'] = pm['team'].apply(infer_sport)

    # Filter Basketball only
    pm_b = pm[pm['sport'] == 'Basketball'].copy()
    print(f'Total Basketball rows: {len(pm_b)}')

    if pm_b.empty:
        print('No Basketball players found')
        return

    # accel 90th percentile threshold computed on Basketball players only
    if 'accel_load_accum' in pm_b.columns:
        accel_thresh = pm_b['accel_load_accum'].dropna().quantile(0.90)
    else:
        accel_thresh = np.inf

    df_risk = classify_risk(pm_b, accel_thresh=accel_thresh, asym_threshold=10.0)
    print(f'Accel 90th percentile threshold (Basketball) = {accel_thresh:.2f}')

    # Print counts by gender
    counts = df_risk.groupby(['gender','risk_category']).size().unstack(fill_value=0)
    print('\nCounts by gender and risk category:')
    print(counts.to_string())

    out = Path('q4_basketball_risk_by_gender.png')
    plot_by_gender(df_risk, out)
    print(f'Bar chart saved to {out.resolve()}')

if __name__ == '__main__':
    main()
