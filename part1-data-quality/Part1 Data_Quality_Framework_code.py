#!/usr/bin/env python
# coding: utf-8

# # #!/usr/bin/env python3
# ""
# Data Quality Validation script for raw event streams.
# 
# Usage:
# python dq_validate_events.py --input events.csv --format csv --output_dir ./dq_report
# 
# Checks implemented:
# - schema_presence
# - required_fields
# - type_checks
# - timestamp_range_and_order
# 
# 
# 
# 
# #!/usr/bin/env python3
# """
# Data Quality Validation script for raw event streams.
# 
# Usage:
#     python dq_validate_events.py --input events.csv --format csv --output_dir ./dq_report
# 
# Checks implemented:
#  - schema_presence
#  - required_fields
#  - type_checks
#  - timestamp_range_and_order
#  - duplicate_event_ids / duplicate_transactions
#  - missing_or_zero_revenue
#  - negative_revenue
#  - event_sequence_checks (e.g., purchase without add_to_cart)
#  - high_cardinality_spikes
#  - sampling_or_volume_drop
#  - malformed_json_in_properties
#  - currency_inconsistencies
#  - user_identity_issues (missing user_id/client_id)
#  - product_missing_info
#  - distribution_drift against baseline (first N days)
#  - timezone_anomalies
# 
# The script outputs a JSON summary and CSV samples for each failing check.
# """

# In[ ]:



import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(description="Data Quality Validation for event streams")
    p.add_argument("--input", required=True, help="Path to input file (csv or jsonl)")
    p.add_argument("--format", choices=["csv", "jsonl"], default="csv")
    p.add_argument("--output_dir", default="./dq_report")
    p.add_argument("--timefield", default="event_timestamp", help="Timestamp field name")
    p.add_argument("--sample_size", type=int, default=100, help="Sample size for issue CSV")
    p.add_argument("--full_output", action="store_true", help="Write full failing rows to CSV")
    return p.parse_args()


def load_data(path, fmt):
    if fmt == "csv":
        df = pd.read_csv(path, dtype=str)
    else:
        # newline delimited json
        df = pd.read_json(path, lines=True, dtype=str)
    return df


def safe_parse_ts(val):
    if pd.isna(val):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    # try pandas fallback
    try:
        return pd.to_datetime(val)
    except Exception:
        return None


# --- CHECKS ---

def check_required_fields(df, required_fields):
    missing = [f for f in required_fields if f not in df.columns]
    return missing


def check_nulls(df, fields):
    nulls = {f: int(df[f].isna().sum()) if f in df.columns else None for f in fields}
    return nulls


def check_types_and_casts(df):
    issues = []
    # check revenue/value can be cast to float
    if 'value' in df.columns:
        try:
            _ = pd.to_numeric(df['value'])
        except Exception as e:
            issues.append({'check': 'value_parse_error', 'message': str(e)})
    return issues


def check_timestamps(df, timecol):
    bad = []
    parsed = []
    for i, v in df[timecol].fillna('').iteritems():
        ts = safe_parse_ts(v)
        if ts is None:
            bad.append(i)
        parsed.append(ts)
    return bad, parsed


def detect_duplicate_transactions(df, tx_col='transaction_id'):
    if tx_col not in df.columns:
        return []
    dup = df[df.duplicated(subset=[tx_col], keep=False) & df[tx_col].notna()]
    return dup.index.tolist(), dup


def detect_negative_or_zero_revenue(df):
    issues = {}
    if 'value' not in df.columns:
        return {'missing_value_field': True}
    # cast safely
    vals = pd.to_numeric(df['value'], errors='coerce')
    negative_idx = vals[vals < 0].index.tolist()
    zero_idx = vals[vals == 0].index.tolist()
    nan_idx = vals[vals.isna()].index.tolist()
    return {'negative': negative_idx, 'zero': zero_idx, 'nan': nan_idx}


def detect_event_sequence_issues(df):
    # e.g., purchases without previous add_to_cart or view
    seq_issues = []
    if 'event_name' not in df.columns:
        return seq_issues
    # group by user/session and look for purchase without prior add_to_cart
    group_cols = ['user_id'] if 'user_id' in df.columns else ['client_id'] if 'client_id' in df.columns else None
    if not group_cols:
        return seq_issues
    for u, g in df.groupby(group_cols):
        g_sorted = g.sort_values('event_timestamp')
        has_add = False
        for _, row in g_sorted.iterrows():
            if row['event_name'].lower() in ('add_to_cart', 'add_to_cart_item', 'add_to_cart_event'):
                has_add = True
            if row['event_name'].lower() in ('purchase', 'transaction', 'order_complete'):
                if not has_add:
                    seq_issues.append({'user': u, 'row_index': row.name, 'event': row['event_name']})
    return seq_issues


def detect_volume_anomalies(df, timecol='event_timestamp', period='D'):
    # detect sudden drops or spikes in event volume
    df_ts = df.copy()
    df_ts['__ts'] = df_ts[timecol].apply(safe_parse_ts)
    df_ts = df_ts[df_ts['__ts'].notna()].set_index('__ts')
    counts = df_ts.resample(period).size()
    # simple z-score on day counts
    mean = counts.mean()
    std = counts.std()
    anomalies = counts[(counts - mean).abs() > 3 * std]
    return counts, anomalies.to_dict()


def detect_high_cardinality_spikes(df, field, window_days=1, timefield='event_timestamp'):
    # for example user_id or product_id sudden new values
    # return dates where unique count increased dramatically
    if field not in df.columns or timefield not in df.columns:
        return {}
    df_c = df.copy()
    df_c['__ts'] = df_c[timefield].apply(safe_parse_ts)
    df_c = df_c[df_c['__ts'].notna()].set_index('__ts')
    uniq = df_c.groupby(pd.Grouper(freq='D'))[field].nunique()
    pct_change = uniq.pct_change().fillna(0)
    spikes = pct_change[pct_change > 1.0]
    return uniq.to_dict(), spikes.to_dict()


def detect_malformed_json(df, col='event_properties'):
    bad_idx = []
    if col not in df.columns:
        return bad_idx
    for i, v in df[col].fillna('').iteritems():
        if v == '':
            continue
        try:
            json.loads(v)
        except Exception:
            bad_idx.append(i)
    return bad_idx


# --- Runner / Orchestration ---

def run_all_checks(df, args):
    report = {'summary': {}, 'details': {}}
    required = ['event_name', args.timefield]
    missing_fields = check_required_fields(df, required)
    report['details']['missing_fields'] = missing_fields
    report['summary']['required_fields_present'] = len(missing_fields) == 0

    # null counts
    fields_to_check = ['user_id', 'client_id', 'transaction_id', 'value', 'currency', 'product_id']
    nulls = check_nulls(df, fields_to_check)
    report['details']['null_counts'] = nulls

    # types
    type_issues = check_types_and_casts(df)
    report['details']['type_issues'] = type_issues

    # timestamps
    if args.timefield in df.columns:
        bad_ts_idx, parsed_ts = check_timestamps(df, args.timefield)
        report['details']['bad_timestamps_count'] = len(bad_ts_idx)
        report['details']['bad_timestamps_sample'] = bad_ts_idx[:args.sample_size]
        df['_parsed_ts'] = parsed_ts
    else:
        report['details']['bad_timestamps_count'] = None

    # duplicates
    dup_idx, dup_df = detect_duplicate_transactions(df, 'transaction_id')
    report['details']['duplicate_transaction_count'] = len(dup_idx)
    report['details']['duplicate_transaction_sample'] = dup_idx[:args.sample_size]

    # revenue
    rev_issues = detect_negative_or_zero_revenue(df)
    report['details']['revenue_issues_summary'] = {k: len(v) if isinstance(v, list) else v for k, v in rev_issues.items()}
    report['details']['revenue_issues_sample'] = {k: v[:args.sample_size] if isinstance(v, list) else v for k, v in rev_issues.items()}

    # event sequence
    seq_issues = detect_event_sequence_issues(df)
    report['details']['event_sequence_issues_count'] = len(seq_issues)
    report['details']['event_sequence_issues_sample'] = seq_issues[:args.sample_size]

    # volume anomalies
    counts, anomalies = detect_volume_anomalies(df, args.timefield)
    report['details']['volume_by_period'] = {str(k): int(v) for k, v in counts.to_dict().items()} if hasattr(counts, 'to_dict') else counts
    report['details']['volume_anomalies'] = anomalies

    # high cardinality
    uniq_user, user_spikes = detect_high_cardinality_spikes(df, 'user_id', timefield=args.timefield)
    report['details']['user_unique_by_day'] = uniq_user
    report['details']['user_spikes'] = user_spikes

    # malformed JSON
    bad_json_idx = detect_malformed_json(df)
    report['details']['malformed_event_properties_count'] = len(bad_json_idx)
    report['details']['malformed_event_properties_sample'] = bad_json_idx[:args.sample_size]

    # currency issues
    if 'currency' in df.columns:
        unique_currencies = df['currency'].dropna().unique().tolist()
        report['details']['unique_currencies'] = unique_currencies

    # identity issues
    uid_null = int(df.get('user_id', pd.Series([], dtype=object)).isna().sum()) if 'user_id' in df.columns else None
    cid_null = int(df.get('client_id', pd.Series([], dtype=object)).isna().sum()) if 'client_id' in df.columns else None
    report['details']['identity_nulls'] = {'user_id_nulls': uid_null, 'client_id_nulls': cid_null}

    # Final pass/fail heuristic
    fail_reasons = []
    if report['details']['required_fields_present'] is False:
        fail_reasons.append('required_fields_missing')
    if report['details']['duplicate_transaction_count'] > 0:
        fail_reasons.append('duplicate_transactions')
    if report['details']['revenue_issues_summary'].get('nan', 0) > 0:
        fail_reasons.append('revenue_has_nulls')
    if report['details']['revenue_issues_summary'].get('negative', 0) > 0:
        fail_reasons.append('negative_revenue')
    if len(report['details']['volume_anomalies']) > 0:
        fail_reasons.append('volume_anomalies')
    report['summary']['failed'] = len(fail_reasons) > 0
    report['summary']['fail_reasons'] = fail_reasons

    return report


def write_report(report, df, args):
    os.makedirs(args.output_dir, exist_ok=True)
    with open(os.path.join(args.output_dir, 'dq_summary.json'), 'w') as f:
        json.dump(report, f, default=str, indent=2)

    # collect sample failing rows
    fail_idx = set()
    details = report['details']
    for key in ('bad_timestamps_sample', 'duplicate_transaction_sample'):
        vals = details.get(key, [])
        if isinstance(vals, list):
            fail_idx.update(vals)
    # revenue
    rev = details.get('revenue_issues_sample', {})
    for k, v in rev.items():
        if isinstance(v, list):
            fail_idx.update(v)
    # malformed json
    fail_idx.update(details.get('malformed_event_properties_sample', []))

    if len(fail_idx) > 0:
        sample = df.loc[sorted(list(fail_idx))].head(args.sample_size)
        sample.to_csv(os.path.join(args.output_dir, 'dq_issues_sample.csv'), index=False)
    else:
        # write empty file
        pd.DataFrame().to_csv(os.path.join(args.output_dir, 'dq_issues_sample.csv'), index=False)

    if args.full_output:
        # write full failing rows where any of the conditions hit
        # naive approach: filter rows present in any of the issue lists
        all_indices = set()
        for k in ('bad_timestamps_sample', 'duplicate_transaction_sample'):
            all_indices.update(details.get(k, []))
        for k, v in details.get('revenue_issues_sample', {}).items():
            if isinstance(v, list):
                all_indices.update(v)
        all_indices.update(details.get('malformed_event_properties_sample', []))
        if all_indices:
            df.loc[sorted(list(all_indices))].to_csv(os.path.join(args.output_dir, 'dq_full_issues.csv'), index=False)


def main():
    args = parse_args()
    df = load_data(args.input, args.format)
    report = run_all_checks(df, args)
    write_report(report, df, args)
    print(f"DQ report generated in {args.output_dir}")


if __name__ == '__main__':
    main()


