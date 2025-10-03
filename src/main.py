import pandas as pd
import argparse
import json
from pathlib import Path
import os

def load_data_from_dirs(dir_paths: list[str]) -> pd.DataFrame:
    """
    Loads all JSON and CSV files from a list of directories into a single pandas DataFrame.
    Treats all columns from CSVs as strings to preserve identifiers with leading zeros.
    """
    all_dfs = []
    for dir_path in dir_paths:
        search_path = Path(dir_path)
        if not search_path.is_dir():
            print(f"Warning: Directory not found: {dir_path}. Skipping.")
            continue
            
        files_to_process = list(search_path.rglob('*.json')) + list(search_path.rglob('*.csv'))
        
        for file_path in files_to_process:
            try:
                if file_path.suffix == '.json':
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            all_dfs.append(pd.DataFrame(data))
                        else:
                            all_dfs.append(pd.DataFrame([data]))
                elif file_path.suffix == '.csv':
                    all_dfs.append(pd.read_csv(file_path, dtype=str))
            except Exception as e:
                print(f"Warning: Could not read or parse {file_path}. Error: {e}. Skipping.")
    
    if not all_dfs:
        return pd.DataFrame()
    
    return pd.concat(all_dfs, ignore_index=True)

def calculate_goal_2_metrics(claims_df: pd.DataFrame, reverts_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates metrics based on npi and ndc dimensions using pandas."""
    print("Executing Goal 2: Calculating core metrics...")
    
    valid_claims_df = claims_df[~claims_df['id'].isin(reverts_df['claim_id'])].copy()
    
    valid_claims_df['price'] = pd.to_numeric(valid_claims_df['price'], errors='coerce')
    valid_claims_df['quantity'] = pd.to_numeric(valid_claims_df['quantity'], errors='coerce')
    valid_claims_df.dropna(subset=['price', 'quantity'], inplace=True)
    valid_claims_df = valid_claims_df[valid_claims_df['quantity'] > 0]
    valid_claims_df['unit_price'] = valid_claims_df['price'] / valid_claims_df['quantity']

    metrics_df = valid_claims_df.groupby(['npi', 'ndc']).agg(
        avg_price=('unit_price', 'mean'),
        total_price=('price', 'sum')
    ).reset_index()

    fills_count = claims_df.groupby(['npi', 'ndc']).agg(fills=('id', 'count')).reset_index()

    reverted_claims_info = claims_df[claims_df['id'].isin(reverts_df['claim_id'])][['id', 'npi', 'ndc']]
    reverts_count = reverted_claims_info.groupby(['npi', 'ndc']).agg(reverted=('id', 'count')).reset_index()

    result_df = pd.merge(fills_count, reverts_count, on=['npi', 'ndc'], how='left')
    result_df = pd.merge(result_df, metrics_df, on=['npi', 'ndc'], how='left')

    result_df['reverted'] = result_df['reverted'].fillna(0).astype(int)
    result_df[['avg_price', 'total_price']] = result_df[['avg_price', 'total_price']].fillna(0)
    
    result_df['avg_price'] = result_df['avg_price'].round(2)
    result_df['total_price'] = result_df['total_price'].round(2)

    print("Goal 2 complete.")
    return result_df

def calculate_goal_3_recommendations(claims_df: pd.DataFrame, pharmacy_df: pd.DataFrame, reverts_df: pd.DataFrame):
    """Calculates the top 2 chains with the lowest average unit price per drug using Pandas."""
    print("Executing Goal 3: Calculating top 2 chain recommendations...")

    valid_claims_df = claims_df[~claims_df['id'].isin(reverts_df['claim_id'])].copy()
    valid_claims_df['price'] = pd.to_numeric(valid_claims_df['price'], errors='coerce')
    valid_claims_df['quantity'] = pd.to_numeric(valid_claims_df['quantity'], errors='coerce')
    valid_claims_df.dropna(subset=['price', 'quantity'], inplace=True)
    valid_claims_df = valid_claims_df[valid_claims_df['quantity'] > 0]
    valid_claims_df['unit_price'] = valid_claims_df['price'] / valid_claims_df['quantity']

    chain_prices_df = pd.merge(valid_claims_df, pharmacy_df, on='npi')
    
    avg_chain_price_df = chain_prices_df.groupby(['ndc', 'chain']).agg(avg_price=('unit_price', 'mean')).reset_index()
    avg_chain_price_df['avg_price'] = avg_chain_price_df['avg_price'].round(2)

    top_2_chains = avg_chain_price_df.sort_values('avg_price').groupby('ndc').head(2)

    def format_chains(group):
        return group[['chain', 'avg_price']].rename(columns={'chain': 'name'}).to_dict('records')

    result = top_2_chains.groupby('ndc').apply(format_chains, include_groups=False).reset_index(name='chain')
    
    print("Goal 3 complete.")
    return json.loads(result.to_json(orient='records'))

def calculate_goal_4_common_quantities(claims_df: pd.DataFrame):
    """Calculates the most prescribed quantities for each drug using Pandas."""
    print("Executing Goal 4: Understanding most common quantities...")

    claims_df['quantity'] = pd.to_numeric(claims_df['quantity'], errors='coerce')
    claims_df.dropna(subset=['quantity'], inplace=True)
    
    quantity_counts = claims_df.groupby(['ndc', 'quantity']).size().reset_index(name='count')
    
    top_5_quantities = quantity_counts.sort_values('count', ascending=False).groupby('ndc').head(5)
    
    result = top_5_quantities.groupby('ndc')['quantity'].apply(list).reset_index(name='most_prescribed_quantity')
    
    print("Goal 4 complete.")
    return json.loads(result.to_json(orient='records'))

def main():
    parser = argparse.ArgumentParser(description="Pharma Analytics Data Pipeline")
    parser.add_argument('--pharmacy_dirs', nargs='+', required=True, help='List of directories for pharmacy data')
    parser.add_argument('--claims_dirs', nargs='+', required=True, help='List of directories for claims data')
    parser.add_argument('--reverts_dirs', nargs='+', required=True, help='List of directories for reverts data')
    parser.add_argument('--output_dir', type=str, required=True, help='Directory to save output JSON files')
    args = parser.parse_args()

    print("Executing Goal 1: Reading data from provided directories...")
    pharmacy_df = load_data_from_dirs(args.pharmacy_dirs)
    claims_df = load_data_from_dirs(args.claims_dirs)
    reverts_df = load_data_from_dirs(args.reverts_dirs)

    # Checks
    if pharmacy_df.empty or 'npi' not in pharmacy_df.columns:
        print("Error: No valid pharmacy data with an 'npi' column found. Cannot proceed.")
        return
    if claims_df.empty or 'npi' not in claims_df.columns:
        print("Error: No valid claims data with an 'npi' column found. Cannot proceed.")
        return
    
    # Data Cleaning and Filtering
    pharmacy_df.dropna(subset=['npi'], inplace=True)
    claims_df.dropna(subset=['npi', 'id'], inplace=True)
    
    initial_claim_count = len(claims_df)
    claims_df = claims_df[claims_df['npi'].isin(pharmacy_df['npi'])]
    
    print(f"Loaded {len(pharmacy_df)} unique pharmacies and {len(reverts_df)} reverts.")
    print(f"Loaded {initial_claim_count} initial claims, {len(claims_df)} remain after filtering for known pharmacies.")

    if claims_df.empty:
        print("Warning: No claims correspond to the pharmacies provided. No metrics to calculate.")
        return

    os.makedirs(args.output_dir, exist_ok=True)

    # --- Goal 2 ---
    goal_2_output = calculate_goal_2_metrics(claims_df, reverts_df)
    output_path = os.path.join(args.output_dir, 'goal_2_metrics.json')
    goal_2_output.to_json(output_path, orient='records', indent=4)
    print(f"Goal 2 output saved to {output_path}")

    # --- Goal 3 ---
    goal_3_output = calculate_goal_3_recommendations(claims_df, pharmacy_df, reverts_df)
    output_path = os.path.join(args.output_dir, 'goal_3_recommendations.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(goal_3_output, f, indent=4)
    print(f"Goal 3 output saved to {output_path}")

    # --- Goal 4 ---
    goal_4_output = calculate_goal_4_common_quantities(claims_df)
    output_path = os.path.join(args.output_dir, 'goal_4_common_quantities.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(goal_4_output, f, indent=4)
    print(f"Goal 4 output saved to {output_path}")

    print("\nPipeline finished successfully.")

if __name__ == "__main__":
    main()