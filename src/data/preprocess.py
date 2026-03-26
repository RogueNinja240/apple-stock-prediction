import pandas as pd
import re
import os

def fix_encoding_and_clean(text):
    if pd.isna(text):
        return ""
    text = str(text)
    
    # 1. Dictionary of common Mojibake encoding errors to fix
    replacements = {
        "â€™": "'",
        "â€œ": '"',
        "â€\x9d": '"',
        "â€": '"',
        "Ã©": "é",
        "â€”": "-",
        "â€“": "-",
        "Â": "",
        "â€˜": "'"
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
        
    # 2. Remove URLs (often left behind in raw text)
    text = re.sub(r'http\S+|www\.\S+', '', text)
    
    # 3. Clean up weird spacing caused by the CSV breaks
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def run_cleaning():
    print("🧹 Starting Data Cleaning Process...")
    input_file = "data/raw/AAPL_news_data.csv"
    output_file = "data/processed/AAPL_cleaned_news.csv"

    # Load data (keeping headers intact)
    df = pd.read_csv(input_file, names=['Date', 'news_articles'], header=0)

    # ==========================================
    # 1. FIX THE SPILLOVER / BROKEN ROWS
    # ==========================================
    # Force convert to datetime. If it's text (like "too"), it becomes NaT (Not a Time)
    df['Parsed_Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # Identify the broken rows where text spilled into the Date column
    spilled_mask = df['Parsed_Date'].isna()
    
    # Move the spilled text from 'Date' over to 'news_articles'
    df.loc[spilled_mask, 'news_articles'] = df.loc[spilled_mask, 'Date'].astype(str) + " " + df.loc[spilled_mask, 'news_articles'].fillna('')

    # Forward-fill the missing dates from the valid rows above them
    df['Parsed_Date'] = df['Parsed_Date'].ffill()

    # ==========================================
    # 2. RE-COMBINE AND CLEAN
    # ==========================================
    print("   → Re-aligning broken rows and fixing Mojibake encoding...")
    
    # Group by the correct date and stitch the separated articles back together
    clean_df = df.groupby('Parsed_Date')['news_articles'].apply(
        lambda x: ' | '.join(x.dropna().astype(str))
    ).reset_index()

    # Apply our regex and encoding cleaner
    clean_df['news_articles'] = clean_df['news_articles'].apply(fix_encoding_and_clean)

    # Rename the date column back to standard
    clean_df.rename(columns={'Parsed_Date': 'Date'}, inplace=True)
    
    # Ensure chronological order
    clean_df = clean_df.sort_values('Date')

    # ==========================================
    # 3. SAVE TO PROCESSED FOLDER
    # ==========================================
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    clean_df.to_csv(output_file, index=False)
    
    print(f"✅ Successfully cleaned {len(clean_df)} daily records!")
    print(f"💾 Saved to: {output_file}")

if __name__ == "__main__":
    run_cleaning()