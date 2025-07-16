# Data Extraction
def run_extraction():
    try:
        data = pd.read_json(r'MarketsData.json')
        print('Data extracted successfully')
    except Exception as e:
        print(f'Error extracting data: {e}')