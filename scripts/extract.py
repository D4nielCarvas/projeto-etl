import pandas as pd
import requests

def extract_api_data() -> dict:

    url = "https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL"
    response = requests.get(url)
    response.raise_for_status()  # Levanta erro HTTP caso a requisição falhe
    data = response.json()
    
    #Extraindo as cotações desejadas
    cotacoes = {
        'USD': float(data['USDBRL']['bid']),
        'EUR': float(data['EURBRL']['bid'])
    }
    return cotacoes

def extract_csv_data(filepath: str) -> pd.DataFrame:

    df = pd.read_csv(filepath)
    return df

if __name__ == "__main__":
    # Teste rápido do módulo
    print("Testando extração da API...")
    print(extract_api_data())