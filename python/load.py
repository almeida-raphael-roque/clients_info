import pandas as pd
import awswrangler as awr

class Load:
    file_names = [
        "despesas_assistencia_analitico",
        "despesas_casco_analitico",
        "despesas_terceiro_analitico",
        "despesas_vidros_analitico",
        "faturas",
        "relatorio_comissao_completo",
        "sinistros"
    ]

    def __init__(self, file_name):
        self.file_name = file_name
        self.today = pd.Timestamp.today().date()
        

    def read_sql(self):
        sql_query_path = fr"C:\Users\raphael.almeida\Documents\Processos\resumo_cooperados\sql\{self.file_name}.sql"

        # Tenta múltiplas codificações para garantir compatibilidade
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        sql_query = None
        
        for encoding in encodings:
            try:
                with open(sql_query_path, 'r', encoding=encoding) as file:
                    sql_query = file.read()
                break
            except UnicodeDecodeError:
                continue
        
        # Se ainda falhar, usa utf-8 com tratamento de erros
        if sql_query is None:
            with open(sql_query_path, 'r', encoding='utf-8', errors='replace') as file:
                sql_query = file.read()
        
        self.df = awr.athena.read_sql_query(sql_query, database='silver')
        return self.df

    def load_csv(self):

        csv_bkp = fr"C:\Users\raphael.almeida\Documents\Processos\resumo_cooperados\csv\{self.today}_{self.file_name}.csv"
        csv_sharepoint = fr"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Resumo dos Cooperados\{self.file_name}.csv"
        
        # Limpa caracteres problemáticos antes de salvar (converte para string e remove caracteres inválidos)
        for col in self.df.select_dtypes(include=['object']).columns:
            self.df[col] = self.df[col].astype(str).apply(
                lambda x: x.encode('utf-8', errors='ignore').decode('utf-8') if pd.notna(x) and x != 'nan' else x
            )
        
        self.df.to_csv(csv_bkp, index=False, encoding='utf-8-sig')
        self.df.to_csv(csv_sharepoint, index=False, encoding='utf-8-sig')

    @classmethod
    def run_load(cls):
        for fname in cls.file_names:
            try:
                loader = cls(fname)
                loader.read_sql()
                loader.load_csv()
                print(f"✓ {fname} processado com sucesso")
            except Exception as e:
                print(f"✗ Erro ao processar {fname}: {str(e)}")
                continue

if __name__ == '__main__':
    Load.run_load()



        
