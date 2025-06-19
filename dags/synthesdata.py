import pandas as pd
import numpy as np
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer
from sklearn.preprocessing import LabelEncoder
from sqlalchemy import create_engine

POSTGRES_URI = "postgresql+psycopg2://postgres:Mika2u7w@localhost:5432/data_filter"
engine = create_engine(POSTGRES_URI)

df = pd.read_sql("SELECT * FROM cks_notsyn", con=engine)

dependent_columns = [
    'full_kato_name', 'kato_2_name', 'kato_2',
    'kato_4_name', 'kato_4', 'kato_42', 'count_iin'
]
df_model = df.drop(columns=dependent_columns, errors='ignore')

categorical_columns = df_model.select_dtypes(include='object').columns.tolist()
encoders = {}
for col in categorical_columns:
    encoder = LabelEncoder()
    df_model[col] = encoder.fit_transform(df_model[col].astype(str))
    encoders[col] = encoder

metadata = SingleTableMetadata()
metadata.detect_from_dataframe(df_model)

model = GaussianCopulaSynthesizer(metadata)
model.fit(df_model)

synthetic = model.sample(10000)

for col, encoder in encoders.items():
    synthetic[col] = encoder.inverse_transform(synthetic[col].astype(int))

synthetic['kato_4'] = np.random.choice(df['kato_4'].unique(), size=len(synthetic))
synthetic['kato_42'] = synthetic['kato_4']
synthetic['kato_2'] = synthetic['kato_4'].astype(str).str[:4]
synthetic['count_iin'] = np.clip(
    np.random.normal(loc=3, scale=1.5, size=len(synthetic)).astype(int), 1, 15
)

kato_df = df[['kato_2', 'kato_2_name', 'kato_4', 'kato_4_name', 'full_kato_name']].drop_duplicates()

synthetic = synthetic.merge(kato_df, on='kato_4', how='left')

synthetic.to_sql("cks", con=engine, index=False, if_exists='replace')

print("DONE")
