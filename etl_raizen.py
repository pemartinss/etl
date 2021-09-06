#Importando as bibliotecas que utilizaremos para realizar o processo de ETL
import pandas as pd 
from datetime import datetime

xls = pd.ExcelFile('vendas-combustiveis-m3(2).xls')


def exec_transform(nome,folhas):
  ''' Função que irá executar o processo de ETL no nosso dataframe'''
  for count, value in enumerate(nome):
       extract = ler_dataframe(folhas[count])
       transform = transformar(extract)
       load = create_csv(transform,nome[count])

def ler_dataframe(folha):
   '''Função que realiza a leitura da tabela e completa os campos nulos com 0, usando o parametro folhas que especifica as sheets da planilha em questão'''
   df1 = pd.read_excel(xls, folha).fillna(0)
   return df1

def transform_unit(x):

  '''Função que separa a string da unidade do produto diesel ou derivados e o retorna entre os parenteses'''

  return x[x.find('(')+1:x.find(')')]

def transformar(df):

  ''' Função que vai rodar as transformações na tabela, utilizando a tabela  xls como parametro para as transformações e armazenando na variavel df_transformado'''
  realizar_check(df)
  df = df[['COMBUSTÍVEL','ANO','ESTADO','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']]
  df.columns = ['Product','Year','Uf','1','2','3','4','5','6','7','8','9','10','11','12']
  df = df.melt(id_vars=['Product','Year','Uf'],var_name= 'month',value_name='volume')
  df.Year = df.Year.astype(str)
  df['year_month'] = df['Year'] + '-' +  df['month'] + '-01'
  df['year_month'] = pd.to_datetime(df['year_month'])
  df['unit'] = df['Product'].apply(transform_unit)
  df['created_at'] = datetime.now()
  df_transformado = df[['year_month','Uf','Product','unit','volume','created_at']]
  return df_transformado

def create_csv(df_transformado, nome):
    ''' Função que criara o arquivo csv utilizando como parametro a tabela (df_transformado) já com todas as transformações'''
    df_transformado.sort_values(by=['Uf','Product'])
    filename = '%s.csv' % nome
    print(filename)
    df_transformado.to_csv(filename, header=True, index=False)
    return df_transformado

def realizar_check(df):
    '''Função que vai realizar a checagem dos dados iniciais da tabela, comparando se a soma total dos meses bate com o campo total da tabela, utilizando o dataframe xls como parametro'''
    result = df.iloc[0,4:16].sum()
    result_2 = df.iloc[0,3]
    
    if result != result_2:
      print('Os dados estão divergentes, será necessário verificar!')
    
    else:
      print('Os dados estão corretos!')

exec_transform(['derivados','diesel'],[1,2])
    

