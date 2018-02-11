import pandas as pd
import math
import ast
def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
warnings.filterwarnings("ignore", category=RuntimeWarning)

import sys

itcont = sys.argv[1]
percentile = sys.argv[2]
repeat_donors = sys.argv[3]


import pandas as pd

data = pd.read_csv(itcont, sep="|", header=None,   error_bad_lines=False, index_col=False, dtype='unicode')
data.columns = ["CMTE_ID", "AMNDT_IND","RPT_TP","TRANSACTION_PGI","IMAGE_NUM","TRANSACTION_TP","ENTITY_TP","NAME","CITY","STATE","ZIP_CODE", "EMPLOYER","OCCUPATION","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID","TRAN_ID","FILE_NUM","MEMO_CD","MEMO_TEXT","SUB_ID"]



percentileP = open(percentile,"r")
percentilePvalue=int(percentileP.read())

open(repeat_donors, 'w').close()
open(repeat_donors, 'a')



data['ZIP_CODE_STR']=data.ZIP_CODE.astype(str)


data = data[pd.isnull(data['OTHER_ID'])]






data = data.dropna(subset = ['CMTE_ID'])
data = data.dropna(subset = ['NAME'])
data = data.dropna(subset = ['ZIP_CODE'])
data = data.dropna(subset = ['TRANSACTION_DT'])
data = data.dropna(subset = ['TRANSACTION_AMT'])


data['ZIP_CODE_STR'] = data["ZIP_CODE_STR"].str[:5]
data['ZIP_CODE_STR']=data.ZIP_CODE.astype(str)
data['TRANSACTION_AMT']=data.TRANSACTION_AMT.astype(str).astype(int)
data['TRANSACTION_DT']=data.TRANSACTION_DT.astype(str).astype(int)




data['TRANSACTION_DT_STR']=data.TRANSACTION_DT.astype(str)
data['TRANSACTION_AMT_STR']=data.TRANSACTION_AMT.astype(str)
data = data[~data['ZIP_CODE_STR'].str.contains("[a-zA-Z*#@$%^&*!|\/?,.;]").fillna(False)]
data = data[~data['TRANSACTION_DT_STR'].str.contains("[a-zA-Z*#@$%^&*!|\/?,.;]").fillna(False)]
data = data[~data['TRANSACTION_AMT_STR'].str.contains("[a-zA-Z*#@$%^&*!|\/?,.;]").fillna(False)]


data=data[data['TRANSACTION_DT_STR'].str.len() <= 8]


data['TRANSACTION_DT_STR'] = data['TRANSACTION_DT_STR'].str[-4:]




data['NAME_ZIP']=data['NAME']+'_'+data['ZIP_CODE_STR']

mask = data.NAME_ZIP.duplicated(keep=False)


new_data=data[mask]


import ast
new_data["TRANSACTION_DT_INT"]= new_data.TRANSACTION_DT_STR.apply(ast.literal_eval)

new_data = new_data[new_data['TRANSACTION_DT_INT'] >= 2015]

max_year_value=new_data['TRANSACTION_DT_INT'].loc[new_data['TRANSACTION_DT_INT'].idxmax()]




#PercentilePvalue

import math
def sortandpercentile(x):
    listx=list(x)
    sortedvalues=sorted(listx)
    Ordinalrank=(percentilePvalue/100)*len(sortedvalues)
    Ordinalrank_rounded=math.ceil(Ordinalrank)
    sortedvalues_new=sortedvalues[Ordinalrank_rounded-1]
    #print(len(sortedvalues))
    return round(sortedvalues_new)





#################################################
new_dataa=new_data

uniqueCMTE_ID=new_data.CMTE_ID.unique()
for Recipient in uniqueCMTE_ID:
    new_data=new_dataa
###############################################


    new_data=new_data[new_data['CMTE_ID'].str.contains(Recipient)]

    new_data=new_data.sort_values(by=['NAME_ZIP', 'TRANSACTION_AMT'])




#sumofvalues


    df00 = new_data[new_data['TRANSACTION_DT_INT'] >= max_year_value]



    df00 = df00.groupby(['CMTE_ID', 'NAME_ZIP'])['TRANSACTION_AMT'].sum().reset_index()


    for i in range(1, len(df00)):
        #print(df00.loc[i-1, 'TRANSACTION_AMT'])
        #print(df00.loc[i, 'TRANSACTION_AMT']+df00.loc[i-1, 'TRANSACTION_AMT'])
        df00.loc[i, 'TRANSACTION_AMT'] = df00.loc[i, 'TRANSACTION_AMT']+df00.loc[i-1, 'TRANSACTION_AMT']
    dfx=df00




#Numberofcontributions

    df1 = new_data['NAME_ZIP'].value_counts().rename_axis('NAME_ZIP').reset_index(name='Total no of contributions')





    f2=new_data.groupby('NAME_ZIP')['TRANSACTION_AMT'].apply(sortandpercentile)



    df0X = new_data[new_data['TRANSACTION_DT_INT'] >= max_year_value]

    XX_PERCENTILE_Contrib=pd.DataFrame({'percentile_contribution' : df0X.groupby('CMTE_ID')['TRANSACTION_AMT'].apply(sortandpercentile)}).reset_index()




    formerging = new_data[['NAME_ZIP','CMTE_ID']].copy()



    formerging['TRANSACTION_DT_INT'] = new_data.groupby(['NAME_ZIP'])['TRANSACTION_DT_INT'].transform(max)
    formerging.head()





#merge values to dataframe

    merged_df1 = formerging.merge(df1, how = 'inner', on = ['NAME_ZIP'])
    merged_df2 = merged_df1.merge(XX_PERCENTILE_Contrib, how = 'inner', on = ['CMTE_ID'])
    merged_df3 = merged_df2.merge(dfx, how = 'outer', on = ['CMTE_ID','NAME_ZIP'])

    merged_df3





    final_towrite=merged_df3.drop_duplicates(subset=['NAME_ZIP'])
    final_towrite




    header = ["CMTE_ID", "TRANSACTION_DT_INT", "percentile_contribution", "TRANSACTION_AMT","Total no of contributions"]
    print(Recipient)
    final_towrite.to_csv(repeat_donors, columns = header,sep='|',mode='a', index=False, header=False)
