# -*- coding: utf-8 -*-
import os
chemin_dossier = 'C:\\Users\\sst\\Downloads\\REITERATION\\source_conversation'
fichiers = os.listdir(chemin_dossier)
nom_du_fichier = "réitération_avril"
intervalle = '60T'
n = 3

import pandas as pd
fichier_excel = [fichier for fichier in fichiers if fichier.endswith('.xlsx')]
dfs = []
for fichier_excel in fichier_excel :
    chemin_fichier = os.path.join(chemin_dossier,fichier_excel)
    df = pd.read_excel(chemin_fichier)
    dfs.append(df)
data_final = pd.concat(dfs,ignore_index=True)

def converti_date(data_final) :
    data_final['date'] = pd.to_datetime(data_final['created_at'], format='%Y-%m-%d %H:%M:%S').dt.date
    data_final['year_month'] = data_final['created_at'].dt.to_period('M')
    return data_final
data_final = converti_date(data_final)

data_final_tranche = data_final.copy()
liste_date = list(data_final_tranche['date'].unique())
data_jour = []
for date in liste_date :
    data_jour.append(data_final_tranche[data_final_tranche['date']==date])
    
tranche = data_final.copy()
def transformation(tranche, intervalle='H'):
    tranche['created_at'] = pd.to_datetime(tranche['created_at'])
    tranche['year_month'] = tranche['created_at'].dt.to_period('M')
    
    hourly_counts = tranche.groupby([tranche['year_month'],
                                     pd.Grouper(key='created_at', freq='D'),
                                     pd.Grouper(key='created_at', freq=intervalle)])['first_content_author_id'].count()
    
    hourly_unique = tranche.groupby([tranche['year_month'],
                                     pd.Grouper(key='created_at', freq='D'),
                                     pd.Grouper(key='created_at', freq=intervalle)])['first_content_author_id'].nunique()
    
    source_name = tranche.groupby([tranche['year_month'],
                                 pd.Grouper(key='created_at', freq='D'),
                                 pd.Grouper(key='created_at', freq=intervalle)])['source_name'].nunique()

    def transform(grouped_data, name):
        grouped_data.index.names = ['Year_Month', 'Date', 'Hour']
        grouped_data = pd.DataFrame(grouped_data)
        grouped_data.columns = [name]
        grouped_data = grouped_data.reset_index()
        grouped_data['Hour'] = grouped_data['Hour'].dt.strftime('%H:%M:%S')
        grouped_data = grouped_data.set_index(['Year_Month', 'Date', 'Hour'])
        return grouped_data

    hourly_counts = transform(hourly_counts, 'Conversations')
    hourly_unique = transform(hourly_unique, 'Conversations_Unique')
    source_name = transform(source_name, 'Source_Name')

    data_tranche = pd.concat([hourly_counts, hourly_unique], axis=1)
    data_tranche['Reiteration'] = round(100 * ((data_tranche['Conversations'] - data_tranche['Conversations_Unique']) / data_tranche['Conversations']), 2)
    data_tranche = pd.concat([data_tranche, source_name], axis=1)
    
    return data_tranche

tranche = data_final.copy()
data_tranche = transformation(tranche)

data_final = converti_date(data_final)
list_source = list(data_final['source_name'].unique())
list_dataframes = []
for i in range(len(list_source)) :
    df_prog = data_final[data_final['source_name']==list_source[i]].copy()
    list_dataframes.append(df_prog)
    
def triage(data_final) :
    pd.options.mode.copy_on_write = True
    reiteration = data_final['date'].unique()
    reiteration = pd.Series(reiteration)
    callers = []
    for i in range(len(reiteration)) :
        jours_glissant = reiteration[i:i+n]
        if len(jours_glissant) < n :
            break
        jours_glissant = list(jours_glissant)
        callers.append(jours_glissant)
    n_callers = []
    for i in range(len(callers)) :
        n_callers.append(data_final[data_final['date'].isin(callers[i])])
    n_unique = [caller[-1] for caller in callers]
    for caller, date in zip(n_callers, n_unique):
        caller['date'] = date
    n_callers = pd.concat(n_callers)
    reiteration = n_callers.set_index('date')
    return reiteration

top_contact = triage(data_final)
top_contact = top_contact.groupby(level=0)['first_content_author_id'].value_counts()
top_contact = pd.DataFrame(top_contact)
top_contact = top_contact.rename(columns={'first_content_author_id':'count'}) 
top_contact = top_contact.reset_index()
top_contact = top_contact[top_contact['count']>=2]
top_contact = top_contact.sort_values(by=['count'],ascending=False)
top_unique = top_contact['first_content_author_id'].unique()
top_contact_unique = data_final[data_final['first_content_author_id'].isin(top_unique)]
top_contact_unique = top_contact_unique.sort_values(by=['first_content_author_id','created_at'],ascending=True).drop(columns=['date'])

reiteration = triage(data_final)
reiteration_par_source = []
for i in range(len(list_dataframes)) :
    reit_p = reiteration[reiteration['source_name']==list_source[i]].copy()
    reiteration_par_source.append(reit_p)
    
def resultat(reiteration) :
    temp = reiteration.drop(columns=['source_name','created_at','first_content_author_id','year_month'])
    temp = temp.groupby(level=0).value_counts()
    temp = pd.DataFrame(temp)
    temp = temp.rename(columns={0:'Conversations'})
    reit = reiteration.drop(columns=['source_name','created_at'])
    reit = reit.groupby(level=0).nunique()
    Reiteration = pd.concat([temp,reit],axis=1)
    Reiteration = Reiteration.rename(columns={'first_content_author_id':'Conversations_Unique'})
    Reiteration['Reiteration'] = round(100*((Reiteration['Conversations']-Reiteration['Conversations_Unique'])/Reiteration['Conversations']),2)
    return Reiteration

globales = resultat(reiteration)
resultat_par_source = list_dataframes.copy()
for i in range(len(list_dataframes)) :
    resultat_par_source[i] = resultat(reiteration_par_source[i])
    resultat_par_source[i]['source_name'] = list_source[i]
    
data_finals = data_final.drop(columns=['date','year_month'])
with pd.ExcelWriter("C:\\Users\\sst\\Downloads\\REITERATION\\"+nom_du_fichier+".xlsx") as writer :
    data_finals.to_excel(writer, sheet_name='Data',index=False)
    data_tranche.to_excel(writer,sheet_name='SummaryH',index=True)
    globales.to_excel(writer,sheet_name='SummaryD',index=True)
    top_contact.to_excel(writer,sheet_name='top_id_conversations',index=False)
    top_contact_unique.to_excel(writer,sheet_name='top_id_converstions_Details',index=False)
    for i in range(len(resultat_par_source)) :
        resultat_par_source[i].to_excel(writer,sheet_name=list_source[i],index=True)