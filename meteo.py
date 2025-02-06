import os
import requests
import pandas as pd
import csv
import json
from datetime import datetime
from math import *



def check_date_format(date_str):
    try:
    # Convertir la chaîne de caractères en objet datetime
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def get_site_id_temp_hum(key,api_token,num_departement,param):
    """
    _Cette fonction permet de récupérer les sites mesurant le pm specifié, par département_

    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        api_token (_str_): _clés d'accées à l'api_
        num_departement (_str_): _Numéro du département_
        param (_str_): _ (humidité ou température)_
    Returns:
        _liste_: _liste des sites de mesure par département_   
    """
    parametres = ['temperature',f'humidit%C3%A9+relative']
    if not(isinstance(num_departement, str)) or (len(num_departement) != 2):
        print("Veuillez entrer le bon numéro de département")
        return None
    if (param not in parametres):
        print("Veuillez choisir PM10 ou PM2.5")
        return None
    sites = {}
    site_id = []
    Commune = []
    Departement = []
    Coordonnees = []
    
    url = f'{key}sites?api_token={api_token}&departements={num_departement}&label_court_polluant={param}&en_service=1'
    
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {api_token}'}
    response = requests.get(url, headers=headers)
    response_json = response.json()
    if 'data' not in response_json:
        return None
    csv_data = response_json['data']
    if not csv_data:
        return None
    
    for i in range((len(csv_data))):
        site_id.append(csv_data[i]['id'])
        Commune.append(csv_data[i]['label_commune'])
        Departement.append(csv_data[i]['label_departement'])
        geojson = json.loads(csv_data[i]['geojson'])
        Coordonnees.append(geojson['coordinates'])
    sites['site_id'] = site_id
    sites['Departement'] = Departement
    sites['Commune'] = Commune
    sites['coord'] = Coordonnees
    
    df = pd.DataFrame(data=sites)
        
    return df


def get_mesure_id_temp_hum(key,api_token,site_id,param,num_departement):
    """
    _Cette fonction permet de récupérer les mesures du pm specifié, par département et par site_

    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        api_token (_str_): _clés d'accées à l'api_
        site_id (_str_): _id su site_
        pm (_str_): _(temperature ou humidite)_
        num_departement (_str_): _Numéro du département_
        
    Returns:
        _DataFrame_: Un dataFrame contenant les informations de chaque mesure sur le site _
    """
    
    site_id_departement = list(get_site_id_temp_hum(key,api_token,num_departement,param)['site_id'])
   
    if (site_id not in site_id_departement):
        return None
    
    if not(isinstance(num_departement, str)) or (len(num_departement) != 2):
        return None
    
    liste_mesure = []
    coordonnees = []
    label_site = []
    date_debut = []
    
    mesures_id = {}
    url = f'{key}mesures?api_token={api_token}&sites={site_id}&label_court_polluant={param}&en_service=1'
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {api_token}'}
    response = requests.get(url, headers=headers)
    response_json = response.json()
    if 'data' not in response_json:
        return None
    csv_data = response_json['data']
    if not csv_data:
        return None
    
    for i in range((len(csv_data))):
        liste_mesure.append(csv_data[i]['id'])
        geojson = json.loads(csv_data[i]['geojson'])
        coordonnees.append(geojson['coordinates'])
        label_site.append(csv_data[i]['label_site'])
        date_debut.append(csv_data[i]['date_debut'])
    mesures_id['mesure_id'] = liste_mesure
    mesures_id['coord'] = coordonnees
    mesures_id['label_site'] = label_site
    mesures_id['date_debut'] = date_debut
    data = pd.DataFrame(data=mesures_id)
    return data
    

def get_csv_temp_hum(key, valeur, path, api_token, param, date_debut, date_fin,num_departement,site_id,mesure_id,period=5):
    """
    Cette fonction permet d'abord de vérifier si le site est bien dans le départemnt num
    et puis de récupérer un tableau de mesures de pm dans le site situé dans la commune pour une période d'une année
    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        valeur (_str_): _type des valeurs de mesure_
        path (_fichier_): _fichier où on va mettre le fichier csv_
        api_token (_str_): _clés d'accées à l'api_
        param (_str_): _(temperature ou humidite) _
        date_debut (_date_): _date de début de mesure_
        date_fin (_date_): _date de fin de mesure_
        num_departement (_str_): _Numéro du département_
        site_id (_str_): _id du site de mesure_
        mesure

    Returns:
        _tableau csv contenant des mesure horaires du parametre param à partir de la date de début jusqu'à la date de fin_
    """
    valeurs = ['horaire','journaliere','mensuelle','annuelle']
    mesures_id = list(get_mesure_id_temp_hum(key,api_token,site_id,param,num_departement)['mesure_id'])
    
    ourdata_year = []
    
    if not(isinstance(num_departement, str)) or (len(num_departement) != 2):
        return None
    if (valeur not in valeurs): 
        return None
    if (not check_date_format(date_debut)) or (not check_date_format(date_fin)):
        return None
    if (mesure_id not in mesures_id):
        return None

    
    dates = pd.date_range(start=date_debut, end=date_fin,periods=period)
    list_months = [str(dates[i])[:10] for i in range(len(list(dates)))]
    
    current_month_start_id = 0
    current_month_end_id = 1
    
    while (current_month_end_id <= len(list_months)-1):
        url = f"{key}valeurs/{valeur}?api_token={api_token}&mesures={mesure_id}&valeur_brute=1&date_debut={list_months[current_month_start_id]}&date_fin={list_months[current_month_end_id]}&timezone_csv=+Europe%2FParis"
        headers = {'Accept': 'application/json', 'Authorization': f'Bearer {api_token}'}
    
        response = requests.get(url, headers=headers)
        response_json = response.json()

        if 'data' not in response_json:
            return None

        csv_data = response_json['data']
        if not csv_data:
            return None

        csvheader = list(csv_data[0].keys())
        ourdata = []

        for x in csv_data:
            listing = [x[k] for k in csvheader]
            ourdata.append(listing)
        
        ourdata_year.append(ourdata)
        current_month_start_id+=1
        current_month_end_id+=1
    # print(ourdata_year)
    csv_file_name = f'{path}\\site_{site_id}{date_debut}--{date_fin}_{valeur}_{param}_data.csv'
    with open(csv_file_name, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(csvheader)
        for data in ourdata_year:
            writer.writerows(data)

    data = pd.read_csv(csv_file_name, delimiter=',')
    data = data.drop(['validation','id_poll_ue','label_court_unite','unite','site_id','label_court_polluant','label_unite','site_label','code_polluant'],axis=1)
    return data
    
def get_merged_temp_hum(data_hum,data_temp):
     merged = pd.merge(data_hum,data_temp,on='date',how='inner',suffixes=('_hum','_temp'))
     merged = merged[merged['date','valeur_hum','valeur_temp']]
     return merged   
    
    
    
    
    
    
    
    