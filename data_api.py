import os
import requests
import pandas as pd
import csv
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import json
from math import *
import geopy.distance


def check_date_format(date_str):
    try:
        # Convertir la chaîne de caractères en objet datetime
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
    
def get_site_id(key,api_token,num_departement,pm):
    """
    _Cette fonction permet de récupérer les sites mesurant le pm specifié, par département_

    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        api_token (_str_): _clés d'accées à l'api_
        num_departement (_str_): _Numéro du département_
        pm (_str_): _type de polluant (PM10 ou PM2.5)_
    Returns:
        _liste_: _liste des sites de mesure par département_
    """
    # label_court_polluant = ['PM10','PM2.5']
    if not(isinstance(num_departement, str)) or (len(num_departement) != 2):
        print("Veuillez entrer le bon numéro de département")
        return None
    # if (pm not in label_court_polluant):
    #     print("Veuillez choisir PM10 ou PM2.5")
    #     return None
    sites = {}
    site_id = []
    Commune = []
    Departement = []
    Coordonnees = []
    
    url = f'{key}sites?api_token={api_token}&departements={num_departement}&label_court_polluant={pm}&en_service=1'
    
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
    sites['coord'] = Coordonnees

    
    df = pd.DataFrame(data=sites)
    return df

def get_mesure_id(key,api_token,site_id,pm,num_departement):
    """
    _Cette fonction permet de récupérer les mesures du pm specifié, par département et par site_

    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        api_token (_str_): _clés d'accées à l'api_
        site_id (_str_): _id su site_
        pm (_str_): _type de polluant (PM10 ou PM2.5)_
        num_departement (_str_): _Numéro du département_
        
    Returns:
        _DataFrame_: Un dataFrame contenant les informations de chaque mesure sur le site _
    """
    
    # label_court_polluant = ['PM10','PM2.5']
    site_id_departement = list(get_site_id(key,api_token,num_departement,pm)['site_id'])
    # if (pm not in label_court_polluant):
    #     return None

    if (site_id not in site_id_departement):
        return None
    
    if not(isinstance(num_departement, str)) or (len(num_departement) != 2):
        return None
    
    liste_mesure = []
    coordonnees = []
    label_site = []
    date_debut = []
    
    mesures_id = {}
    url = f'{key}mesures?api_token={api_token}&sites={site_id}&label_court_polluant={pm}&en_service=1'
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

def get_csv(key, valeur, path, api_token, pm, date_debut, date_fin,num_departement,site_id,period=7):
    """
    Cette fonction permet d'abord de vérifier si le site est bien dans le départemnt num
    et puis de récupérer un tableau de mesures de pm dans le site situé dans la commune pour une période d'une année
    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        valeur (_str_): _type des valeurs de mesure_
        path (_fichier_): _fichier où on va mettre le fichier csv_
        api_token (_str_): _clés d'accées à l'api_
        pm (_str_): _type de polluant (PM10 ou PM2.5) _
        date_debut (_date_): _date de début de mesure_
        date_fin (_date_): _date de fin de mesure_
        num_departement (_str_): _Numéro du département_
        site_id (_str_): _id du site de mesure_

    Returns:
        _tableau csv contenant des mesure horaires du polluant pm à partir de la date de début jusqu'à la date de fin_
    """
    valeurs = ['horaire','journaliere','mensuelle','annuelle']
    # label_court_polluant = ['PM10','PM2.5']
    site_id_departement = list(get_site_id(key,api_token,num_departement,pm)['site_id'])
    
    mesures_id = list(get_mesure_id(key,api_token,site_id,pm,num_departement)['mesure_id'])
    
    ourdata_year = []
    
    if not(isinstance(num_departement, str)) or (len(num_departement) != 2):
        return None
    if (valeur not in valeurs): 
        return None
    # if (pm not in label_court_polluant):
    #     return None
    if (not check_date_format(date_debut)) or (not check_date_format(date_fin)):
        return None
    if (site_id not in site_id_departement):
        return None
    
    dates = pd.date_range(start=date_debut, end=date_fin,periods=period)
    list_months = [str(dates[i])[:10] for i in range(len(list(dates)))]
    
    current_month_start_id = 0
    current_month_end_id = 1
    
    while (current_month_end_id <= len(list_months)-1):
        url = f"{key}valeurs/{valeur}?api_token={api_token}&valeur_brute=1&sites={site_id}&date_debut={list_months[current_month_start_id]}&date_fin={list_months[current_month_end_id]}&label_court_polluant={pm}&timezone_csv=+Europe%2FParis"
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
    csv_file_name = f'{path}\\site_{site_id}{date_debut}--{date_fin}_{valeur}_{pm}_data.csv'
    with open(csv_file_name, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(csvheader)
        for data in ourdata_year:
            writer.writerows(data)

    data = pd.read_csv(csv_file_name, delimiter=',')
    return data

def get_distance_sites(key,api_token,num_departement,pm):
    """_summary_
    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        api_token (_str_): _clés d'accées à l'api_
        site_id (_str_): _id su site_
        pm (_str_): _type de polluant (PM10 ou PM2.5)_
        num_departement (_str_): _Numéro du département_
    Returns:
        _DataFrame_: _Un tableau ayant pour chaque point sa distance avec les autres points de mesure du même département_
    """
    sites = get_site_id(key,api_token,num_departement,pm)
    dict = {'Sites':list(sites['site_id'])}
    i = 0
    # while i<len(sites['site_id']):
    for i in range(len(sites['site_id'])):
        distance = []
        for k in range(len(sites['site_id'])):
                coord_i = sites[sites['site_id'] == sites['site_id'][i]]['coord'].values[0]
                coord_k = sites[sites['site_id'] == sites['site_id'][k]]['coord'].values[0]
                dist = geopy.distance.geodesic(coord_i,coord_k)
                # dist_formatted = "{:.2f}".format(dist)
                distance.append(dist)
        dict[sites['site_id'][i]] = distance
        # i = i + 1
    df = pd.DataFrame(data=dict)
    return df

def get_distance_measures(key,api_token,site_id,pm,num_departement):
    """_summary_
    Args:
        key (_str_): _Chemin commun pour accéder à l'url_
        api_token (_str_): _clés d'accées à l'api_
        site_id (_str_): _id su site_
        pm (_str_): _type de polluant (PM10 ou PM2.5)_
        num_departement (_str_): _Numéro du département_
    Returns:
        _DataFrame_: _Un tableau ayant pour chaque point sa distance avec les autres points de mesure du même site_
    """
    site = get_mesure_id(key, api_token,site_id,pm,num_departement)
    dict = {'Distance':site['mesure_id']}
    i = 0
    while i<len(site['mesure_id']):
        distance = []
        for k in range(len(site['mesure_id'])):
            coord_i = site[site['mesure_id']==site['mesure_id'][i]]['coord'].values[0]
            coord_k = site[site['mesure_id']==site['mesure_id'][k]]['coord'].values[0]
            dist = geopy.distance.geodesic(coord_i,coord_k)
            distance.append(dist)
        dict[site['mesure_id'][i]] = dist
        i=i+1
    df = pd.DataFrame(data=dict)
    return(df)

