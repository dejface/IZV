"""
File: download.py
Author: David Oravec (xorave05)
Description:
    - contains class DataDownloader, which downloads and process data about
      accidents in Czech Republic by year
"""
import os, requests, re, zipfile, csv, io, pickle, gzip
import numpy as np
from bs4 import BeautifulSoup


class DataDownloader:
    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="data", cache_filename="data_{}.pkl.gz"):
        self.url = url
        self.regions = {
            "PHA" : "00.csv",   #praha
            "STC" : "01.csv",   #stredocesky
            "JHC" : "02.csv",   #jihocesky
            "PLK" : "03.csv",   #plzensky
            "KVK" : "19.csv",   #karlovarsky
            "ULK" : "04.csv",   #ustecky
            "LBK" : "18.csv",   #liberecky
            "HKK" : "05.csv",   #kralovehradeckz
            "PAK" : "17.csv",   #pardubicky
            "OLK" : "14.csv",   #olomoucky
            "MSK" : "07.csv",   #moravskoslezky
            "JHM" : "06.csv",   #jihomoravsky
            "ZLK" : "15.csv",   #zlisnky
            "VYS" : "16.csv"    #vysocina
        }
        self.string_list = [
        "p1",               #int identifikacne cislo
        "p36",              #int druh pozemnej komunikacie
        "p37",              #int cislo pozemnej komunikacie
        "p2a",              #str den,mesiac,rok
        "weekday(p2a)",     #int den v tyzdni
        "p2b",              #int cas
        "p6",               #int druh nehody
        "p7",               #int druh zrazky vozidiel
        "p8",               #int druh pevnej prekazky
        "p9",               #int charakter nehody
        "p10",              #int zavinenie nehody
        "p11",              #int alkohol u vinnika
        "p12",              #int hlavna pricina nehody
        "p13a",             #int mrtve osoby
        "p13b",             #int tazko zranene osoby
        "p13c",             #int lahko zranene osoby
        "p14",              #int celkova hmotna skoda
        "p15",              #int povrch vozovky
        "p16",              #int stav povrchu vozovky v dobe nehody
        "p17",              #int stav komunikacie
        "p18",              #int poveternostne podmienky v dobe nehody
        "p19",              #int viditelnost
        "p20",              #int rozhladove pomery
        "p21",              #int delenie komunikacie
        "p22",              #int situovanie nehody na komunikacii
        "p23",              #int riadenie premavky v dobe nehody
        "p24",              #int miestna uprava prednosti v jazde
        "p27",              #int specificke miesta a objekty na mieste nehody
        "p28",              #int smerove pomery
        "p34",              #int pocet zucastnenych vozidiel
        "p35",              #int miesto dopravnej nehody
        "p39",              #int druh krizujucej komunikacie
        "p44",              #int druh vozidla
        "p45a",             #int vyrobna znacka motoroveho vozidla
        "p47",              #str rok vyroby vozidla
        "p48a",             #int charakteristika vozidla
        "p49",              #int smyk
        "p50a",             #int vozidlo po nehode
        "p50b",             #int unik prepravovanych hmot
        "p51",              #int sposob vybratia osob z vozidla
        "p52",              #int smer jazdy alebo postavenie vozidla
        "p53",              #int skoda na vozidle
        "p55a",             #int kategoria vodica
        "p57",              #int stav vodica
        "p58",              #int vonkajsie ovplyvnenie vodica
        "a",                #float
        "b",                #float
        "d",                #float suradnice x
        "e",                #float suradnice y
        "f",                #float
        "g",                #float
        "h",                #str
        "i",                #str
        "j",                #str
        "k",                #str
        "l",                #str
        "n",                #int
        "o",                #str
        "p",                #str P a O si nie som isty
        "q",                #str
        "r",                #int
        "s",                #int
        "t",                #str
        "p5a",              #int
        "region"]           #str skratka regionu  

        self.processed_regions = {}         #variable to store already processed regions
        self.global_list = [[] for _ in range(65)]
        self.string_index = set([3,34,51,52,53,54,55,57,58,59,63,64])
        self.float_index = set([45,46,47,48,49,50])


        if os.path.isdir(folder):
            self.folder = folder
        else:
            os.mkdir(folder)
            self.folder = folder

        zipped_data = os.listdir(self.folder)
        pattern = re.compile(r'^.*\.zip')  
        #storing which zip files are downloaded
        self.zipped_data = [zipped_data[i] for i, x in enumerate(zipped_data) if pattern.search(x)]
        pattern = re.compile(r'^.*\.gz')
        #storing cached files
        self.found_gz = set([zipped_data[i] for i, x in enumerate(zipped_data) if pattern.search(x)])
        self.cache_filename = cache_filename        
        
    """ Method for downloading necessary data from 'url'. Downloading only the latest
        zip of the year."""

    def download_data(self):
        #make a request with headers to avoid robot detection
        req = requests.post(self.url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'})
        soup = BeautifulSoup(req.text, "html.parser")

        #parsing html and getting the latest file from each year
        for item in soup.find_all('a'):
            if item.contents[0] == 'ZIP':
                if item.parent.parent.td.text.find('Prosinec') != -1:
                    if item.get('href').replace('data/','') in self.zipped_data:
                        continue
                    else:
                        self.zipped_data.append(item.get('href'))
                elif item.parent.parent.td.text.find('2020') != -1:
                    if len(self.zipped_data) <= 4:
                        self.zipped_data.append(item.get('href'))
                    else:
                        if self.zipped_data[-1].find('2020') != -1:
                            self.zipped_data.pop(len(self.zipped_data)-1)
                            self.zipped_data.append(item.get('href'))
    
        #if the zip file already exists, do nothing, else download zip
        for i in range(len(self.zipped_data)):
            if os.path.exists(os.path.join(self.folder,self.zipped_data[i].replace('data/',''))):
                continue   
            else:    
                req = requests.get(self.url + self.zipped_data[i], stream=True)
                with open(self.zipped_data[i], 'wb') as fd:
                    for chunk in req.iter_content(chunk_size=128):
                        fd.write(chunk)

    """ Method for parsing specific region data. Returns tuple(list_of_headers,list_of_nparrays)"""

    def parse_region_data(self, region):
        #if any of .zip data files is missing, download it
        if len(self.zipped_data) != 5:
            self.download_data()

        list_of_np_arrays = []
        temp_arrays = [[] for _ in range(65)]
        
        for j in range(len(self.zipped_data)):
            with zipfile.ZipFile(os.path.join(self.folder,self.zipped_data[j].replace('data/',''))) as zf:
                with zf.open(self.regions[region],'r') as f:
                    reader = csv.reader(io.TextIOWrapper(f,'windows-1250'),delimiter=';')
                    for line in reader:
                        for i in range(len(line)+1):
                            if i == 5:  #hour column
                                hours = int(line[i][:2])
                                minutes = int(line[i][2:])
                                if hours > 24 or minutes > 59:  #invalid hour or minute
                                    line[i] = '-1'
                            if i not in self.string_index and i not in self.float_index:
                                try:
                                    line[i] = int(line[i])
                                except:
                                    line[i] = -1
                            elif i in self.float_index:
                                try:
                                    line[i] = float(line[i].replace(',','.'))
                                except:
                                    line[i] = '-1.0' 
                            if i == 64: #appending region to list
                                temp_arrays[i].append(region)
                                self.global_list[i].append(region)
                            else:
                                temp_arrays[i].append(line[i])
                                self.global_list[i].append(line[i])

        #change every list to numpy array and specify its type

        for i in range(len(temp_arrays)):
            if i == 3:
                arr = np.array(temp_arrays[i],dtype='datetime64')
            elif i not in self.string_index and i not in self.float_index:
                arr = np.array(temp_arrays[i],dtype='int64')
            elif i in self.float_index:
                arr = np.array(temp_arrays[i],dtype='float64')
            else:
                arr = np.array(temp_arrays[i])
            list_of_np_arrays.append(arr)

        return (self.string_list, list_of_np_arrays)

    """ Method for getting specific region data. Returns tuple(list_of_headers,list_of_nparrays)"""

    def get_list(self, regions = None):
        result = []

        #if none of regions are specified, make a full list of regions in CR

        if regions is None:
            regions = list(self.regions.keys())    

        for i in range(len(regions)):
            curr_region = regions[i]

            #if current region is already processed in memory, continue

            if curr_region in self.processed_regions:
                continue
            elif curr_region not in self.processed_regions:     #if region is not processed yet
                pattern = self.cache_filename.format(curr_region) 
                if pattern in self.found_gz:    #if region is in cache, load it 
                    with gzip.open(os.path.join(self.folder,pattern),'rb') as f: 
                        self.processed_regions[curr_region] = pickle.load(f)
                    for j in range(len(self.processed_regions[curr_region][1])):
                        self.global_list[j] += self.processed_regions[curr_region][1][j].tolist()
                else:   #else process it
                        data = self.parse_region_data(curr_region)
                        with gzip.open(os.path.join(self.folder,pattern),'wb') as f:
                            pickle.dump(data,f)
                        self.processed_regions[curr_region] = data
        
        for k in range(len(self.global_list)):
            if k == 3:
                arr = np.array(self.global_list[k],dtype='datetime64')
            elif k not in self.string_index and k not in self.float_index:
                arr = np.array(self.global_list[k],dtype='int64')
            elif k in self.float_index:
                arr = np.array(self.global_list[k],dtype='float64')
            else:
                arr = np.array(self.global_list[k])
            result.append(arr)

        return (self.string_list,result)

if __name__ == "__main__":
    my_class = DataDownloader().get_list(["PLK","PAK","KVK"])
    print("Names of columns:\n")
    for i in range(len(my_class[0])):
        print(my_class[0][i], end=' | ')

    print("\n\nNumber of entries: ",len(my_class[1][0]))
    print("\nProcessed regions are: ",set(my_class[1][64]))
