import os, requests, re, zipfile, csv, io, json, pickle, gzip
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
        self.processed_regions = {}
        self.global_list = [[] for _ in range(65)]
        self.was_downloaded = False

        if os.path.isdir(folder):
            self.folder = folder
            self.path = os.path.abspath(folder)
        else:
            os.mkdir(folder)
            self.folder = folder
            self.path = os.path.abspath(folder)

        #requests_cache.install_cache(cache_filename)
        self.cache_filename = cache_filename
        
        

    def download_data(self):
        req = requests.post(self.url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'})
        soup = BeautifulSoup(req.text, "html.parser")
        class_a = []
        for item in soup.find_all('a'):
            class_a.append(item.get('href'))
        
        for i in range(len(class_a)):
            if re.findall(r"^.*\.zip$", class_a[i]):
                if os.path.exists(os.path.join(self.path,class_a[i].replace('data/',''))):
                    continue   
                else:    
                    req = requests.get(self.url + class_a[i], stream=True)
                    with open(class_a[i], 'wb') as fd:
                        for chunk in req.iter_content(chunk_size=128):
                            fd.write(chunk)
        self.was_downloaded = True

    def parse_region_data(self, region):
        if self.was_downloaded is False:
            self.download_data()
        zip_names = os.listdir(self.folder)

        oldest_files = {
            "2016" : ("",0),
            "2017" : ("",0),
            "2018" : ("",0),
            "2019" : ("",0),
            "2020" : ("",0)
        }

        string_list = [
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

        string_index = [3,34,51,52,53,54,55,57,58,59,63,64]
        float_index = [45,46,47,48,49,50]
        
        """ Get the oldest files from each year for recent stats"""
        for file in zip_names:
            if (re.match(r"^.*(2016)\.zip$",file)):
                size = os.path.getsize(os.path.join(self.folder,file))
                if size > int(oldest_files["2016"][1]):
                    oldest_files["2016"] = (file,size)
            elif (re.match(r"^.*(2017)\.zip$",file)):
                size = os.path.getsize(os.path.join(self.folder,file))
                if size > int(oldest_files["2017"][1]):
                    oldest_files["2017"] = (file,size)
            elif (re.match(r"^.*(2018)\.zip$",file)):
                size = os.path.getsize(os.path.join(self.folder,file))
                if size > int(oldest_files["2018"][1]):
                    oldest_files["2018"] = (file,size)
            elif (re.match(r"^.*(2019)\.zip$",file)):
                size = os.path.getsize(os.path.join(self.folder,file))
                if size > int(oldest_files["2019"][1]):
                    oldest_files["2019"] = (file,size)
            elif (re.match(r"^.*(2020)\.zip$",file)):
                size = os.path.getsize(os.path.join(self.folder,file))
                if size > int(oldest_files["2020"][1]):
                    oldest_files["2020"] = (file,size)

        list_of_np_arrays = []
        temp_arrays = [[] for _ in range(65)]

        for i in oldest_files:
            with zipfile.ZipFile(os.path.join(self.path,oldest_files[i][0])) as zf:
                with zf.open(self.regions[region],'r') as f:
                    reader = csv.reader(io.TextIOWrapper(f,'ISO 8859-2'),delimiter=';')
                    for line in reader:
                        for i in range(len(line)+1):
                            try:
                                if i == 6:
                                    hours = int(line[i][:2])
                                    minutes = int(line[i][2:])
                                    if hours > 24 or minutes > 59:
                                        line[i] = '-1'
                                if i not in string_index and i not in float_index:
                                    try:
                                        line[i] = int(line[i])
                                    except:
                                        line[i] = -1
                                elif i in float_index:
                                    try:
                                        line[i] = float(line[i].replace(',','.'))
                                    except:
                                        line[i] = '-1.0'
                                if i == 64:
                                    temp_arrays[i].append(region)
                                    self.global_list[i].append(region)
                                else:
                                    temp_arrays[i].append(line[i])
                                    self.global_list[i].append(line[i])
                            except:
                                pass

        for i in range(len(temp_arrays)):
            list_of_np_arrays.append(np.array(temp_arrays[i]))

        return (string_list, list_of_np_arrays)

    def get_list(self, regions = None):
        dirs = os.listdir(self.folder)
        found_gz = [dirs[i] for i, x in enumerate(dirs) if re.findall(r'^.*\.gz',x)]
        found = False
        result = []
        if regions is None:
            regions = []
            for region in self.regions.keys():
                regions.append(region)

        for i in range(len(regions)):
            found = False
            if regions[i] in self.processed_regions:
                continue
            elif regions[i] not in self.processed_regions: #ak region neni v atribute v pamati
                pattern = re.compile('data_{}.pkl.gz'.format(regions[i]))   #spravim pattern na hladanie suborov v cache
                for x in range(len(found_gz)):
                    if (re.match(pattern,found_gz[x])): #ak je pattern rovnaky ako subor
                        cached_file = gzip.open(os.path.join(self.folder,found_gz[x]),'rb') #otvaram subor
                        self.processed_regions[regions[i]] = pickle.load(cached_file)
                        cached_file.close()
                        found = True
                        for j in range(len(self.processed_regions[regions[i]][1])):
                            self.global_list[j] += self.processed_regions[regions[i]][1][j].tolist()
                if (found is False):
                    header, data = self.parse_region_data(regions[i])
                    cached_file = gzip.open(os.path.join(self.folder,self.cache_filename.format(regions[i])),'wb')
                    pickle.dump((header, data),cached_file)
                    cached_file.close()
                    self.processed_regions[regions[i]] = (header,data)

        for k in range(len(self.global_list)):
            result.append(np.array(self.global_list[k]))

        return (self.processed_regions[regions[0]][0],result)

if __name__ == "__main__":
    my_class = DataDownloader().get_list(["PLK","PAK","KVK"])
    print("Names of columns:\n")
    for i in range(len(my_class[0])):
        print(my_class[0][i], end=' | ')

    print("\n\nNumber of entries: ",len(my_class[1][0]))
    print("\nProcessed regions are: ",set(my_class[1][64]))