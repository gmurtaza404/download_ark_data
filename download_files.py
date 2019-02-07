import os
import glob
from itertools import chain
import wget
from multiprocessing import Pool

path = "/media/murtaza/Storage/downloads/data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2017"

MODULE_FILES_TO_DOWNLOAD = 0






def make_module_folders(modules):
    os.chdir("downloads")
    for module in modules:
        try:
            os.mkdir(module)
        except:
            print "directory {} already exists...".format(module)
    
    os.chdir("..")

def copy_relevant_files(path, module_names):
    cycles = map(lambda x: path+'/'+x , os.listdir(path) )
    path_to_files = []
    cycles = filter( (lambda x: "html" not in x), cycles)
    
    for cycle in cycles:
        path_to_files = path_to_files + map( lambda x: cycle+'/'+x, os.listdir(cycle) )

    path_to_files = filter( (lambda x: "html" not in x and "md5" not in x), path_to_files)

    print path_to_files
    for module in module_names:
        files_to_copy = filter( (lambda x: module in x), path_to_files )
        for file_to_copy in files_to_copy: 
            file_name = file_to_copy.split("/")[-1]
            print "Copying {} ...".format(file_name)
            os.system("cp {} ./downloads/{}/{}".format(file_to_copy, module, file_name))
            #print "cp {} ./downloads/{}/{}".format(file_to_copy, module, file_name)




def update_links_list(modules):
    for module in modules:
        list_to_download = map(lambda x: (x.split("/")[-1], x) ,open("./download_links/{}.txt".format(module)).read().split("\n"))
        list_to_download = filter(lambda (x,y): x!= '', list_to_download)

        downloaded = os.listdir("./downloads/{}".format(module))
        
        #print list_to_download
        
        #print downloaded
        list_remaining = filter((lambda (x,y): x not in downloaded), list_to_download)
        list_remaining = map((lambda (x,y): y), list_remaining)
        
        #print list_remaining
        with open("./download_links/{}.txt".format(module), "wb") as f:
            for link in list_remaining:
                f.write(link + "\n")




def download_file(link):
    file_name = link.split("/")[-1]
    module_name = file_name.split(".")[5]
    filename = wget.download(link, "./downloads/{}/{}".format(module_name,file_name))
    return True
    



def main():
    module_names = map( (lambda x: x.split(".")[0] ),os.listdir("./download_links"))   
    #make_module_folders(module_names)    
    #copy_relevant_files(path, module_names)
    update_links_list(module_names)
    list_to_download = open("./download_links/{}.txt".format(module_names[MODULE_FILES_TO_DOWNLOAD])).read().split("\n")
    list_to_download = filter(lambda x: x!= '', list_to_download)

    
    p = Pool(5)

    print p.map(download_file, list_to_download)
    

    #download_file('http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2017/cycle-20171121/daily.l7.t1.c006153.20171122.sin-sg.warts.gz')



main()


