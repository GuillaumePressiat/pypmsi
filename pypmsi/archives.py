
import polars
import zipfile

def adezip(path, archive, liste = None):
    path_to_zip_file = path + '/' + archive
    
    if liste is None:
        with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extractall(path)
    else:
        temp = archive.split('.')
        finess,annee,mois = temp[0:3]
        for i in liste:
            i = str(finess) + '.' + str(annee) + '.' + str(mois) + '.' + i
        with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
            zip_ref.extract(liste, path)

