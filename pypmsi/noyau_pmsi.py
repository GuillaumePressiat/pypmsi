
from pypmsi.mco.irsa import irsa
from pypmsi.mco.irum import irum
from pypmsi.mco.iano_mco import iano_mco
from pypmsi.mco.fichcomps import imed_mco
from pypmsi.mco.fichcomps import idmi_mco
from pypmsi.mco.irsf import irsf
from pypmsi.mco.irsfa import irsfa







class noyau_pmsi:
    def __init__(self, **kwargs):
        self.finess = kwargs.get('finess')
        self.annee  = kwargs.get('annee')
        self.mois  = kwargs.get('mois')
        self.path  = kwargs.get('path')


    def update_args(self, **kwargs):
        
        if 'annee' in kwargs:
            self.annee = kwargs.get('annee')
        
        if 'finess' in kwargs:
            self.finess = kwargs.get('finess')
        
        if 'mois' in kwargs:
            self.mois = kwargs.get('mois')
        
        if 'path' in kwargs:
            self.path = kwargs.get('path')
        
        return self

    def irsa(self, typi = 4, tdiag = True, **kwargs):
        self = self.update_args(**kwargs)
        return irsa(finess = self.finess, annee = self.annee, mois = self.mois, path = self.path, typi = typi, tdiag = tdiag)

    def irum(self, typi = 3, tdiag = True, **kwargs):
        self = self.update_args(**kwargs)
        return irum(finess = self.finess, annee = self.annee, mois = self.mois, path = self.path, typi = typi, tdiag = tdiag)

    def iano_mco(self, typano = "out", **kwargs):
        self = self.update_args(**kwargs)
        return iano_mco(finess = self.finess, annee = self.annee, mois = self.mois, path = self.path, typano = typano)
    
    def imed_mco(self, typmed = "out", **kwargs):
        self = self.update_args(**kwargs)
        return imed_mco(finess = self.finess, annee = self.annee, mois = self.mois, path = self.path, typmed = typmed)

    def idmi_mco(self, typdmi = "out", **kwargs):
        self = self.update_args(**kwargs)
        return idmi_mco(finess = self.finess, annee = self.annee, mois = self.mois, path = self.path, typdmi = typdmi)

    def irsf(self, ini = True, **kwargs):
        self = self.update_args(**kwargs)
        return irsf(finess = self.finess, annee = self.annee, mois = self.mois, path = self.path, ini = ini)

    def irsfa(self, ini = True, **kwargs):
        self = self.update_args(**kwargs)
        return irsfa(finess = self.finess, annee = self.annee, mois = self.mois, path = self.path)




class chemin_pmsi:
    def __init__(self, **kwargs):
        self.filepath = kwargs.get('filepath')
        self.annee  = kwargs.get('annee')

    def update_args(self, **kwargs):
        if 'annee' in kwargs:
            self.annee = kwargs.get('annee')
        if 'filepath' in kwargs:
            self.mois = kwargs.get('filepath')

        return self

    def read_rsa(self, typi = 1, tdiag = True, **kwargs):
        self = self.update_args(**kwargs)
        return irsa(filepath = self.filepath, annee = self.annee, finess = None, mois = None, path = None, typi = typi, tdiag = tdiag)

    def read_rum(self, typi = 1, tdiag = True, **kwargs):
        self = self.update_args(**kwargs)
        return irum(filepath = self.filepath, annee = self.annee, finess = None, mois = None, path = None, typi = typi, tdiag = tdiag)

    def read_ano_mco(self, typano = "out",  **kwargs):
        self = self.update_args(**kwargs)
        return iano_mco(filepath = self.filepath, annee = self.annee, finess = None, mois = None, path = None, typano = typano)

    def read_med_mco(self, typmed = "out",  **kwargs):
        self = self.update_args(**kwargs)
        return imed_mco(filepath = self.filepath, annee = self.annee, finess = None, mois = None, path = None, typmed = typmed)

    def read_dmi_mco(self, typdmi = "out",  **kwargs):
        self = self.update_args(**kwargs)
        return idmi_mco(filepath = self.filepath, annee = self.annee, finess = None, mois = None, path = None, typdmi = typdmi)

    def read_rsf(self, ini = True, **kwargs):
        self = self.update_args(**kwargs)
        return irsf(filepath = self.filepath, annee = self.annee, finess = None, mois = None, path = None, ini = ini)

    def read_rsfa(self, **kwargs):
        self = self.update_args(**kwargs)
        return irsfa(filepath = self.filepath, annee = self.annee, finess = None, mois = None, path = None)

