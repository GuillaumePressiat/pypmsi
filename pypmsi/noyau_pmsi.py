
from pypmsi.mco.irsa import irsa
from pypmsi.mco.irum import irum
from pypmsi.mco.iano_mco import iano_mco
from pypmsi.mco.fichcomps import imed_mco
from pypmsi.mco.fichcomps import idmi_mco
from pypmsi.mco.fichcomps import idiap_mco
from pypmsi.mco.fichcomps import ipie_mco
from pypmsi.mco.irsf import irsf
from pypmsi.mco.irsfa import irsfa

from pypmsi.ssr.irha import irha
from pypmsi.ssr.irhs import irhs
from pypmsi.ssr.iano_ssr import iano_ssr
from pypmsi.ssr.fichcomps import imed_ssr

from pypmsi.tra.itra import itra


import copy


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

    def __repr__(self):
         return f"---- Noyau PMSI pour pypmsi :\n----\n-- - finess : {self.finess}\n-- - annee  : {self.annee}\n-- - mois   : {self.mois}\n-- - path   : {self.path}"

    def __str__(self):
         return f"-- Noyau PMSI pour pypmsi :\n----\n-- - finess : {self.finess}\n-- - annee  : {self.annee}\n-- - mois   : {self.mois}\n-- - path   : {self.path}"
  

    def irsa(self, typi = 4, tdiag = True, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        r = irsa(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typi = typi, tdiag = tdiag, n_rows = n_rows)
        return r

    def itra(self, champ = 'mco', n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return itra(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, champ = champ, n_rows = n_rows)

    def irha(self, typi = 3, tdiag = False, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irha(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typi = typi, tdiag = tdiag, n_rows = n_rows)

    def issrha(self, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return issrha(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, n_rows = n_rows)

    def irum(self, typi = 3, tdiag = True, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irum(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typi = typi, tdiag = tdiag, n_rows = n_rows)

    def irhs(self, typi = 3, tdiag = False, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irhs(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typi = typi, tdiag = tdiag, n_rows = n_rows)

    def iano_mco(self, typano = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return iano_mco(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typano = typano, n_rows = n_rows)
 
    def iano_ssr(self, typano = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return iano_ssr(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typano = typano, n_rows = n_rows)
     
    def imed_mco(self, typmed = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return imed_mco(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typmed = typmed, n_rows = n_rows)

    def imed_ssr(self, typmed = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return imed_ssr(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typmed = typmed, n_rows = n_rows)

    def idmi_mco(self, typdmi = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return idmi_mco(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typdmi = typdmi, n_rows = n_rows)

    def irsf(self, ini = True, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irsf(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, ini = ini, n_rows = n_rows)

    def irsfa(self, ini = True, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irsfa(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, n_rows = n_rows)

    def idiap_mco(self, typdiap = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return idiap_mco(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typdiap = typdiap, n_rows = n_rows)

    def ipie_mco(self, typpie = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return ipie_mco(finess = noyau.finess, annee = noyau.annee, mois = noyau.mois, path = noyau.path, typpie = typpie, n_rows = n_rows)


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

    def read_rsa(self, typi = 1, tdiag = True, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irsa(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typi = typi, tdiag = tdiag, n_rows = n_rows)

    def read_tra(self, champ = 'mco', n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return itra(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, champ = champ, n_rows = n_rows)

    def read_rha(self, typi = 1, tdiag = False, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irha(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typi = typi, tdiag = tdiag, n_rows = n_rows)

    def read_ssrha(self, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return issrha(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, n_rows = n_rows)


    def read_rum(self, typi = 1, tdiag = True, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irum(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typi = typi, tdiag = tdiag, n_rows = n_rows)

    def read_rhs(self, typi = 1, tdiag = False, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irhs(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typi = typi, tdiag = tdiag, n_rows = n_rows)

    def read_ano_mco(self, typano = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return iano_mco(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typano = typano, n_rows = n_rows)

    def read_ano_ssr(self, typano = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return iano_ssr(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typano = typano, n_rows = n_rows)

    def read_med_mco(self, typmed = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return imed_mco(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typmed = typmed, n_rows = n_rows)

    def read_med_ssr(self, typmed = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return imed_ssr(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typmed = typmed, n_rows = n_rows)

    def read_dmi_mco(self, typdmi = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return idmi_mco(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typdmi = typdmi, n_rows = n_rows)

    def read_rsf(self, ini = True, n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irsf(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, ini = ini, n_rows = n_rows)

    def read_rsfa(self,  n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return irsfa(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, n_rows = n_rows)

    def read_diap_mco(self, typdiap = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return imed_mco(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typdiap = typdiap, n_rows = n_rows)

    def read_pie_mco(self, typpie = "out", n_rows = None, **kwargs):
        noyau = copy.copy(self)
        noyau.update_args(**kwargs)
        return imed_mco(filepath = noyau.filepath, annee = noyau.annee, finess = None, mois = None, path = None, typpie = typpie, n_rows = n_rows)
