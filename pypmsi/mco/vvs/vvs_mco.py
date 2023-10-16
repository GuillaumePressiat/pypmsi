
import polars as pl

def vvs_mco(rsa_v, ano_v):
    return rsa_v.join(ano_v, on = 'cle_rsa', how = 'left')
