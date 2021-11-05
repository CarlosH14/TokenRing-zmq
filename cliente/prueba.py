import sys, os
import string
import random
import hashlib

def crear_cade(n_cad, t_cad): #n_cad: Número de cadenas, t_cad: tamaño de cadenas
    cads = []
    for x in range(n_cad):
        c = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(t_cad))
        cads.append(c)
    return cads

def c_servs(n):
    print("Número de servidores: {}".format(n))
    n_serv = n
    servs = crear_cade(int(n_serv), 7)
    print("Nombres: {}".format(servs))
    return servs

with open("Servs.txt" , 'w' ) as file:
    servs=c_servs(5)
    for s in servs:
        file.write(str(s)+'\n')
