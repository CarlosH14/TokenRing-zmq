import zmq
import sys, os
import json
import time
from zmq.backend import has
import hashlib

def sha_cad(cadena):
    sha1 = hashlib.sha1(cadena.encode())
    b16sha = sha1.hexdigest()
    b10sha = int(b16sha, 16)
    return b10sha

def sha_arch(arch):
    sha1 = hashlib.sha1(arch)
    b16sha = sha1.hexdigest()
    b10sha = int(b16sha, 16)
    return b10sha

class Rango:
    def __init__(self,lb,ub):
        self.lb = lb
        self.ub = ub
    def primerCaso(self):
        return self.lb > self.ub
    def miembro(self, id):
        if self.primerCaso():
            return (id >= self.lb and id < 1<<160) or (id >= 0 and id < self.ub )
        else:
            return id >= self.lb and id < self.ub
    def enCadena(self):
        if self.primerCaso():
            return '[' + str(self.lb) + ' , 2^160) U [' + '0 , ' +  str(self.ub) + ')'
        else:
            return '[' + str (self.lb) + ' , ' + str(self.ub) + ')'

if __name__ == '__main__':
    nombre = sys.argv[1]
    funcion = sys.argv[2]
    aux = sys.argv[3]
    n_servs = []
    with open("Servs.txt", 'r') as file:
        for i in range(5):
            n_servs.append(file.readline())
    servs = []
    for num in range(1,6):
        context = zmq.Context()
        socket = context.socket( zmq.REQ )
        socket.connect( 'tcp://127.0.0.{}:800{}'.format(num, num) )
        servs.append( socket )
    sha_servs = []
    s_servs ={}
    for n in range(5):
        sha_one = sha_cad( n_servs[n] )
        sha_servs.append( sha_one )
        s_servs[sha_one] = servs[n]
    sha_servs.sort()
    rangos = []
    for n in range( len(sha_servs)-1 ):
        lb = sha_servs[n]
        ub = sha_servs[n+1]
        rangos.append( Rango( lb,ub ) )
    rangos.append( Rango( sha_servs[4], sha_servs[0] ) )


    if funcion == "subir":
        print("Subiendo: {}".format(aux))
        nombre = nombre.encode('utf-8')
        funcion = funcion.encode('utf-8')
        indexacion = aux.split('.')[0]
        indexacion = indexacion+'.index'
        with open( aux , 'rb' ) as file:
            with open( indexacion, 'a' ) as index:
                index.write( aux+'\n' )
                arch = file.read(1048576)
                while True:
                    if not len(arch):
                        break
                    sha_p = sha_arch( arch )
                    for r in rangos:
                        if r.miembro(sha_p):
                            index.write(str(sha_p)+'\n')
                            socket = s_servs.get(r.lb)
                            aux = aux.encode('utf-8')
                            socket.send_multipart([nombre, funcion, aux])
                            aux=aux.decode('utf-8')
                            socket.recv_string()
                            socket.send_multipart([arch])
                            socket.recv_string()
                            arch = file.read(1048576)
                            break
    elif funcion == "descargar":
        print("Descargando: {}".format(aux))
        nombre = nombre.encode('utf-8')
        funcion = funcion.encode('utf-8')
        with open(aux, 'r' ) as index:
            aux2  = index.readline()
            aux =  aux2.split('\n')[0]
            with open(aux, 'ab') as file:
                while True:
                    sha_p = index.readline()
                    if ( len(sha_p)==0 ):
                        break
                    sha_p = int(sha_p)
                    for r in rangos:
                        if r.miembro(sha_p):
                            socket = s_servs.get(r.lb)
                            aux= sha_p
                            aux=str(aux)
                            aux = aux.encode('utf-8')
                            socket.send_multipart([nombre, funcion, aux])
                            aux=aux.decode('utf-8')
                            aux=int(aux)
                            arch = socket.recv_multipart()
                            file.write(arch[0])
                            break
