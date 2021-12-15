import json
import zmq
import os
import hashlib
import sys
import os.path as path

def sha_arch(arch):
    sha1 = hashlib.sha1(arch)
    b16sha = sha1.hexdigest()
    b10sha = int(b16sha, 16)
    return b10sha

def sha_cad(cadena):
    sha1 = hashlib.sha1(cadena.encode())
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
    n_serv = sys.argv[1]
    port = 'tcp://127.0.0.{}:800{}'.format(n_serv, n_serv)
    context = zmq.Context()
    socket = context.socket( zmq.REP )
    socket.bind( port)

    if n_serv != 5:
        socket2 = context.socket( zmq.REQ )
        socket2.connect('tcp://127.0.0.{}:800{}'.format(int(n_serv)+1, int(n_serv)+1))

    dir = './serv{}/'.format(n_serv)
    if not path.isdir(dir):
        os.makedirs(dir)
    print('*** Servidor {} encendido ***'.format(n_serv))

    n_servs = []
    with open("Servs.txt", 'r') as file:
        for i in range(5):
            n_servs.append(file.readline())
    sha_servs = []
    for n in range(5):
        sha_one = sha_cad( n_servs[n] )
        sha_servs.append( sha_one )
    sha_servs.sort()
    rangos = []
    for n in range( len(sha_servs)-1 ):
        lb = sha_servs[n]
        ub = sha_servs[n+1]
        rangos.append(Rango( lb,ub ))
    rangos.append(Rango( sha_servs[4], sha_servs[0]))

    while True:
        m = socket.recv_multipart()
        nombre=m[0]
        nombre=nombre.decode('utf-8')
        funcion=m[1]
        funcion=funcion.decode('utf-8')
        aux=m[2]
        aux=aux.decode('utf-8')



        if funcion == "subir":
            print("Subiendo: {}".format(aux))
            socket.send_string('fin')
            arch = socket.recv_multipart()
            sha_p = sha_arch(arch[0])
            if rangos[int(n_serv)-1].miembro(sha_p):
                socket.send_string('fin')
                sha_p = sha_arch(arch[0])
                n_arch = dir+str(sha_p)
                print("Queda")
                with open(n_arch, 'ab') as file:
                    file.write(arch[0])
            elif n_serv != 5:
                nombre = nombre.encode('utf-8')
                funcion = funcion.encode('utf-8')
                aux = aux.encode('utf-8')
                print("Pasa")
                socket2.send_multipart([nombre, funcion, aux])
                socket2.recv_string()
                socket2.send_multipart([arch[0]])
                socket2.recv_string()
                socket.send_string('fin')
        elif funcion == "devuelta":
            print("Devolver")
            nombre = nombre.encode('utf-8')
            funcion = "devuelta"
            funcion = funcion.encode('utf-8')
            socket.send_multipart([nombre, funcion, arch])
        elif funcion == "descargar":
            print("Descargando: {}".format(aux))
            aux=int(aux)
            sha_p = aux
            print("sha1: {}".format(sha_p))
            print(nombre)
            print(funcion)
            if rangos[int(n_serv)-1].miembro(sha_p):
                n_arch = dir+str(sha_p)
                with open(n_arch, 'rb') as file:
                    arch = file.read()
                    nombre = nombre.encode('utf-8')
                    funcion = "devuelta"
                    funcion = funcion.encode('utf-8')
                    socket.send_multipart([nombre, funcion, arch])
            else:
                nombre = nombre.encode('utf-8')
                funcion = "descargar"
                funcion = funcion.encode('utf-8')
                aux=str(aux)
                aux = aux.encode('utf-8')
                socket2.send_multipart([nombre, funcion, aux])
