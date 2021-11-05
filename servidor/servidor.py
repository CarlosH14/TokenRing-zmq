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

if __name__ == '__main__':
    n_serv = sys.argv[1]
    port = 'tcp://127.0.0.{}:800{}'.format(n_serv, n_serv)
    context = zmq.Context()
    socket = context.socket( zmq.REP )
    socket.bind( port)
    dir = './serv{}/'.format(n_serv)
    if not path.isdir(dir):
        os.makedirs(dir)
    print('*** Servidor {} encendido ***'.format(n_serv))
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
            socket.send_string('fin')
            sha_p = sha_arch(arch[0])
            n_arch = dir+str(sha_p)
            with open(n_arch, 'ab') as file:
                file.write(arch[0])
        elif funcion == "descargar":
            print("Descargando: {}".format(aux))
            aux=int(aux)
            sha_p = aux
            print("sha1: {}".format(sha_p))
            print(aux)
            print(nombre)
            print(funcion)
            print(sha_p)
            n_arch = dir+str(sha_p)
            with open(n_arch, 'rb') as file:
                arch = file.read()
                socket.send_multipart([arch])
