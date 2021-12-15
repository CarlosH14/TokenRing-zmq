# TokenRing-zmq

Proyecto de Arquitectura Cliente / Servidor - UTP

Para usarlo:

Correr los servidores en distintas terminales:   
En cada terminal -> python3 servidor.py num

num -> 1, 2, 3, 4, 5

Correr el cliente:  
En terminal -> python3 cliente.py nombreUsuario nombreFuncion auxiliar

# Lista de funciones:

subir -> sube un archivo de la carpeta cliente a las carpetas del servidor

ejemplo:  
python3 cliente.py carlos subir prueba.txt

----------------------------------------------------------------------------------------------------------------------------------------------------------------
descargar (no terminado) -> descarga un archivo de las carpetas del servidor a la carpeta del cliente.

La extensión del archivo debe ser .index independientemente del tipo de archivo

Al descargarlo recupera la extensión original

ejemplo:  
python3 cliente.py carlos descargar anuncio.index

----------------------------------------------------------------------------------------------------------------------------------------------------------------
