from conectar import baseDatos

#Creamos los usuarios
usuarios = {
    "usuario:1": {"nombre": "Juan", "edad": 28},
    "usuario:2": {"nombre": "María", "edad": 34},
    "usuario:3": {"nombre": "Pedro", "edad": 45},
    "usuario:4": {"nombre": "Ana", "edad": 23},
    "usuario:5": {"nombre": "Luis", "edad": 37}
}

# Ofertas
ofertas = {
    "oferta:101": {"titulo": "Desarrollador Python", "empresa": "TechCorp", "ubicacion": "Madrid"},
    "oferta:102": {"titulo": "Analista de Datos", "empresa": "DataSolutions", "ubicacion": "Barcelona"},
    "oferta:103": {"titulo": "Ingeniero DevOps", "empresa": "CloudServices", "ubicacion": "Valencia"},
    "oferta:104": {"titulo": "Diseñador UX/UI", "empresa": "Creativa", "ubicacion": "Sevilla"},
    "oferta:105": {"titulo": "Project Manager", "empresa": "GlobalCorp", "ubicacion": "Bilbao"}
}

# Insertar usuarios en Redis
for clave, datos in usuarios.items():
    baseDatos.hset(clave, mapping=datos)

# Insertar ofertas en Redis
for clave, datos in ofertas.items():
    baseDatos.hset(clave, mapping=datos)


clicks_manual = [
    (1, 101, 5),
    (1, 102, 2),
    (2, 101, 3),
    (2, 103, 4),
    (3, 104, 1),
    (4, 105, 6),
    (5, 102, 2)
]

for usuario_id, oferta_id, numero_de_clicks in clicks_manual:
    hash_clicks = f"usuario:{usuario_id}:clicks"
    campo_oferta = f"oferta:{oferta_id}"
    baseDatos.hset(hash_clicks, campo_oferta, numero_de_clicks)


