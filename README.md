# Gnoss.BackgroundTask.VisitRegistry.OpenCORE

Aplicación de segundo plano que expone un puerto UDP, al que la Web le envía las visitas a cada recurso. Este servicio se encarga de agruparlas y enviarlas cada poco tiempo al servicio Visit Cluster para que las contabilice en base de datos.

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
visitregistry:
    image: visitregistry
    env_file: .env
    ports:
     - 8145:1745
    environment:
     virtuosoConnectionString: ${virtuosoConnectionString}
     acid: ${acid}
     base: ${base}
     RabbitMQ__colaServiciosWin: ${RabbitMQ}
     redis__redis__ip__master: ${redis__redis__ip__master}
     redis__redis__bd: ${redis__redis__bd}
     redis__redis__timeout: ${redis__redis__timeout}
     redis__recursos__ip__master: ${redis__recursos__ip__master}
     redis__recursos__bd: ${redis__recursos_bd}
     redis__recursos__timeout: ${redis__recursos_timeout}
     redis__liveUsuarios__ip__master: ${redis__liveUsuarios__ip__master}
     redis__liveUsuarios__bd: ${redis__liveUsuarios_bd}
     redis__liveUsuarios__timeout: ${redis__liveUsuarios_timeout}
     idiomas: "es|Español,en|English"
     Servicios__urlBase: "https://servicios.test.com"
     connectionType: "0"
     intervalo: "100"
     puertoUDP: 1745
    volumes:
     - ./logs/visitregistry:/app/logs 
     - ./visitregistry/Recursos:/app/recursos
     - ./visitregistry/Votos:/app/Votos
     - ./visitregistry/Visitas:/app/Visitas

```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.Platform.Deploy
