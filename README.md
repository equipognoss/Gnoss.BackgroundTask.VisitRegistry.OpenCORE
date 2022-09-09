![](https://content.gnoss.ws/imagenes/proyectos/personalizacion/7e72bf14-28b9-4beb-82f8-e32a3b49d9d3/cms/logognossazulprincipal.png)

# Gnoss.BackgroundTask.VisitRegistry.OpenCORE

![](https://github.com/equipognoss/Gnoss.BackgroundTask.VisitRegistry.OpenCORE/workflows/BuildVisitRegistry/badge.svg)

Aplicación de segundo plano que expone un puerto UDP, al que la Web le envía las visitas a cada recurso. Este servicio se encarga de agruparlas y enviarlas cada poco tiempo al servicio Visit Cluster para que las contabilice en base de datos.

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
visitregistry:
    image: gnoss/visitregistry
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

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE

## Código de conducta
Este proyecto a adoptado el código de conducta definido por "Contributor Covenant" para definir el comportamiento esperado en las contribuciones a este proyecto. Para más información ver https://www.contributor-covenant.org/

## Licencia
Este producto es parte de la plataforma [Gnoss Semantic AI Platform Open Core](https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE), es un producto open source y está licenciado bajo GPLv3.
