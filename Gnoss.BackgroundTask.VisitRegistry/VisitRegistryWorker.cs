using Es.Riam.Gnoss.Elementos.Suscripcion;
using Es.Riam.Gnoss.ServicioActualizacionOffline;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.VisitRegistry
{
    public class VisitRegistryWorker : Worker
    {
        private volatile Dictionary<int, List<string>> mSocketsList = new Dictionary<int, List<string>>();
        private readonly ConfigService _configService;
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;

        public VisitRegistryWorker(ConfigService configService, IServiceScopeFactory scopeFactory, ILogger<VisitRegistryWorker> logger, ILoggerFactory loggerFactory) : base(logger, scopeFactory)
        {
            _configService = configService;
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }


        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            int numVisitasHilo = _configService.ObtenerNumVisitasHilo();
            int numHilosAbiertos = _configService.ObtenerNumHilosAbiertos();
            int minutosAntesProcesar = _configService.ObtenerMinutosAntesProcesar();
            int horasProcesarVisitasVirtuoso = _configService.ObtenerHorasProcesarVisitasVirtuoso();
            ControladorServicioGnoss.INTERVALO_SEGUNDOS = _configService.ObtenerIntervalo();
            int puerto = _configService.ObtenerPuertoUDP();

            mSocketsList.Add(puerto, new List<string>());
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new Controller_UDPListener(mSocketsList[puerto], puerto, ScopedFactory, _configService, mLoggerFactory.CreateLogger<Controller_UDPListener>(), mLoggerFactory));

            controladores.Add(new Controller_ProcessarLista(numVisitasHilo, numHilosAbiertos, minutosAntesProcesar, mSocketsList[puerto], puerto, horasProcesarVisitasVirtuoso, ScopedFactory, _configService, mLoggerFactory.CreateLogger<Controller_ProcessarLista>(), mLoggerFactory));


            return controladores;
        }
    }
}
