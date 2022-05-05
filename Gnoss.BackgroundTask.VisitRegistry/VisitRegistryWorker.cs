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
        private readonly ILogger<VisitRegistryWorker> _logger;
        private readonly ConfigService _configService;

        public VisitRegistryWorker(ILogger<VisitRegistryWorker> logger, ConfigService configService, IServiceScopeFactory scopeFactory) : base(logger, scopeFactory)
        {
            _logger = logger;
            _configService = configService;
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
            controladores.Add(new Controller_UDPListener(mSocketsList[puerto], puerto, ScopedFactory, _configService));

            controladores.Add(new Controller_ProcessarLista(numVisitasHilo, numHilosAbiertos, minutosAntesProcesar, mSocketsList[puerto], puerto, horasProcesarVisitasVirtuoso, ScopedFactory, _configService));


            return controladores;
        }
    }
}
