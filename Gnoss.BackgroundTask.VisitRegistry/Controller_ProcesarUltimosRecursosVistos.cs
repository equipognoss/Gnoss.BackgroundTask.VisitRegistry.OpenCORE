using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Util;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.ServicioActualizacionOffline
{
    public class Controller_ProcesarUltimosRecursosVistos : ControladorServicioGnoss
    {
        List<string> mDocumentosVisitados;

        public Controller_ProcesarUltimosRecursosVistos(List<string> pDocumentosVistos, IServiceScopeFactory serviceScopeFactory, ConfigService configService) : base(serviceScopeFactory, configService)
        {
            mDocumentosVisitados = pDocumentosVistos;
        }

        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {

            try
            {
                Dictionary<Guid, List<Guid>> listaRecursosVisitados = LeerRecursosVisitados();

                ActualizarRecursosVisitados(listaRecursosVisitados, entityContext, entityContextBASE, loggingService, servicesUtilVirtuosoAndReplication);
            }
            catch (Exception ex)
            {
                loggingService.GuardarLog(ex.Message);
            }
        }

        private Dictionary<Guid, List<Guid>> LeerRecursosVisitados()
        {
            Dictionary<Guid, List<Guid>> listaDocumentosPorProyecto = new Dictionary<Guid, List<Guid>>();

            foreach (string linea in mDocumentosVisitados)
            {
                //Parto la línea
                char[] separator = { '|' };
                string[] datos = linea.Split(separator, StringSplitOptions.RemoveEmptyEntries);
                Guid docID;
                Guid proyectoSeleccionadoID;

                if (Guid.TryParse(datos[1], out docID) && Guid.TryParse(datos[2], out proyectoSeleccionadoID))
                {
                    if (!proyectoSeleccionadoID.Equals(ProyectoAD.MetaProyecto))
                    {
                        if (!listaDocumentosPorProyecto.ContainsKey(proyectoSeleccionadoID))
                        {
                            listaDocumentosPorProyecto.Add(proyectoSeleccionadoID, new List<Guid>());
                        }
                        listaDocumentosPorProyecto[proyectoSeleccionadoID].Add(docID);
                    }
                }
            }

            return listaDocumentosPorProyecto;
        }

        private void ActualizarRecursosVisitados(Dictionary<Guid, List<Guid>> pListaDocumentosVisitadosPorProyecto, EntityContext entityContext, EntityContextBASE entityContextBASE, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            foreach (Guid proyectoID in pListaDocumentosVisitadosPorProyecto.Keys)
            {
                List<Guid> listaDocumentosVisitados = pListaDocumentosVisitadosPorProyecto[proyectoID];

                // lee de base de datos los recursos actuales
                BaseComunidadCN baseComunidadCN = new BaseComunidadCN(entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                List<Guid> listaDocumentos = baseComunidadCN.ObtenerUltimosRecursosVisitadosDeProyecto(proyectoID);

                bool crearNuevaFila = false;

                if (listaDocumentos == null)
                {
                    //No hay nada en la base de datos, hay que crear la fila
                    crearNuevaFila = true;
                    listaDocumentos = new List<Guid>();
                }
                else
                {
                    //Comprueba si hay ids repetidos y los quita
                    List<Guid> listaRepetidos = listaDocumentos.Intersect(listaDocumentosVisitados).ToList();
                    List<Guid> temporalListDocumentos = new List<Guid>();
                    foreach (Guid docID in listaDocumentos)
                    {
                        if (!listaRepetidos.Contains(docID))
                        {
                            temporalListDocumentos.Add(docID);
                        }
                    }

                    listaDocumentos = temporalListDocumentos;
                }

                //Pone los nuevos al principio
                listaDocumentos.InsertRange(0, listaDocumentosVisitados);

                listaDocumentos = listaDocumentos.Distinct().ToList();
                //Si suman más de 20, quita los que sobran
                if (listaDocumentos.Count > 20)
                {
                    listaDocumentos.RemoveRange(20, listaDocumentos.Count - 20);
                }
                
                //Actualiza la bd
                baseComunidadCN.ActualizarUltimosRecursosVisitadosDeProyecto(proyectoID, listaDocumentos, crearNuevaFila);
            }
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new Controller_ProcesarUltimosRecursosVistos(mDocumentosVisitados, ScopedFactory, mConfigService);
        }
    }
}
