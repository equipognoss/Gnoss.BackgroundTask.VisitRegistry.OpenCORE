using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.AD.ParametroAplicacion;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.ServicioActualizacionOffline
{
    public class ActualizarNumeroVisitasVirtuoso : ControladorServicioGnoss
    {
        #region Miembros

        private int mNumHorasIntervalo;

        #endregion

        public ActualizarNumeroVisitasVirtuoso(int pNumHorasIntervalo, IServiceScopeFactory serviceScopeFactory, ConfigService configService)
            : base(serviceScopeFactory, configService)
        {
            mNumHorasIntervalo = pNumHorasIntervalo;
        }

        #region Metodos

        /// <summary>
        /// Procesa las visitas escritas en el fichero. Las agrupa y las procesa.
        /// </summary>
        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ParametroAplicacionCN paramCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            parametroAplicacionGBD.ObtenerConfiguracionGnoss(gestorParametroAplicacion);
            mUrlIntragnoss = gestorParametroAplicacion.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;

            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            DataWrapperDocumentacion docDW = docCN.ObtenerUltimosRecursosVisitados(mNumHorasIntervalo);
            FacetadoAD facAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);

            foreach(AD.EntityModel.Models.Documentacion.DocumentoWebVinBaseRecursosExtra dwvbr in docDW.ListaDocumentoWebVinBaseRecursosExtra)
            {
                List<AD.EntityModel.Models.Documentacion.BaseRecursosProyecto> filas = docDW.ListaBaseRecursosProyecto.Where(baseRec => baseRec.BaseRecursosID.Equals(dwvbr.BaseRecursosID)).ToList();

                if (filas.Count > 0)
                {
                    // No hace falta consultar si exite el triple porque el Base lo inserta al crear el documento.

                    string grafo = mUrlIntragnoss + filas[0].ProyectoID.ToString().ToLower();
                    facAD.ActualizarVirtuoso("SPARQL MODIFY GRAPH <" + grafo + "> DELETE {?s ?p ?o. } INSERT { ?s ?p " + dwvbr.NumeroConsultas + ".}  WHERE {?s ?p ?o. FILTER(?s = <http://gnoss/" + dwvbr.DocumentoID.ToString().ToUpper() + "> AND ?p = <http://gnoss/hasnumeroVisitas>)}", grafo);
                }
            }

            facAD.Dispose();
            docCN.Dispose();
        }

        #endregion

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new ActualizarNumeroVisitasVirtuoso(mNumHorasIntervalo, ScopedFactory, mConfigService);
        }
    }
}
