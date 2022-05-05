using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Es.Riam.Gnoss.AD.Live;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Recursos;
using System.IO;
using System.Threading.Tasks;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.ServicioActualizacionOffline.Models;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Util.General;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.ServicioActualizacionOffline
{
    public class ProcesarFichero : ControladorServicioGnoss
    {
        #region Miembros

        private string mTempFile;

        private UtilsServicioUDP mUtilsServicioUDP;

        private string mTipoDatoActualizacion = "";

        private string mFicheroLog;

        private object mObjErrorLock;
        private object mObjLineaLock;
        public ProcesarFichero(IServiceScopeFactory serviceScopeFactory, ConfigService configService)
            : base(serviceScopeFactory, configService)
        {
            mUtilsServicioUDP = new UtilsServicioUDP(configService);
        }

        #endregion

        #region Propiedades

        public string TempFile
        {
            get
            {
                return mTempFile;
            }
            set
            {
                mTempFile = value;
            }
        }

        internal UtilsServicioUDP UtilsServicioUDP
        {
            get { return mUtilsServicioUDP; }
            set { mUtilsServicioUDP = value; }
        }

        public string TipoDatoActualizacion
        {
            get { return mTipoDatoActualizacion; }
            set { mTipoDatoActualizacion = value; }
        }

        public string FicheroLog
        {
            get { return mFicheroLog; }
            set { mFicheroLog = value; }
        }

        public object ObjErrorLock
        {
            get
            {
                return mObjErrorLock;
            }
            set
            {
                mObjErrorLock = value;
            }
        }

        public object ObjLineaLock
        {
            get
            {
                return mObjLineaLock;
            }
            set
            {
                mObjLineaLock = value;
            }
        }

        #endregion

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

            try
            {
                Dictionary<Guid, DatosOfflineModel> dicDatosLinea = ObtenerListaDatosLinea(loggingService);
                if (mTipoDatoActualizacion.Equals("Votos") || mTipoDatoActualizacion.Equals("Comentarios") || mTipoDatoActualizacion.Equals("recursos"))
                {
                    ProcesarRecursosVotosComentarios(dicDatosLinea, entityContext, loggingService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                }
                else
                {
                    ProcesarVisitas(dicDatosLinea, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }

                File.Delete(TempFile);
                Controller_ProcessarLista.mNumTaskOpen--;
                //TaskList.Remove(TempFile);
            }
            catch (Exception ex)
            {
                //Escribir en un fichero para procesarlas por la noche
                UtilsServicioUDP.GuardarLogYEnviarCorreo("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace, mFicheroLog, ObjErrorLock);
            }
        }

        private void ProcesarVisitas(Dictionary<Guid, DatosOfflineModel> pDicDatosLinea, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            // Eliminado el código de actualización de virtuoso porque se hacían demasiadas insercciones en virtuoso y los checkpoints tenían que registrar demasiadas transacciones, por lo que costaban mucho tiempo y se podía perder información.
            foreach (Guid documentoID in pDicDatosLinea.Keys)
            {
                try
                {
                    // Actualizar en BBDD la fecha de la última visita en el recurso.
                    ActualizarFechaUltimaVisitaDocumento(documentoID, pDicDatosLinea[documentoID].BaseRecursosID, pDicDatosLinea[documentoID].Fecha, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                    ProcesarSocketRecibidoLive(pDicDatosLinea[documentoID], entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                    ProcesarSocketRecibidoLiveExtra(pDicDatosLinea[documentoID], entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }
                catch (Exception ex)
                {
                    //Escribir en un fichero para procesarlas por la noche
                    UtilsServicioUDP.GuardarLogYEnviarCorreo("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace, mFicheroLog, ObjErrorLock);
                }
            }
        }

        private void ProcesarRecursosVotosComentarios(Dictionary<Guid, DatosOfflineModel> pDicDatosLinea, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            foreach (Guid documentoID in pDicDatosLinea.Keys)
            {
                try
                {
                    ProcesarSocketRecibidoBase(pDicDatosLinea[documentoID], entityContext, loggingService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                }
                catch (Exception ex)
                {
                    //Escribir en un fichero para procesarlas por la noche
                    UtilsServicioUDP.GuardarLogYEnviarCorreo("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace, mFicheroLog, ObjErrorLock);
                }
            }

            //Comprobar si algún recurso de los pasados está entre los 10 primeros y limpiar la caché de la comunidad.
            //try
            //{
            //    List<DatosOfflineModel> listaDatosOffline = pDicDatosLinea.Values.ToList();
            //    //Comprobar si algún recurso de los pasados está entre los 10 primeros y limpiar la caché de la comunidad.
            //    Dictionary<Guid, string> dicDatosAgrupados = AgruparDatosLimpiezaCache(listaDatosOffline);
            //    ProcesarDatosLimpiezaCache_NoVisitas(dicDatosAgrupados);
            //    dicDatosAgrupados.Clear();
            //}
            //catch (Exception ex)
            //{
            //    //Escribir en un fichero para procesarlas por la noche
            //    UtilsServicioUDP.GuardarLogYEnviarCorreo("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace, mFicheroLog, ObjErrorLock);
            //}
        }

        //private void ProcesarDatosLimpiezaCache_NoVisitas(Dictionary<Guid, string> pDicDatosAgrupados, EntityContext entityContext,LoggingService loggingService, EntityContextBASE entityContextBASE)
        //{
        //    char[] delimiter = { '|' };

        //    List<Guid> proyAgnadido = new List<Guid>();
        //    foreach (Guid proyID in pDicDatosAgrupados.Keys)
        //    {
        //        foreach (string docID in pDicDatosAgrupados[proyID].Split(delimiter, StringSplitOptions.RemoveEmptyEntries))
        //        {
        //            if (UtilsServicioUDP.ComprobarDocumento10Primeros(mUrlIntragnoss, mFicheroConfiguracionBD, proyID, new Guid(docID), entityContext, ) && !proyAgnadido.Contains(proyID))
        //            {
        //                BaseComunidadCN baseComunidadCN = new BaseComunidadCN(entityContext, loggingService, entityContextBASE, mConfigService);
        //                baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Recursos);
        //                baseComunidadCN.Dispose();

        //                //Agnadimos el proyecto a la lista para saber que ya se ha enviado una fila al refresco caché.
        //                proyAgnadido.Add(proyID);

        //                break;
        //            }
        //        }
        //    }

        //    proyAgnadido.Clear();
        //}

        // Agrupación de las filas del fichero

        private Dictionary<Guid, DatosOfflineModel> ObtenerListaDatosLinea(LoggingService loggingService)
        {
            List<string> visitas = UtilsServicioUDP.LeerFichero(TempFile);
            Dictionary<Guid, DatosOfflineModel> dicDatosOffline = new Dictionary<Guid, DatosOfflineModel>();

            char[] separator = { '|' };
            foreach (string visita in visitas)
            {
                try
                {
                    Guid documentoID = Guid.Empty;
                    Guid proyectoID = Guid.Empty;
                    Guid identidadID = Guid.Empty;
                    Guid baseRecursosID = Guid.Empty;
                    Guid creadorDocID = Guid.Empty;
                    DateTime fechaVisita = DateTime.Now;
                    long numeroDeVisitas;

                    string[] datos = visita.Split(separator, StringSplitOptions.RemoveEmptyEntries);

                    if (Guid.TryParse(datos[0], out documentoID) &&
                        Guid.TryParse(datos[1], out proyectoID) &&
                        Guid.TryParse(datos[2], out identidadID) &&
                        Guid.TryParse(datos[3], out baseRecursosID) &&
                        Guid.TryParse(datos[4], out creadorDocID))
                    {
                        if (datos.Length > 5)
                        {
                            DateTime.TryParseExact(datos[5], "yyyyMMddHHmmss", null, System.Globalization.DateTimeStyles.None, out fechaVisita);
                        }

                        if (dicDatosOffline.ContainsKey(documentoID))
                        {
                            if (datos.Length == 7 && long.TryParse(datos[6], out numeroDeVisitas))
                            {
                                dicDatosOffline[documentoID].NumeroDeVisitas = dicDatosOffline[documentoID].NumeroDeVisitas + numeroDeVisitas;
                            }
                            else
                            {
                                dicDatosOffline[documentoID].NumeroDeVisitas++;
                            }
                        }
                        else
                        {
                            DatosOfflineModel nuevoDatoOffline = new DatosOfflineModel();
                            nuevoDatoOffline.DocumentoID = documentoID;
                            nuevoDatoOffline.ProyectoID = proyectoID;
                            nuevoDatoOffline.IdentidadID = identidadID;
                            nuevoDatoOffline.BaseRecursosID = baseRecursosID;
                            nuevoDatoOffline.IdentidadCreadorID = creadorDocID;
                            nuevoDatoOffline.Fecha = fechaVisita;

                            if (datos.Length == 7 && long.TryParse(datos[6], out numeroDeVisitas))
                            {
                                nuevoDatoOffline.NumeroDeVisitas = numeroDeVisitas;
                            }
                            else
                            {
                                nuevoDatoOffline.NumeroDeVisitas = 1;
                            }

                            dicDatosOffline.Add(documentoID, nuevoDatoOffline);
                        }
                    }
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog("Fichero: " + TempFile + ". Linea: " + visita + ". \r\n" + loggingService.DevolverCadenaError(ex, "1.0.0.0"));
                    throw;
                }
            }

            return dicDatosOffline;
        }

        private Dictionary<Guid, string> AgruparDatosLimpiezaCache(List<DatosOfflineModel> pListaDatosOffline)
        {
            Dictionary<Guid, string> dicDatos = new Dictionary<Guid, string>();

            foreach (DatosOfflineModel dato in pListaDatosOffline)
            {
                if (dicDatos.ContainsKey(dato.ProyectoID))
                {
                    dicDatos[dato.ProyectoID] = dicDatos[dato.ProyectoID] + "|" + dato.DocumentoID.ToString();
                }
                else
                {
                    dicDatos.Add(dato.ProyectoID, dato.DocumentoID.ToString());
                }
            }

            return dicDatos;
        }

        // Procesado de las filas del fichero

        private void ActualizarFechaUltimaVisitaDocumento(Guid pDocumentoID, Guid pBaseRecursosID, DateTime pFechaUltimaVisita, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            docCN.ActualizarFechaUltimaVisitaDocumento(pDocumentoID, pBaseRecursosID, pFechaUltimaVisita);
            docCN.Dispose();
        }

        private void ProcesarSocketRecibidoBase(DatosOfflineModel pDatosOffline,EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            try
            {
                //Actualizacion del modelo Base
                UtilsServicioUDP.ActualizacionVirtuoso(pDatosOffline.ProyectoID, pDatosOffline.DocumentoID, pDatosOffline.IdentidadCreadorID, mUrlIntragnoss, pDatosOffline.NumeroDeVisitas, mTipoDatoActualizacion,entityContext, loggingService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            }
            catch (Exception ex)
            {
                //Aumentamos en 1 el número de solicitudes erroneas
                UtilsServicioUDP.GuardarLogYEnviarCorreo("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace, mFicheroLog, ObjErrorLock);
                UtilsServicioUDP.GuardarLineaErronea(pDatosOffline.DocumentoID, mFicheroLog, ObjLineaLock);
            }
            finally
            {
                ControladorConexiones.CerrarConexiones();
            }
        }

        private void ProcesarSocketRecibidoLive(DatosOfflineModel pDatosOffline, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            try
            {
                //Actualización del modelo Live
                //Actualizar el número de consultas al documento.
                UtilsServicioUDP.ActualizarGnossLive(pDatosOffline.ProyectoID, pDatosOffline.DocumentoID, AccionLive.VisitaRecurso, (int)TipoLive.Recurso, false, mFicheroConfiguracionBDBase, PrioridadLive.Media, pDatosOffline.BaseRecursosID.ToString() + "|" + "NumVisitas=" + pDatosOffline.NumeroDeVisitas, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, false);
                //baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyectoSeleccionadoID, TiposEventosRefrescoCache.ModificarCaducidadCache, TipoBusqueda.Recursos, docID.ToString());
                //baseComunidadCN.Dispose();
            }
            catch (Exception ex)
            {
                //Aumentamos en 1 el número de solicitudes erroneas
                UtilsServicioUDP.GuardarLogYEnviarCorreo("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace, mFicheroLog, ObjErrorLock);
                UtilsServicioUDP.GuardarLineaErronea(pDatosOffline.DocumentoID, mFicheroLog, ObjLineaLock);
            }
            finally
            {
                ControladorConexiones.CerrarConexiones();
            }
        }

        private void ProcesarSocketRecibidoLiveExtra(DatosOfflineModel pDatosOffline, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            try
            {
                //Actualiza las visitas por usuarioID
                UtilsServicioUDP.ActualizarGnossLivePopularidad(pDatosOffline.ProyectoID, pDatosOffline.DocumentoID, pDatosOffline.IdentidadID, AccionLive.VisitaRecurso, (int)TipoLive.Recurso, (int)TipoLive.Miembro, true, PrioridadLive.Media, mFicheroConfiguracionBDBase, "NumVisitas=" + pDatosOffline.NumeroDeVisitas, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
            }
            catch (Exception ex)
            {
                //Aumentamos en 1 el número de solicitudes erroneas
                UtilsServicioUDP.GuardarLogYEnviarCorreo("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace, mFicheroLog, ObjErrorLock);
                UtilsServicioUDP.GuardarLineaErronea(pDatosOffline.DocumentoID, mFicheroLog, ObjLineaLock);
            }
            finally
            {
                ControladorConexiones.CerrarConexiones();
            }
        }

        #endregion

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new ProcesarFichero(ScopedFactory, mConfigService);
        }
    }
}
