using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Es.Riam.Gnoss.AD.BASE_BD.Model;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.Logica.Live;
using Es.Riam.Gnoss.AD.Live;
using Es.Riam.Gnoss.AD.Live.Model;
using System.Globalization;
using System.IO;
using Es.Riam.Gnoss.Logica.Identidad;
using Es.Riam.Gnoss.Logica.Facetado;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Recursos;
using System.Threading;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.CL.Facetado;
using System.Data;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.RabbitMQ;
using Newtonsoft.Json;
using Es.Riam.Util;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.Util.Configuracion;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.ServicioActualizacionOffline
{
    class UtilsServicioUDP //: ControladorServicioGnoss
    {
        private ConfigService mConfigService;
        public UtilsServicioUDP(ConfigService configService)
        {
            mConfigService = configService;
        }

        #region Constantes

        private const string EXCHANGE = "";
        private const string COLA_VISITAS = "ColaVisitas";
        private const string COLA_RABBIT = "cola";

        #endregion

        #region Variables miembro

        private static bool? mHayConexionRabbit = null;

        #endregion

        /// <summary>
        /// Devuelve el proyecto de la identidadID
        /// </summary>
        /// <param name="pIdentidadID">IdentidadID</param>
        /// <returns>Proyecto al que pertenece la identidadID</returns>
        public Guid ObtenerProyectoSeleccionadoID(Guid pIdentidadID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid proyID = identCN.ObtenerProyectoDeIdentidad(pIdentidadID);
            identCN.Dispose();
            return proyID;
        }

        /// <summary>
        /// Devuelve la Base de Recursos del proyectoID
        /// </summary>
        /// <param name="pProyectoSeleccionadoID">Proyecto ID</param>
        /// <returns>Base de recursos del proyectoID</returns>
        public Guid ObtenerBaseRecursosID(Guid pProyectoSeleccionadoID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid baseRecursosID = proyCN.ObtenerBaseRecursosProyectoPorProyectoID(pProyectoSeleccionadoID);
            proyCN.Dispose();
            return baseRecursosID;
        }

        /// <summary>
        /// Devuelve el creador del documentoID pasado como parámetro
        /// </summary>
        /// <param name="pDocID">DocumentoID</param>
        /// <returns>ID del creador</returns>
        public Guid ObtenerCreadorRecursoID(Guid pDocID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid creadorID = docCN.ObtenerCreadorDocumentoID(pDocID);
            docCN.Dispose();
            return creadorID;
        }

        /// <summary>
        /// Lectura del fichero de visitas.txt
        /// </summary>
        /// <returns>Lista con las visitas a procesar.</returns>
        public List<string> LeerFichero(string pFile)
        {
            List<string> listaVisitas = new List<string>();

            StreamReader sr = new StreamReader(pFile);
            string linea = "";
            while ((linea = sr.ReadLine()) != null)
            {
                listaVisitas.Add(linea);
            }
            sr.Dispose();

            return listaVisitas;
        }

        #region Métodos de Log

        /// <summary>
        /// Escribe fisicamente las entradas en el log
        /// </summary>
        /// <param name="infoEntry"></param>
        public void GuardarLogYEnviarCorreo(string pInfoEntry, string pFicheroLog, object pObjectLock)
        {
            StreamWriter logWriter = null;
            FileStream logFile = null;
            try
            {
                //Bloqueamos el objeto que nos pasan para que ningún otro hilo pueda escribir en el fichero y tenga que esperar.
                lock (pObjectLock)
                {
                    if (pInfoEntry != String.Empty)
                    {
                        string nombreFichero = pFicheroLog + "_" + DateTime.Now.ToString("yyyy-MM-dd") + ".log";
                        // File access and writing
                        if (File.Exists(nombreFichero))
                        {
                            logFile = new FileStream(nombreFichero, FileMode.Append, FileAccess.Write);
                        }
                        else
                        {
                            logFile = new FileStream(nombreFichero, FileMode.Create, FileAccess.Write);
                        }
                        logWriter = new StreamWriter(logFile, Encoding.UTF8);

                        // Log entry
                        CultureInfo culture = new CultureInfo(CultureInfo.CurrentCulture.ToString());
                        String logEntry = DateTime.Now.ToString(@"yyyy-MM-dd HH:mm:ss", culture) + " " + pInfoEntry;
                        logWriter.WriteLine(logEntry);
                    }
                }
            }
            catch (Exception ex)
            {
                //Se ha producido un error al guardar la excepción. Lo guardamos en otro fichero:
            }
            finally
            {
                if (logWriter != null)
                {
                    logWriter.Dispose();
                    logWriter.Close();
                }
                if (logFile != null)
                {
                    logFile.Dispose();
                    logFile.Close();
                }
            }
        }

        internal void GuardarLineaErronea(Guid datos, string pFicheroLog, object pObjectLock)
        {
            StreamWriter pSw = null;
            try
            {
                //Bloqueamos el objeto que nos pasan para que ningún otro hilo pueda escribir en el fichero y tenga que esperar.
                lock (pObjectLock)
                {
                    string fileName = pFicheroLog + "_" + nombreFicheroTxtConErrores(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day);
                    pSw = new StreamWriter(fileName, true, Encoding.Default);

                    if (datos != null)
                    {
                        pSw.Write(DateTime.Now + "\t");
                        pSw.WriteLine(datos);
                    }
                }
            }
            catch (Exception ex)
            {
                //Se ha producido un error al copiar una fila erronea al fichero
            }
            finally
            {
                if (pSw != null)
                {
                    pSw.Dispose();
                    pSw.Close();
                }
            }
        }

        /// <summary>
        /// Devuelve el nombre del fichero del día de hoy que tenga errores.
        /// </summary>
        /// <param name="pAgno">Agno</param>
        /// <param name="pMes">Mes</param>
        /// <param name="pDia">Día</param>
        /// <returns>Nombre del fichero con los errores</returns>
        public string nombreFicheroTxtConErrores(int pAgno, int pMes, int pDia)
        {
            return pAgno.ToString() + pMes.ToString() + pDia.ToString() + "_Errores.txt";
        }

        #endregion

        #region Actualizaciones virtuoso

        /// <summary>
        /// Actualización de las visitas en virtuoso.
        /// </summary>
        /// <param name="pProyectoSeleccionadoID">Proyecto en el que se encuentra el recurso</param>
        /// <param name="pDocID">ID del recurso que se ha visitado</param>
        /// <param name="pCreadorDocID">ID del creador del recurso</param>
        public void ActualizacionVirtuoso(Guid pProyectoSeleccionadoID, Guid pDocID, Guid pCreadorDocID, string pUrlIntragnoss, long pNumVisitas, string pTipoDato, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            #region Parte1: DocumentoID

            FacetadoCN facetadoCN = new FacetadoCN(pUrlIntragnoss, pProyectoSeleccionadoID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            //No se aumenta en 1 el número de recursos que tiene un recurso, no tiene sentido.
            if (!pTipoDato.Equals("recursos"))
            {
                try
                {
                    //Actualizar el número de consultas al documento.
                    facetadoCN.ModificarVotosVisitasComentarios(pProyectoSeleccionadoID.ToString(), pDocID.ToString(), pTipoDato, pNumVisitas);

                }
                catch (Exception ex)
                {
                    //Cerramos las conexiones
                    ControladorConexiones.CerrarConexiones();

                    //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                    while (!virtuosoAD.ServidorOperativo())
                    {
                        //Dormimos 30 segundos
                        Thread.Sleep(30 * 1000);
                    }

                    facetadoCN = new FacetadoCN(pUrlIntragnoss, pProyectoSeleccionadoID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoCN.ModificarVotosVisitasComentarios(pProyectoSeleccionadoID.ToString(), pDocID.ToString(), pTipoDato, pNumVisitas);
                }
                finally
                {
                    facetadoCN.Dispose();
                    facetadoCN = null;
                }
            }
            #endregion

            #region Parte2: CreadorID

            List<Guid> listaIdentidad = new List<Guid>();
            listaIdentidad.Add(pCreadorDocID);

            //Si no se encuentra la identidad que ha visitado el recurso, falla.
            IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            List<Guid> perfilesList = identCN.ObtenerPerfilesDeIdentidades(listaIdentidad);

            //Si no encuentra el perfil de la persona que ha creado el recurso a partir de su identidad puede ser porque sea de pruebas.
            if (perfilesList.Count > 0)
            {
                string perfilcreador = perfilesList[0].ToString();
                identCN.Dispose();
                facetadoCN = new FacetadoCN(pUrlIntragnoss, pProyectoSeleccionadoID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);

                try
                {
                    //Actualizar el número de consultas de los documentos del creador.
                    facetadoCN.ModificarVotosVisitasComentarios(perfilcreador, pDocID.ToString(), pTipoDato, pNumVisitas);
                }
                catch (Exception ex)
                {
                    //Cerramos las conexiones
                    ControladorConexiones.CerrarConexiones();

                    //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                    while (!virtuosoAD.ServidorOperativo())
                    {
                        //Dormimos 30 segundos
                        Thread.Sleep(30 * 1000);
                    }

                    facetadoCN = new FacetadoCN(pUrlIntragnoss, pProyectoSeleccionadoID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoCN.ModificarVotosVisitasComentarios(perfilcreador, pDocID.ToString(), pTipoDato, pNumVisitas);
                }
                finally
                {
                    facetadoCN.Dispose();
                    facetadoCN = null;
                }
            }

            #endregion
        }

        #endregion

        #region Actualizaciones de la BD Live

        /// <summary>
        /// Añade a la cola de GnossLive un elemento para su procesamiento.
        /// </summary>
        /// <param name="pProyectoID">Proyecto al que pertenece el elemento</param>
        /// <param name="pElementoID">Identificador del elemento que se está tratando</param>
        /// <param name="pAccion">Acción que se está realizando</param>
        /// <param name="pTipoElemento">Tipo de elemento que se está tratando</param>
        /// <param name="pSoloPersonal"></param>
        /// <param name="pRutaLive"></param>
        /// <param name="pPrioridad">Prioridad</param>
        /// <param name="pInfoExtra">Infomación extra</param>
        public void ActualizarGnossLive(Guid pProyectoID, Guid pElementoID, AccionLive pAccion, int pTipoElemento, bool pSoloPersonal, string pRutaLiveBase, PrioridadLive pPrioridad, string pInfoExtra, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            LiveCN liveCN = new LiveCN(pRutaLiveBase, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            LiveDS liveDS = new LiveDS();

            try
            {
                InsertarFilaEnColaVisitasRabbitMQ(pProyectoID, pElementoID, pAccion, pTipoElemento, 0, DateTime.Now, pSoloPersonal, (short)pPrioridad,loggingService, pInfoExtra);
            }
            catch(Exception ex)
            {
                loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos 'BASE', tabla 'cola' o tabla 'ColaVisitas'");
                liveDS.Cola.AddColaRow(pProyectoID, pElementoID, (int)pAccion, pTipoElemento, 0, DateTime.Now, pSoloPersonal, (short)pPrioridad, pInfoExtra);
            }
            
            //liveDS.ColaHomePerfil.AddColaHomePerfilRow(pProyectoID, pElementoID, (int)pAccion, pTipoElemento, 0, DateTime.Now, (short)pPrioridad);

            liveCN.ActualizarBD(liveDS);

            liveDS.Dispose();
            liveCN.Dispose();
        }

        public bool HayConexionRabbit
        {
            get
            {
                if (!mHayConexionRabbit.HasValue)
                {
                    mHayConexionRabbit = mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN);
                }
                return mHayConexionRabbit.Value;
            }
        }

        public void InsertarFilaEnColaVisitasRabbitMQ(Guid pProyectoID, Guid pID, AccionLive pAccion, int pTipo, int pNumIntentos, DateTime pFecha, bool pSoloPersonal, short pPrioridad, LoggingService loggingService, string pInfoExtra = null)
        {
            LiveDS.ColaRow filaCola = new LiveDS().Cola.NewColaRow();
            filaCola.ProyectoId = pProyectoID;
            filaCola.Id = pID;
            filaCola.Accion = (int)pAccion;
            filaCola.Tipo = pTipo;
            filaCola.NumIntentos = pNumIntentos;
            filaCola.Fecha = pFecha;
            filaCola.SoloPersonal = pSoloPersonal;
            filaCola.Prioridad = pPrioridad;
            filaCola.InfoExtra = pInfoExtra;

            //AcctionLive.VisitaRecurso

            if (AccionLive.VisitaRecurso.Equals(pAccion) && HayConexionRabbit)
            {
                RabbitMQClient rabbitMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_VISITAS,loggingService, mConfigService, EXCHANGE, COLA_VISITAS);
                rabbitMQ.AgregarElementoACola(JsonConvert.SerializeObject(filaCola.ItemArray));
            }
            else if (HayConexionRabbit)
            {
                RabbitMQClient rabbitMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_RABBIT, loggingService, mConfigService, EXCHANGE, COLA_RABBIT);
                rabbitMQ.AgregarElementoACola(JsonConvert.SerializeObject(filaCola.ItemArray));
            }
        }
        /// <summary>
        /// Añade a la cola ColaPopularidad de GnossLive un elemento para su procesamiento.
        /// </summary>
        /// <param name="pProyectoID">Proyecto al que pertenece el elemento</param>
        /// <param name="pElementoID">ElementoID que se va a aumentar su popularidad</param>
        /// <param name="pIdentidadID">IdentidadID del usuario que ha aumentado la popularidad con alguna acción</param>
        /// <param name="pAccion">Acción que se está realizando</param>
        /// <param name="pTipoElemento">Tipo de elemento que se está tratando</param>
        /// <param name="pTipoElemento">Tipo del segundo elemento que se está tratando</param>
        /// <param name=param name="pPrioridad">Prioridad</param>
        public void ActualizarGnossLivePopularidad(Guid pProyectoID, Guid pElementoID, Guid pIdentidadID, AccionLive pAccion, int pTipoElemento, int pTipoElemento2, bool pSoloPersonal, PrioridadLive pPrioridad, string pFicheroConfiguracionBase, string pInfoExtra, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            LiveCN liveCN = null;

            if (!string.IsNullOrEmpty(pFicheroConfiguracionBase))
            {
                liveCN = new LiveCN(pFicheroConfiguracionBase, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            }
            else
            {
                liveCN = new LiveCN("live", entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            }


            LiveDS liveDS = new LiveDS();

            liveDS.ColaPopularidad.AddColaPopularidadRow(pProyectoID, pElementoID, pIdentidadID, (int)pAccion, pTipoElemento, pTipoElemento2, 0, DateTime.Now, pSoloPersonal, (short)pPrioridad, pInfoExtra);


            liveCN.ActualizarBD(liveDS);

            liveDS.Dispose();
            liveCN.Dispose();
        }

        #endregion

        /// <summary>
        /// Comprueba si el documento pasado como parámetro esta entre los 10 últimos publicados.
        /// </summary>
        /// <param name="pProyID"></param>
        /// <param name="pDocID"></param>
        /// <returns>TRUE si el rec está entre los 10 primeros, False caso contrario</returns>
        internal bool ComprobarDocumento10Primeros(string pUrlIntragnoss, string pFicheroConfiguracionBD, Guid pProyID, Guid pDocID, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //Doc Privado = consultar si esta entre los 10 últimos publicados
            //DocumentacionCN publico, consultar la caché.

            bool contiene = false;

            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            bool privado = docCN.EsDocumentoEnProyectoPrivadoEditores(pDocID, pProyID);

            if (privado)
            {
                //Conusltamos el modelo ácido
                DataWrapperDocumentacion docDW = docCN.ObtenerUltimosRecursosPublicados(pProyID, 10);
                foreach (AD.EntityModel.Models.Documentacion.Documento dr in docDW.ListaDocumento)
                {
                    Guid docID = dr.DocumentoID;

                    if (docID == pDocID)
                    {
                        contiene = true;
                        break;
                    }
                }
            }
            else
            {
                //Consultamos la caché.
                string resultado = "";

                //Obtengo los resultados de la caché
                FacetadoCL facetadoCL = new FacetadoCL(pFicheroConfiguracionBD, pFicheroConfiguracionBD, pUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoCL.Dominio = pUrlIntragnoss.Substring(7, pUrlIntragnoss.Length - 8);
                
                Guid invitadoID = new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff");

                //URLIntragnoss + "_facetado_facetado_" + ProyID + "_recurso_invitado_es_1"
                
                //Comprobamos si la identidad que ha realizado la acción tiene una caché especial
                resultado = facetadoCL.ObtenerResultadosYFacetasDeBusquedaEnProyecto(pProyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Recursos), invitadoID, "1", "es", null, true, "", true);

                if (!string.IsNullOrEmpty(resultado) && resultado.Contains(pDocID.ToString()))
                {
                    contiene = true;
                }

                facetadoCL.Dispose();
            }

            docCN.Dispose();

            //select top 10 DocumentoID
            return contiene;
        }

        #region Miembros de ICloneable

        protected UtilsServicioUDP ClonarControlador()
        {
            return new UtilsServicioUDP(mConfigService);
        }

        #endregion
    }
}
