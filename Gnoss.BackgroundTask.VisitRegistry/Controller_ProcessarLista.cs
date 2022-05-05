using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Recursos;
using System.IO;
using Es.Riam.Gnoss.AD.Live;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.Util.General;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.ServicioActualizacionOffline
{
    internal class Controller_ProcessarLista : ControladorServicioGnoss
    {
        #region Miembros

        private volatile List<string> mSocketsList;

        private Dictionary<string, Task> mTaskList;

        //Al no ser un objeto no hay que pasarselo a traves del constructor.
        //Declararlo estatico y acceder desde la clase que sea necesario.
        public volatile static int mNumTaskOpen = 0;

        private int mNumSolicitudesVisitas = 0;
        private string mDirectorioVisitas = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + Path.DirectorySeparatorChar + "Visitas" + Path.DirectorySeparatorChar;
        private DateTime mUltimaVisita;

        private string mDirectorioVotos = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + Path.DirectorySeparatorChar + "Votos" + Path.DirectorySeparatorChar;
        private int mNumSolicitudesVotos = 0;
        private DateTime mUltimoVoto;

        private string mDirectorioComentarios = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + Path.DirectorySeparatorChar + "Comentarios" + Path.DirectorySeparatorChar;
        private int mNumSolicitudesComentarios = 0;
        private DateTime mUltimoComentario;

        private string mDirectorioRecursos = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + Path.DirectorySeparatorChar + "recursos" + Path.DirectorySeparatorChar;
        private int mNumSolicitudesRecursos = 0;
        private DateTime mUltimoRecurso;

        // Control de las actualizaciones del número de recursos en virtuoso.
        private DateTime mFechaUltimaActualizacionNumeroVisitasEnVirtuoso;
        private int mHorasIntervaloActualizarVisitasVirtuoso;

        private string mFicheroVisitas = "";
        private string mFicheroVotos = "";
        private string mFicheroComentarios = "";
        private string mFicheroRecursos = "";

        private int mNumLineasHilo = 100;
        private int mNumHilosAbiertos = 5;
        private int mMinutosAntesProcesar = 5;

        private int mPuerto = 0;

        private readonly object objErrorLock = new object();
        private readonly object objLineaLock = new object();

        private DateTime mFechaUltimaActualizacionUltimasVisitas = DateTime.Now;
        private List<string> mUltimasVisitas = new List<string>();

        #endregion

        #region Constructores

        public Controller_ProcessarLista(int pNumLineasHilos, int pNumHilosAbiertos, int pMinutosAntesProcesar, List<string> mSocketsList, int pPuerto, int pHorasProcesarVisitasVirtuoso, IServiceScopeFactory serviceScopeFactory, ConfigService configService)
            : base(serviceScopeFactory, configService)
        {
            this.mSocketsList = mSocketsList;
            mTaskList = new Dictionary<string, Task>();


            mNumLineasHilo = pNumLineasHilos;
            mNumHilosAbiertos = pNumHilosAbiertos;
            mMinutosAntesProcesar = pMinutosAntesProcesar;

            mPuerto = pPuerto;

            mFicheroVisitas = "visitas_" + pPuerto + ".txt";
            mNumSolicitudesVisitas = 0;
            mUltimaVisita = DateTime.Now;

            mFicheroVotos = "votos_" + pPuerto + ".txt";
            mNumSolicitudesVotos = 0;
            mUltimoVoto = DateTime.Now;

            mFicheroComentarios = "comentarios_" + pPuerto + ".txt";
            mNumSolicitudesComentarios = 0;
            mUltimoComentario = DateTime.Now;

            mFicheroRecursos = "recursos_" + pPuerto + ".txt";
            mNumSolicitudesRecursos = 0;
            mUltimoRecurso = DateTime.Now;

            mHorasIntervaloActualizarVisitasVirtuoso = pHorasProcesarVisitasVirtuoso;
            mFechaUltimaActualizacionNumeroVisitasEnVirtuoso = DateTime.Now;
        }

        #endregion

        #region Metodos generales

        /// <summary>
        /// Procesa las solicitudes TCP que hay almacenadas en la lista
        /// </summary>
        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ParametroAplicacionCN paramCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            mUrlIntragnoss = GestorParametroAplicacionDS.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;

            //Antes de hacer el mantenimiento, revisar si quedan ficheros pendientes y procesarlos.
            CreaccionDeDirectorios();
            ProcesarFicherosPendientesEnDirectorio();
            InicializarContadores();

            while (true)
            {
                ComprobarCancelacionHilo();
                EstaHiloActivo = true;

                try
                {
                    //Comprueba si hay mNumLineas y crea y almacena los hilos en la lista
                    CreaccionDeHilos();

                    //Comprueba si se ha terminado algún hilo y arranca el siguiente
                    ArrancadoDeHilosNuevos(loggingService);

                    //Comprueba si hay elementos pendientes en la lista compartida con el otro hilo y los escribe en un fichero
                    AgregarFilasNuevas_Fichero(entityContext, entityContextBASE, utilidadesVirtuoso, loggingService, redisCacheWrapper, gnossCache, virtuosoAD, servicesUtilVirtuosoAndReplication);
                }
                catch (OperationCanceledException)
                {
                    ControladorConexiones.CerrarConexiones();
                    break;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog("UDPListener ERROR: Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                finally
                {
                    Thread.Sleep(5 * 1000);
                }
            }
        }

        /// <summary>
        /// Creacción de los directorios encargados del procesamiento de los datos.
        /// </summary>
        private void CreaccionDeDirectorios()
        {
            if (!Directory.Exists(mDirectorioVisitas))
            {
                Directory.CreateDirectory(mDirectorioVisitas);
            }
            if (!Directory.Exists(mDirectorioVotos))
            {
                Directory.CreateDirectory(mDirectorioVotos);
            }
            if (!Directory.Exists(mDirectorioComentarios))
            {
                Directory.CreateDirectory(mDirectorioComentarios);
            }
            if (!Directory.Exists(mDirectorioRecursos))
            {
                Directory.CreateDirectory(mDirectorioRecursos);
            }
        }

        private void AgregarFilasNuevas_Fichero(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ProcesarUltimasVisitas(entityContext, entityContextBASE, utilidadesVirtuoso, loggingService, redisCacheWrapper, gnossCache, virtuosoAD, servicesUtilVirtuosoAndReplication);

            if (this.mSocketsList.Count > 0)
            {
                while (this.mSocketsList.Count > 0 && (mNumSolicitudesVisitas < mNumLineasHilo || mNumSolicitudesVotos < mNumLineasHilo || mNumSolicitudesComentarios < mNumLineasHilo || mNumSolicitudesRecursos < mNumLineasHilo))
                {
                    //Recogemos la primera línea y la borramos de la lista.
                    string primeraLinea = mSocketsList.ElementAt(0);
                    mSocketsList.Remove(mSocketsList.ElementAt(0));

                    string fichEscribir = "";
                    if (primeraLinea.StartsWith("Votos"))
                    {
                        fichEscribir = mDirectorioVotos + mFicheroVotos;
                        mNumSolicitudesVotos++;
                    }
                    else if (primeraLinea.StartsWith("Comentarios"))
                    {
                        fichEscribir = mDirectorioComentarios + mFicheroComentarios;
                        mNumSolicitudesComentarios++;
                    }
                    else if (primeraLinea.StartsWith("recursos"))
                    {
                        fichEscribir = mDirectorioRecursos + mFicheroRecursos;
                        mNumSolicitudesRecursos++;
                    }
                    else
                    {
                        fichEscribir = mDirectorioVisitas + mFicheroVisitas;
                        mNumSolicitudesVisitas++;

                        // Almaceno la línea en otra variable para procesarla cada 15 segundos
                        mUltimasVisitas.Add(primeraLinea);
                    }

                    string primeraLineaEscribirFichero = primeraLinea.Substring(primeraLinea.IndexOf("|"));
                    primeraLineaEscribirFichero += "|" + DateTime.Now.ToString("yyyyMMddHHmmss");

                    //Escribimos en un fichero las líneas
                    EscribirLinea(fichEscribir, primeraLineaEscribirFichero);

                    EstaHiloActivo = true;
                }
            }
        }

        private void ProcesarUltimasVisitas(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            if (DateTime.Now.Subtract(mFechaUltimaActualizacionUltimasVisitas).TotalSeconds > 15 && mUltimasVisitas.Count > 0)
            {
                List<string> listUltimasVisitas = new List<string>(mUltimasVisitas);
                // Si hace más de 15 segundos que se envían visitas, envío las visitas a Sql Server
                Task.Factory.StartNew(new Action(()=> { new Controller_ProcesarUltimosRecursosVistos(listUltimasVisitas, ScopedFactory, mConfigService).RealizarMantenimiento(entityContext, entityContextBASE, utilidadesVirtuoso, loggingService, redisCacheWrapper, gnossCache, virtuosoAD, servicesUtilVirtuosoAndReplication); }));

                mUltimasVisitas.Clear();
                mFechaUltimaActualizacionUltimasVisitas = DateTime.Now;
            }
        }

        /// <summary>
        /// Iniciamos los contadores de visitas a partir de los ficheros de votos, visitas y comentarios.
        /// </summary>
        private void InicializarContadores()
        {
            if (File.Exists(mDirectorioVisitas + mFicheroVisitas))
            {
                UtilsServicioUDP utilsServicioUDP = new UtilsServicioUDP(mConfigService);

                //Obtenemos el número de líneas que tiene el fichero por si se ha vuelto a arrancar para que procese las que tenga.
                if (utilsServicioUDP.LeerFichero(mDirectorioVisitas + mFicheroVisitas).Count != mNumSolicitudesVisitas)
                {
                    mNumSolicitudesVisitas = utilsServicioUDP.LeerFichero(mDirectorioVisitas + mFicheroVisitas).Count;
                }
            }

            if (File.Exists(mDirectorioVotos + mFicheroVotos))
            {
                UtilsServicioUDP utilsServicioUDP = new UtilsServicioUDP(mConfigService);

                //Obtenemos el número de líneas que tiene el fichero por si se ha vuelto a arrancar para que procese las que tenga.
                if (utilsServicioUDP.LeerFichero(mDirectorioVotos + mFicheroVotos).Count != mNumSolicitudesVotos)
                {
                    mNumSolicitudesVotos = utilsServicioUDP.LeerFichero(mDirectorioVotos + mFicheroVotos).Count;
                }
            }

            if (File.Exists(mDirectorioComentarios + mFicheroComentarios))
            {
                UtilsServicioUDP utilsServicioUDP = new UtilsServicioUDP(mConfigService);

                //Obtenemos el número de líneas que tiene el fichero por si se ha vuelto a arrancar para que procese las que tenga.
                if (utilsServicioUDP.LeerFichero(mDirectorioComentarios + mFicheroComentarios).Count != mNumSolicitudesComentarios)
                {
                    mNumSolicitudesComentarios = utilsServicioUDP.LeerFichero(mDirectorioComentarios + mFicheroComentarios).Count;
                }
            }

            if (File.Exists(mDirectorioRecursos + mFicheroRecursos))
            {
                UtilsServicioUDP utilsServicioUDP = new UtilsServicioUDP(mConfigService);

                //Obtenemos el número de líneas que tiene el fichero por si se ha vuelto a arrancar para que procese las que tenga.
                if (utilsServicioUDP.LeerFichero(mDirectorioRecursos + mFicheroRecursos).Count != mNumSolicitudesRecursos)
                {
                    mNumSolicitudesRecursos = utilsServicioUDP.LeerFichero(mDirectorioRecursos + mFicheroRecursos).Count;
                }
            }
        }

        /// <summary>
        /// Comprobamos el estado de los contadores y la fecha de la última visita y creamos hilos para que procesen los votos, visitas, comentarios y recursos pendientes.
        /// </summary>
        private void CreaccionDeHilos()
        {
            if (mNumSolicitudesVisitas >= mNumLineasHilo || (mUltimaVisita.AddMinutes(mMinutosAntesProcesar) <= DateTime.Now && File.Exists(mDirectorioVisitas + mFicheroVisitas)))
            {
                //Cada 100 visitas guardadas en un fichero, cambiar el nombre del fichero y abrir un hilo.
                //No crear un fichero temporal...
                string tempName = Path.GetRandomFileName() + "_" + mPuerto;

                File.Move(mDirectorioVisitas + mFicheroVisitas, mDirectorioVisitas + tempName);

                //Crear un Hilo y procesar el fichero temporal independientemente.
                AbrirHiloYProcesarFichero(mDirectorioVisitas + tempName, "Visitas");
                mNumSolicitudesVisitas = 0;
                mUltimaVisita = DateTime.Now;
            }

            if (mNumSolicitudesVotos >= mNumLineasHilo || (mUltimoVoto.AddMinutes(mMinutosAntesProcesar) <= DateTime.Now && File.Exists(mDirectorioVotos + mFicheroVotos)))
            {
                //Cada 100 visitas guardadas en un fichero, cambiar el nombre del fichero y abrir un hilo.
                //No crear un fichero temporal...
                string tempName = Path.GetRandomFileName() + "_" + mPuerto;

                File.Move(mDirectorioVotos + mFicheroVotos, mDirectorioVotos + tempName);

                //Crear un Hilo y procesar el fichero temporal independientemente.
                AbrirHiloYProcesarFichero(mDirectorioVotos + tempName, "Votos");
                mNumSolicitudesVotos = 0;
                mUltimoVoto = DateTime.Now;
            }

            if (mNumSolicitudesComentarios >= mNumLineasHilo || (mUltimoComentario.AddMinutes(mMinutosAntesProcesar) <= DateTime.Now && File.Exists(mDirectorioComentarios + mFicheroComentarios)))
            {
                //Cada 100 visitas guardadas en un fichero, cambiar el nombre del fichero y abrir un hilo.
                //No crear un fichero temporal...
                string tempName = Path.GetRandomFileName() + "_" + mPuerto;

                File.Move(mDirectorioComentarios + mFicheroComentarios, mDirectorioComentarios + tempName);

                //Crear un Hilo y procesar el fichero temporal independientemente.
                AbrirHiloYProcesarFichero(mDirectorioComentarios + tempName, "Comentarios");
                mNumSolicitudesComentarios = 0;
                mUltimoComentario = DateTime.Now;
            }

            if (mNumSolicitudesRecursos >= mNumLineasHilo || (mUltimoRecurso.AddMinutes(mMinutosAntesProcesar) <= DateTime.Now && File.Exists(mDirectorioRecursos + mFicheroRecursos)))
            {
                //Cada 100 visitas guardadas en un fichero, cambiar el nombre del fichero y abrir un hilo.
                //No crear un fichero temporal...
                string tempName = Path.GetRandomFileName() + "_" + mPuerto;

                File.Move(mDirectorioRecursos + mFicheroRecursos, mDirectorioRecursos + tempName);

                //Crear un Hilo y procesar el fichero temporal independientemente.
                AbrirHiloYProcesarFichero(mDirectorioRecursos + tempName, "recursos");
                mNumSolicitudesRecursos = 0;
                mUltimoRecurso = DateTime.Now;
            }

            // Actualizar las visitas en virtuoso
            if (mHorasIntervaloActualizarVisitasVirtuoso > 0 && mFechaUltimaActualizacionNumeroVisitasEnVirtuoso.AddHours(mHorasIntervaloActualizarVisitasVirtuoso) <= DateTime.Now)
            {
                //Crear un Hilo y procesar el fichero temporal independientemente.
                AbrirHiloYActualizarVisitas();

                mFechaUltimaActualizacionNumeroVisitasEnVirtuoso = DateTime.Now;
            }
        }

        /// <summary>
        /// Se comprueban las listas de hilos y se arrancan a medida que se van finalizando otros.
        /// </summary>
        private void ArrancadoDeHilosNuevos(LoggingService loggingService)
        {
            if (mNumTaskOpen < mNumHilosAbiertos && mTaskList.Count > 0)
            {
                while (mTaskList.Count > 0)
                {
                    if (mNumTaskOpen < mNumHilosAbiertos)
                    {
                        if (mTaskList[mTaskList.Keys.ElementAt(0)].Status != TaskStatus.Running)
                        {
                            //Arrancamos y eliminamos
                            mTaskList[mTaskList.Keys.ElementAt(0)].Start();

                            //En caso de que haya fallado, recogemos el error.
                            mTaskList[mTaskList.Keys.ElementAt(0)].ContinueWith(DelegadoErrorEnTarea => RelanzarTareaFallida(mTaskList[mTaskList.Keys.ElementAt(0)], loggingService), TaskContinuationOptions.OnlyOnFaulted);

                            mTaskList.Remove(mTaskList.Keys.ElementAt(0));
                            mNumTaskOpen++;
                        }
                        else
                        {
                            //Si está arrancado lo eliminamos
                            mTaskList.Remove(mTaskList.Keys.ElementAt(0));
                        }
                    }
                    else
                    {
                        // No hay hilos disponibles, duermo un segundo para esperar a que se liberen
                        Thread.Sleep(1000);
                    }

                    EstaHiloActivo = true;
                }
            }
        }

        /// <summary>
        /// Al finalizar la tarea recogemos el resultado del hilo y en caso de error guardamos en el Log.
        /// </summary>
        /// <param name="pTareaFallida">Tarea que se ha procesado.</param>
        private void RelanzarTareaFallida(Task pTareaFallida, LoggingService loggingService)
        {
            if (pTareaFallida.IsFaulted)
            {
                Exception ex = pTareaFallida.Exception;
                if (ex != null)
                {
                    if (ex.InnerException != null)
                    {
                        ex = ex.InnerException;
                    }
                    loggingService.GuardarLog(loggingService.DevolverCadenaError(ex, "1.0.0.0"));
                }
            }
        }

        /// <summary>
        /// Abre un hilo por cada fichero que encuentre en el directorio de las visitas.
        /// </summary>
        private void ProcesarFicherosPendientesEnDirectorio()
        {
            string[] ficherosVisitas = Directory.GetFiles(mDirectorioVisitas);
            foreach (string fich in ficherosVisitas)
            {
                if (!fich.Equals(mDirectorioVisitas + mFicheroVisitas) && fich.EndsWith(mPuerto.ToString()))
                {
                    //Procesamos los ficheros que no están procesados.
                    AbrirHiloYProcesarFichero(fich, "Visitas");
                }
            }

            string[] ficherosVotos = Directory.GetFiles(mDirectorioVotos);
            foreach (string fich in ficherosVotos)
            {
                if (!fich.Equals(mDirectorioVotos + mFicheroVotos) && fich.EndsWith(mPuerto.ToString()))
                {
                    //Procesamos los ficheros que no están procesados.
                    AbrirHiloYProcesarFichero(fich, "Votos");
                }
            }

            string[] ficherosComentarios = Directory.GetFiles(mDirectorioComentarios);
            foreach (string fich in ficherosComentarios)
            {
                if (!fich.Equals(mDirectorioComentarios + mFicheroComentarios) && fich.EndsWith(mPuerto.ToString()))
                {
                    //Procesamos los ficheros que no están procesados.
                    AbrirHiloYProcesarFichero(fich, "Comentarios");
                }
            }

            string[] ficherosRecursos = Directory.GetFiles(mDirectorioRecursos);
            foreach (string fich in ficherosRecursos)
            {
                if (!fich.Equals(mDirectorioRecursos + mFicheroRecursos) && fich.EndsWith(mPuerto.ToString()))
                {
                    //Procesamos los ficheros que no están procesados.
                    AbrirHiloYProcesarFichero(fich, "recursos");
                }
            }
        }

        /// <summary>
        /// Abre un hilo donde se procesan las visitas del fichero pasado como param.
        /// </summary>
        /// <param name="pFich">Path completo del fichero a procesar.</param>
        private void AbrirHiloYProcesarFichero(string pFich, string pTipoDatoActualizacion)
        {
            // *************************************************************************
            // Abrir solo 5 hilos y cuando creemos uno comprobar que se puede procesar.
            // *************************************************************************

            ProcesarFichero procFich = new ProcesarFichero(ScopedFactory, mConfigService);

            procFich.TempFile = pFich;
            procFich.FicheroLog = mFicheroLog;
            procFich.TipoDatoActualizacion = pTipoDatoActualizacion;

            //Objetos que se bloquean cuando se va a esribir en el fichero, de tal manera que se impide que se den errores de concurrencia (escituras multiples sobre el mismo fichero)
            procFich.ObjErrorLock = objErrorLock;
            procFich.ObjLineaLock = objLineaLock;

            Task t = new Task(procFich.EmpezarMantenimiento);
            mTaskList.Add(pFich, t);
        }


        private void AbrirHiloYActualizarVisitas()
        {
            if (!mTaskList.ContainsKey("ActualizarVisitasVirtuoso"))
            {
                ActualizarNumeroVisitasVirtuoso actualizarVisitasVirtuoso = new ActualizarNumeroVisitasVirtuoso(mHorasIntervaloActualizarVisitasVirtuoso, ScopedFactory, mConfigService);
                Task t = new Task(actualizarVisitasVirtuoso.EmpezarMantenimiento);
                mTaskList.Add("ActualizarVisitasVirtuoso", t);
            }
        }

        /// <summary>
        /// Escritura de un paquete en el fichero pasado como parámetro
        /// </summary>
        /// <param name="pRutaFichero">Fichero en el que se van a escribir las líneas.</param>
        /// <param name="pLineaEscribir">Línea que se escribe en el fichero.</param>
        private void EscribirLinea(string pRutaFichero, string pLineaEscribir)
        {
            StreamWriter sw = new StreamWriter(pRutaFichero, true, Encoding.Default);
            sw.WriteLine(pLineaEscribir);
            sw.Flush();
            sw.Dispose();
        }

        #endregion

        #region Miembros de ICloneable

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new Controller_ProcessarLista(mNumLineasHilo, mNumHilosAbiertos, mMinutosAntesProcesar, mSocketsList, mPuerto, mHorasIntervaloActualizarVisitasVirtuoso, ScopedFactory, mConfigService);
        }

        #endregion
    }
}
