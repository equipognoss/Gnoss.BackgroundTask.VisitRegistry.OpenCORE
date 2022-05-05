using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Net;
using Es.Riam.Gnoss.Servicios;
using System.Net.Sockets;
using Es.Riam.Gnoss.Recursos;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.ServicioActualizacionOffline
{
    internal class Controller_UDPListener : ControladorServicioGnoss
    {
        #region Miembros

        private volatile List<string> mListaSockets;
        private int mPUERTO_UDP_VISITAS;

        #endregion

        #region Constructores

        public Controller_UDPListener(List<string> pListaSockets, int pPuerto, IServiceScopeFactory serviceScopeFactory, ConfigService configService)
            : base(serviceScopeFactory, configService)
        {
            mListaSockets = pListaSockets;
            mPUERTO_UDP_VISITAS = pPuerto;
        }

        #endregion

        #region Metodos generales

        /// <summary>
        /// Procesa las solicitudes TCP entrantes en el servidor
        /// </summary>
        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            string urlIntragnoss = "";
            try
            {
                GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
                ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
                parametroAplicacionGBD.ObtenerConfiguracionGnoss(gestorParametroAplicacion);
                mUrlIntragnoss = gestorParametroAplicacion.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;
            }
            catch (Exception ex)
            {
                loggingService.GuardarLog("UDPListener ERROR: Obteniendo urlIntragnoss.");
                loggingService.GuardarLog(ex.Message);
                throw;
            }

            bool puertoUdpBloqueado = EstaElPuertoEnUso(mPUERTO_UDP_VISITAS);

            if (!puertoUdpBloqueado)
            {
                //Creamos el canal de comunicación por el puerto cargado de configuración
                ClienteUDP = new UdpClient(mPUERTO_UDP_VISITAS);
                loggingService.GuardarLog("UDPListener: '" + mFicheroConfiguracionBD + "' Escuchando por el puerto: " + mPUERTO_UDP_VISITAS);

                // Permitimos que más de un cliente escuche en el mismo puerto.
                ClienteUDP.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

                EstaHiloActivo = true;

                while (true)
                {

                    ComprobarCancelacionHilo();

                    try
                    {
                        //Clases que recogen los datos
                        IPEndPoint conexionEntrante = null;
                        string datosRecibidos = Encoding.ASCII.GetString(ClienteUDP.Receive(ref conexionEntrante));

                        if (datosRecibidos.Equals(SOCKETSOFFLINE_ACTIVADOR_UDP))
                        {
                            // Marcar el hilo activo y también en el diccionario de Hilos 
                            EstaHiloActivo = true;
                        }
                        else if (datosRecibidos.Equals(SOCKETSOFFLINE_CANCELADOR_HILO_UDP))
                        {
                            //Cerramos la conexión de la variable UDP
                            ClienteUDP.Close();
                            ClienteUDP = null;
                            ControladorConexiones.CerrarConexiones();
                            break;
                        }
                        else
                        {
                            mListaSockets.Add(datosRecibidos);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        //Cerramos la conexión de la variable UDP
                        ClienteUDP.Close();
                        ClienteUDP = null;
                        ControladorConexiones.CerrarConexiones();
                        break;
                    }
                    catch (ObjectDisposedException)
                    {
                        //Cerramos la conexión de la variable UDP
                        ClienteUDP.Close();
                        ClienteUDP = null;
                        ControladorConexiones.CerrarConexiones();
                        break;
                    }
                    catch (Exception ex)
                    {
                        loggingService.GuardarLog("UDPListener ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                    }
                }

                if (ClienteUDP != null)
                {
                    ClienteUDP.Close();
                    ClienteUDP = null;
                }
            }
        }

        private bool EstaElPuertoEnUso(int pPUERTO_UDP_VISITAS)
        {
            return (from p in System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties().GetActiveUdpListeners() where p.Port == pPUERTO_UDP_VISITAS select p).Count() == 1;
        }

        #endregion

        #region Miembros de ICloneable

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new Controller_UDPListener(mListaSockets, mPUERTO_UDP_VISITAS, ScopedFactory, mConfigService);
        }

        #endregion
    }
}
