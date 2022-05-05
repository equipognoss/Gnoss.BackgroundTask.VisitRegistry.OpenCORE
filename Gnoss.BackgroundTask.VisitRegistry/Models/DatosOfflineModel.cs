using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Es.Riam.Gnoss.ServicioActualizacionOffline.Models
{
    public class DatosOfflineModel
    {
        public Guid DocumentoID { get; set; }
        public Guid ProyectoID { get; set; }
        public Guid IdentidadID { get; set; }
        public Guid BaseRecursosID { get; set; }
        public Guid IdentidadCreadorID { get; set; }
        public DateTime Fecha { get; set; }
        public long NumeroDeVisitas { get; set; }
    }
}
