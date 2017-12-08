# DESCRIPCION SCRIPT -----------------------------------------------------------------------------------------
# NOMBRE: SCI, SPI LOAD
# CODIGO: sci_spi_load.R
# VERSION: 01.01-170817
# AUTOR: PATRICIO FUENMAYOR VITERI
# FECHA PROGRAMACION: 2017-08-17
# DETALLE: CARGA DE ARCHIVOS SCI Y SPI
# HISTORICO VERSIONES (versión|cambio|solicitante|realizador):
# 01.01-170817|Inicio de versionamiento|evallejo|pfuenmay

# CARGA DE CONFIGURACIONES INICIALES -------------------------------------------------------------------------
source("d:/spt_mx/scr/r_ini.R")
source("d:/git/sci_spi/id_vld.R")
source("d:/git/sci_spi/sci_spi_fn.R")
source("d:/git/sci_spi/sci_spi_etl.R")

# DESCARGA ARCHIVOS FTP --------------------------------------------------------------------------------------
usr_pwd <- "uio\\usrpmure:Pichincha2"
#usr_pwd <- "uio\\pfuenmay:MUREX?!nov2017"

# fechas de referencia por día o por mes
#rpt_dt <- fmt_day_mnt("2017-12","m")
rpt_dt <- fmt_day_mnt("2017-12-07","d")

# directorios
dir_rpt <- "d:/rpt_mx/sci_spi"
dir_cfg <- "d:/spt_mx/cfg"
dir_ftp <- "ftp://10.0.0.32/Regionales/SNP/OPERACIONES"
dir_ls <- paste0(dir_ftp,"/bp_2017_",c("sci","spi"),rep(1:4,each=2),"/")
dir_ls <- dir_ls[!str_detect(dir_ls,"sci1|sci4")]

ftp_ls <- data.table()
for(dir_nm in dir_ls){
ftp_ls01 <- getURL(dir_nm,userpwd=usr_pwd,ftp.use.epsv=FALSE,dirlistonly=TRUE)
ftp_ls02 <- data.table(dir=dir_nm,fil=unlist(str_split(ftp_ls01,"\r*\n")))
ftp_ls <- rbindlist(list(ftp_ls,ftp_ls02))}

# ARCHIVOS A DESCARGAR
dwn_ls <- ftp_ls[str_detect(fil,rpt_dt)] #por fecha
#dwn_ls <- ftp_ls[str_detect(fil,rpt_mnt)] #por mes
#dwn_ls <- ftp_ls[str_detect(fil,"2017_|-2017")] #todos
dwn_ls[,":="(dir_ftp=paste0(dir,fil),dir_loc=paste0(dir_rpt,"/",fil))]

cnx <-getCurlHandle(ftp.use.epsv=FALSE,userpwd=usr_pwd)
dwn_ls[,.(mapply(function(url,fil) writeBin(getBinaryURL(url,curl=cnx,dirlistonly=FALSE),fil),dir_ftp,dir_loc))]

# DESCOMPRIME ARCHIVOS ZIP
system(paste0("7za.exe e -y ",dir_rpt,"/*.zip -o",dir_rpt))

# DEFINCION DIRECTORIOS Y LISTA DE ARCHIVOS ------------------------------------------------------------------
loc_ls <- data.table(fil=dir(dir_rpt))
file.remove(paste0(dir_rpt,"/",loc_ls[str_detect(fil,"\\.MD5$|\\.md5$|\\.zip$"),fil]),showWarnings=FALSE)
loc_ls <- data.table(fil=dir(dir_rpt))

#-------------------------------------------------------------------------------------------------------------
# ESTRUCTURA SPI1
fil_ls <- loc_ls[str_detect(fil,"BP_SPI1.*.txt") & str_detect(fil,rpt_dt),fil]
#fil_ls <- loc_ls[str_detect(fil,"BP_SPI1.*.txt"),fil]
spi1 <- sci_spi_load(dir_nm=dir_rpt,fil_ls,rpt_nm="spi1",cut_prc_pos=9)

# ESTRUCTURA SPI4
fil_ls <- loc_ls[str_detect(fil,"spi04.*.txt") & str_detect(fil,rpt_dt),fil]
#fil_ls <- loc_ls[str_detect(fil,"spi04.*.txt"),fil]
spi4 <- sci_spi_load(dir_nm=dir_rpt,fil_ls,rpt_nm="spi4",cut_prc_pos=7)

#-------------------------------------------------------------------------------------------------------------
# ESTRUCTURA SPI2
fil_ls <- loc_ls[str_detect(fil,"SPI02.*.txt") & str_detect(fil,rpt_dt),fil]
#fil_ls <- loc_ls[str_detect(fil,"SPI02.*.txt"),fil]
spi2 <- sci_spi_load(dir_nm=dir_rpt,fil_ls,rpt_nm="spi2",cut_prc_pos=7)

# ESTRUCTURA SPI3
fil_ls <- loc_ls[str_detect(fil,"BP_SPI3.*.txt") & str_detect(fil,rpt_dt),fil]
#fil_ls <- loc_ls[str_detect(fil,"BP_SPI3.*.txt"),fil]
spi3 <- sci_spi_load(dir_nm=dir_rpt,fil_ls,rpt_nm="spi3",cut_prc_pos=9)

#-------------------------------------------------------------------------------------------------------------
# ESTRUCTURA SCI2
fil_ls <- loc_ls[str_detect(fil,"sci2.*.txt") & str_detect(fil,rpt_dt),fil]
#fil_ls <- loc_ls[str_detect(fil,"sci2.*.txt"),fil]
sci2 <- sci_spi_load(dir_nm=dir_rpt,fil_ls,rpt_nm="sci2")

# ESTRUCTURA SCI3
fil_ls <- loc_ls[str_detect(fil,"SCI3.*.txt") & str_detect(fil,rpt_dt),fil]
#fil_ls <- loc_ls[str_detect(fil,"SCI3.*.txt"),fil]
sci3 <- sci_spi_load(dir_nm=dir_rpt,fil_ls,rpt_nm="sci3")
