# DESCRIPCION SCRIPT -----------------------------------------------------------------------------------------
# NOMBRE: SCI SPI FUNCTIONS
# CODIGO: sci_spi_fn.R
# VERSION: 01.01-171010
# AUTOR: PATRICIO FUENMAYOR VITERI
# FECHA PROGRAMACION: 2017-10-10
# DETALLE: FUNCIONES PARA SCI Y SPI
# HISTORICO VERSIONES (versión|cambio|solicitante|realizador):
# 01.01-171010|Inicio de versionamiento|pfuenmay|pfuenmay

# FUNCION PARA CARGAR LISTA DE ARCHIVOS PLANOS USANDO data.table::fread --------------------------------------
sci_spi_load <- function(dir_nm,fil_ls,rpt_nm,tp,cut_prc_pos=0,quote="\'"){
# funcion para llamado de variables
`..` <- function (...,.env=environment()) get(deparse(substitute(...)),env=.env)
# carga archivo de configuraciones
rpt_cfg <- data.table(read_excel(paste0(dir_cfg,"/rpt_cfg.xlsx"),sheet="sci_spi"))
rpt_h <- paste0(rpt_nm,"h")
head_cfg <- rpt_cfg[rpt_nm==..(rpt_h),.(var_cod,var_tp=hom_tp_dat_r(ini_var_tp))]
rpt_d <- paste0(rpt_nm,"d")
dat_cfg <- rpt_cfg[rpt_nm==..(rpt_d),.(var_cod,var_tp=hom_tp_dat_r(ini_var_tp))]
# proceso acumulativo de archivos
head_ls <- list()
dat_ls <- list()
log_prc_id <- 1
for(fil_nm in fil_ls){
# variables
cut_prc <- ifelse(cut_prc_pos==0,1,as.numeric(str_sub(fil_nm,cut_prc_pos,cut_prc_pos)))
# headers
head01 <- data.table(read.table(paste0(dir_nm,"/",fil_nm),sep=",",nrows=1,colClasses=head_cfg[,var_tp],
            col.names=head_cfg[,var_cod],stringsAsFactors=FALSE))
head01[,":="(fil_nm=fil_nm,cut_prc=cut_prc,log_prc_id=log_prc_id)]
# info_dt
info_dt <- as.Date(dmy_hms(head01[1,1]))

head_ls <- append(head_ls,list(head01))
# data
dat01 <- fread(paste0(dir_nm,"/",fil_nm),sep=",",header=FALSE,skip=1,quote=quote,colClasses=dat_cfg[,var_tp],
           col.names=dat_cfg[,var_cod])
dat01[,":="(cut_prc=cut_prc,log_prc_id=log_prc_id,info_dt=info_dt)]
dat_ls <- append(dat_ls,list(dat01))
# incrementa log process
log_prc_id <- log_prc_id+1}
head_ls <- rbindlist(head_ls)
dat_ls <- rbindlist(dat_ls)
return(list(head=head_ls,dat=dat_ls))}

# FUNCIONES PARA PREPARACION DE DATOS ------------------------------------------------------------------------
# modifica campos texto
txt_mdf <- function(dt,cols_mdf){
dt[,(cols_mdf):=lapply(.SD,str_replace_all,"Ã.","Ñ"),.SDcols=cols_mdf]
dt[,(cols_mdf):=lapply(.SD,hom_txt,myc=TRUE,spc_chr=TRUE),.SDcols=cols_mdf]
return(dt)}
# modifica variables datetime y crea variables date y time (en horas)
dttm_mdf <- function(dt,cols_mdf){
cols_dttm <- paste0(cols_mdf,"_dttm")
cols_dt <- paste0(cols_mdf,"_dt")
cols_tm <- paste0(cols_mdf,"_tm")
dt[,(cols_dttm):=lapply(.SD,dmy_hms),.SDcols=cols_dttm] # convierte variables datetime
dt[,(cols_dt):=lapply(.SD,as_date),.SDcols=cols_dttm] # crea date
dt[,(cols_tm):=lapply(.SD,function(x) hour(x)+minute(x)/60),.SDcols=cols_dttm] # crea time (horas)
return(dt)}
# funcion para formatos de fecha y mes
fmt_day_mnt <- function(dat,tp){
if(tp=="m") {dat <- as.Date(paste0(dat,"-01"));dat <- paste0(format(dat,"%Y_%m"),"|",format(dat,"%m-%Y"))}
else {dat <- as.Date(dat); dat <- paste0(format(dat,"%Y_%m_%d"),"|",format(dat,"%d-%m-%Y"))}
return(dat)}

# funcion para eliminar registros en multiples tablas
del_info_tbl <- function(cnx,tbl,prm,vls){
for(t in tbl){
sql_del <- paste0("delete from ",t," where ",prm," in ",vls)
dbSendQuery(cnx,sql_del)}}

# proceso para validar identificaciones
id_vld_pcs <- function(dt){
# crear filtro con tabla id
dt <- copy(id_cnt01)

cols_mdf <- c("id_cod","cnt_nm")
cols_lng <- paste0(cols_mdf,"_lng")
dt[,(cols_lng):=lapply(.SD,str_length),.SDcols=cols_mdf]
dt[,":="(id_tp=ifelse(id_cod_lng>10,3L,1L),id_cod_ini=id_cod)] # cédula=1, ruc=3 (catálogo)
dt1 <- dt[!str_detect(id_cod,"\\D") & id_cod_lng %in% c(9,10,12,13)]
dt1[id_cod_lng==9,":="(id_cod=str_pad(id_cod,10,side="left",pad="0"))] # completa tamaño en cédula
dt1[id_cod_lng==12,":="(id_cod=str_pad(id_cod,13,side="left",pad="0"))] # completa tamaño en ruc
dt1[,":="(id_vld=id_vld(id_cod,id_tp))] # ejecuta validación identificaciones


dt1[,.N,.(id_tp,id_vld)]
dt1[id_tp==3 & id_vld==0]


dt1[id_vld==0,":="(id_tp=2)]

id_fix <- dt1[id_cod_lng %in% c(9,12) & id_vld==1]

dt1[,.N,id_tp]

dt2 <- dt[str_detect(id_cod,"\\D") | !id_cod_lng %in% c(9,10,12,13)]
dt2[,":="(id_tp=2,id_vld=0L)] # pasaporte=2 (contiene letras y tamaños diferentes a (9,10,12,13))
dt <- rbindlist(list(dt1,dt2))
dt[,":="(id_cod_lng=str_length(id_cod))]

# extrae cédulas de rucs para los que son válidos
dt[,":="(id_cod_tmp=ifelse(id_tp==3 & as.numeric(str_sub(id_cod,3,3)) %in% c(0:6,9),str_sub(id_cod,end=10),id_cod))] # extrae 10 digitos del ruc
dt_c <- unique(dt[id_tp!=2 & as.numeric(str_sub(id_cod_tmp,3,3))<7,.(id_cod=id_cod_tmp)])[,":="(cnt_uid=1:.N)] # cédulas únicas
dt_r <- dt[id_tp==3,-2] # todos los rucs
dt1_1 <- melt(merge(dt_r,dt_c[,.(id_cod,cnt_uid)],by.x="id_cod_tmp",by.y="id_cod",all=TRUE),id.vars=c("cnt_uid"),measure.vars=c("id_cod","id_cod_tmp"))[order(cnt_uid)]
dt1_2 <- unique(dt1_1[!is.na(cnt_uid) & !is.na(value),.(cnt_uid,id_cod=value)])
dt1_3 <- unique(dt1_1[is.na(cnt_uid) & !is.na(value) & variable=="id_cod",.(id_cod=value)])[,":="(cnt_uid=max(dt1_2[,cnt_uid])+1:.N)]
dt1 <- rbindlist(list(dt1_2,dt1_3),fill=TRUE)
dt1[,":="(id_cod_prm=1:.N),cnt_uid]
dt1[,":="(id_tp=ifelse(str_length(id_cod)>10,3L,1L))] # cédula=1, ruc=3 (catálogo)
dt_p <- dt[id_tp==2,.(id_cod,id_cod_prm=1L,id_tp,cnt_uid=max(dt1[,cnt_uid])+1:.N)] # todos los pasaportes

# hasta aqui validado

# toma únicos en base a mayor longitud y luego por primera posición
#dt2 <- unique(dt1[dt1[id_vld==1,.(cnt_nm_lng=max(cnt_nm_lng)),id_cod],on=.(id_cod,cnt_nm_lng)],by="id_cod")

dt2 <- unique(dt1[dt[,.(cnt_nm_lng=max(cnt_nm_lng)),id_cod_vld],on=.(id_cod_vld,cnt_nm_lng)],by="id_cod_vld")

# falta asignar el id unico
#> dt1[id_cod %in% dt1[,.N,id_cod][order(-N)][N==2,id_cod]][order(id_cod)]
#       id_cod                         cnt_nm id_cod_lng cnt_nm_lng id_tp id_cod_vld id_vld
#1: 0120817643               JOSE LUIS ACOSTA         10         16     1 0120817643      0
#2: 0120817643               JOSE LUIS ACOSTA          9         16     1  120817643      0
#3: 0138398615               JOSE DANIEL DIAZ         10         16     1 0138398615      1
#4: 0138398615                    JOSE D DIAZ          9         11     1  138398615      1
#5: 0909614190 MONTANERO SOLEDISPA EDUARDO AR          9         30     1  909614190      1
#6: 0909614190    MONTANERO SOLEDISPA EDUARDO         10         27     1 0909614190      1


dt1[,c("id_cod_lng","cnt_nm_lng","id_vld"):=NULL]


unique(dt1[,.(id_cod_vld)])

dt1[id_cod %in% dt1[,.N,id_cod][order(-N)][N==2,id_cod]][order(id_cod)]



return(dt2)}
