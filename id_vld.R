# DESCRIPCION SCRIPT -----------------------------------------------------------------------------------------
# NOMBRE: ID VALIDATION
# CODIGO: id_vld.R
# VERSION: 01.01-170910
# AUTOR: PATRICIO FUENMAYOR VITERI
# FECHA PROGRAMACION: 2017-09-10
# DETALLE: VALIDA IDENTIFICACIONES CEDULAS Y RUCS
# HISTORICO VERSIONES (versión|cambio|solicitante|realizador):
# 01.01-170910|Inicio de versionamiento|evallejo|pfuenmay

# FUNCION PARA VALIDAR IDENTIFICACIONES ----------------------------------------------------------------------
id_vld <- function(id_cod,id_tp){
# luhn algoritm: mdl = 10, 11
luhn_alg <- function(dgt,mdl,mlt){
# lista multiplicadores
mlt_ls <- list(mlt01=c(4,3,2,7,6,5,4,3,2),
               mlt02=c(3,2,7,6,5,4,3,2),
               mlt03=c(2,1,2,1,2,1,2,1,2))
# realiza los calculos
ret <- as.numeric(unlist(str_split(dgt,"")))
calc <- ret * unlist(mlt_ls[[mlt]])
if(mdl==10) vld <- (10-(sum(calc%%10)+sum(calc%/%10))%%10)%%10
if(mdl==11) vld <- (11-(sum(calc)%%11))%%11
return(as.integer(vld))}
# crea data.table para calculos
dat <- data.table(id_cod,id_tp)[,":="(uid=1:.N)]
# crea variables de filtro
dat[,":="(ruc_tp=as.integer(str_sub(id_cod,3,3)),id_lgt=str_length(id_cod),id_prv=as.integer(str_sub(id_cod,1,2)))]
dat[,":="(ruc_adc=ifelse(id_tp==3,as.integer(str_sub(id_cod,start=11)),1L))]
dat[,":="(lgt=ifelse(id_tp==1 & id_lgt==10,TRUE,ifelse(id_tp==3 & id_lgt==13,TRUE,FALSE)))]
# filtra tipos nulos y diferentes a ruc, cédula y rucs no validos
dat[is.na(id_tp)|!id_tp %in% c(1,3)|ruc_tp %in% c(7,8)|lgt==FALSE|ruc_adc==0|!between(id_prv,1,24),":="(exc=TRUE,vld=FALSE)]
# estructura de validación
dat_exc <- dat[exc==TRUE]
dat <- dat[is.na(exc)]
dat[,":="(dgt=ifelse(ruc_tp==6,str_sub(id_cod,end=8),str_sub(id_cod,end=9)))]
dat[,":="(chk=as.integer(ifelse(ruc_tp==6,str_sub(id_cod,9,9),str_sub(id_cod,10,10))))]
dat[,":="(mdl=ifelse(ruc_tp %in% c(6,9),11,10))]
dat[,":="(mlt=ifelse(ruc_tp==9,1,ifelse(ruc_tp==6,2,3)))]
dat[,chk_c:=unlist(parallel::mcMap(luhn_alg,dgt,mdl,mlt))]
#dat[,":="(chk_c=luhn_alg(dgt,mdl,mlt)),uid]
dat[,":="(vld=ifelse(chk==chk_c,TRUE,FALSE))]
dat[,c("dgt","chk","mdl","mlt","chk_c"):=NULL]
dat <- rbindlist(list(dat_exc,dat))[order(uid)]
#dat[is.na(exc),":="(dgt=ifelse(ruc_tp==6,str_sub(id_cod,end=8),str_sub(id_cod,end=9)))]
#dat[is.na(exc),":="(chk=as.integer(ifelse(ruc_tp==6,str_sub(id_cod,9,9),str_sub(id_cod,10,10))))]
#dat[is.na(exc),":="(mdl=ifelse(ruc_tp %in% c(6,9),11,10))]
#dat[is.na(exc),":="(mlt=ifelse(ruc_tp==9,1,ifelse(ruc_tp==6,2,3)))]
#dat[is.na(exc),":="(chk_c=luhn_alg(dgt,mdl,mlt)),uid]
#dat[is.na(exc),":="(vld=ifelse(chk==chk_c,TRUE,FALSE))]
dat[,":="(vld=as.integer(vld))]
return(dat[,vld])}
