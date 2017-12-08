# DESCRIPCION SCRIPT -----------------------------------------------------------------------------------------
# NOMBRE: SCI, SPI ETL
# CODIGO: sci_spi_etl.R
# VERSION: 01.01-170817
# AUTOR: PATRICIO FUENMAYOR VITERI
# FECHA PROGRAMACION: 2017-08-17
# DETALLE: ETL PARA SCI Y SPI
# HISTORICO VERSIONES (versión|cambio|solicitante|realizador):
# 01.01-170817|Inicio de versionamiento|evallejo|pfuenmay
# PACKAGES ---------------------------------------------------------------------------------------------------
require(RPostgreSQL)
require(dataPreparation)

# CONEXION POSTGRESQL
cnx_pg01 <- dbConnect(PostgreSQL(),user="postgres",password="postgres",dbname="sci_spi")
#cnx_pg01 <- dbConnect(PostgreSQL(),user="postgres",password="postgres",dbname="sci_spi",host="10.0.5.104")

# VALIDACION DE ARCHIVOS -------------------------------------------------------------------------------------
# SPI1 (con archivo spi4)
spi1_vld_1 <- spi1$dat[spi4$dat[,.(itt_ptt_ref_cod,sta_end_cod,info_dt)],on=.(info_dt,itt_ptt_ref_cod)]
spi1_vld_2 <- spi1_vld_1[,.(amn=sum(ifelse(is.na(amn),0,amn)),num_row=.N),.(info_dt,cut_prc,sta_end_cod)][order(-amn)]
spi1_vld_2[,":="(sta_end_ar=ifelse(sta_end_cod==1,"A","R"))]
spi1_vld_3 <- spi1_vld_2[,.(amn=sum(amn),num_row=sum(num_row)),.(info_dt,sta_end_ar,cut_prc)][,":="(tp_cod="spi1")]
spi1_vld <- dcast(spi1_vld_3,info_dt~tp_cod+cut_prc+sta_end_ar,fun=sum,value.var="amn")

# SPI2 (con archivo spi3)
spi2_vld_1 <- spi2$dat[spi3$dat[,.(bce_ref_cod,bce_seq_cod,sta_end_cod,info_dt)],on=.(info_dt,bce_ref_cod,bce_seq_cod)]
spi2_vld_2 <- spi2_vld_1[,.(amn=sum(ifelse(is.na(amn),0,amn)),num_row=.N),.(info_dt,cut_prc,sta_end_cod)][order(-amn)]
spi2_vld_2[,":="(sta_end_ar=ifelse(sta_end_cod==1,"A","R"))]
spi2_vld_3 <- spi2_vld_2[,.(amn=sum(amn),num_row=sum(num_row)),.(info_dt,sta_end_ar,cut_prc)][,":="(tp_cod="spi2")]
spi2_vld <- dcast(spi2_vld_3,info_dt~tp_cod+cut_prc+sta_end_ar,fun=sum,value.var="amn")

# SCI2 (con archivo sci3)
sci2_vld_1 <- sci2$dat[sci3$dat[,.(bce_ref_cod,bce_seq_cod,sta_end_cod,info_dt)],on=.(info_dt,bce_ref_cod,bce_seq_cod)]
sci2_vld_2 <- sci2_vld_1[,.(amn=sum(ifelse(is.na(amn),0,amn)),num_row=.N),.(info_dt,cut_prc,sta_end_cod)][order(-amn)]
sci2_vld_2[,":="(sta_end_ar=ifelse(sta_end_cod==12,"A","R"))]
sci2_vld_3 <- sci2_vld_2[,.(amn=sum(amn),num_row=sum(num_row)),.(info_dt,sta_end_ar,cut_prc)][,":="(tp_cod="sci2")]
sci2_vld <- dcast(sci2_vld_3,info_dt~tp_cod+cut_prc+sta_end_ar,fun=sum,value.var="amn")

# VALIDACION GLOBAL ------------------------------------------------------------------------------------------
# concatena información
sci_spi_vld_1 <- spi1_vld[spi2_vld,on=.(info_dt)][sci2_vld,on=.(info_dt)]
# cálculos
sci_spi_vld_1[,":="(spi_1_snd=spi1_1_A+spi1_1_R+spi2_1_R,spi_1_rcv=spi2_1_A+spi2_1_R+spi1_1_R)]
sci_spi_vld_1[,":="(spi_2_snd=spi1_2_A+spi1_2_R+spi2_2_R,spi_2_rcv=spi2_2_A+spi2_2_R+spi1_2_R)]
sci_spi_vld_1[,":="(spi_3_snd=spi1_3_A+spi1_3_R+spi2_3_R+sci2_1_A,spi_3_rcv=spi2_3_A+spi2_3_R+spi1_3_R)]
sci_spi_vld_1[,":="(sci=sci2_1_A+sci2_1_R,spi_snd=spi_1_snd+spi_2_snd+spi_3_snd,spi_rcv=spi_1_rcv+spi_2_rcv+spi_3_rcv)]
sci_spi_vld_1[,":="(net=spi_rcv-spi_snd)]

vld <- sci_spi_vld_1[,.(info_dt,spi_1_snd,spi_1_rcv,spi_2_snd,spi_2_rcv,spi_3_snd,spi_3_rcv,sci,spi_snd,spi_rcv,net)]
#to_excel(vld,"d:/","sci_spi_vld","vld")
#dbSendQuery(cnx_pg01,"truncate table vld")
dbWriteTable(cnx_pg01,'vld',vld,append=TRUE,row.names=FALSE)

# INFORMACION RESUMIDA ---------------------------------------------------------------------------------------
# SPI1
# recodificacion variables
rcv_acc_tp <- data.table(read_excel(paste0(dir_cfg,"/rpt_cfg.xlsx"),sheet="rcv"))[str_detect(cv_nm,"TIPO_CUENTA"),.(cv_cod,rcv_des_abbr)]
setnames(rcv_acc_tp,"rcv_des_abbr","acc_tp_abbr")
spi1_vld_1 <- spi1_vld_1[rcv_acc_tp,on=.(ptt_acc_tp=cv_cod)]

# niveles de agregación (montos)
spi1_1 <- spi1_vld_1[sta_end_cod==1,.(amn=sum(ifelse(is.na(amn),0,amn)),num_row=.N),.(info_dt,acc_tp_abbr,ptt_acc,cut_prc)]
spi1_1[,":="(amn_cut=ifelse(amn>=500000,">=500000","<500000"))]
spi1_1_amn_1 <- dcast(spi1_1,info_dt~cut_prc+acc_tp_abbr+amn_cut,fun=sum,value.var="amn")
spi1_1_amn_2 <- dcast(spi1_1,info_dt~cut_prc+acc_tp_abbr,fun=sum,value.var="amn")
spi1_1_amn_3 <- dcast(spi1_1,info_dt~cut_prc,fun=sum,value.var="amn")
spi1_1_amn_4 <- dcast(spi1_1,info_dt~acc_tp_abbr,fun=sum,value.var="amn")
spi1_1_amn_5 <- spi1_1[,.(TOT=sum(amn)),info_dt]
spi1_1_amn <- spi1_1_amn_5[spi1_1_amn_4,on=.(info_dt)][spi1_1_amn_3,on=.(info_dt)][spi1_1_amn_2,on=.(info_dt)][spi1_1_amn_1,on=.(info_dt)]
#to_excel(spi1_1_amn,"d:/","spi1_amn_","vld")
#dbSendQuery(cnx_pg01,"truncate table spi1_amn")
dbWriteTable(cnx_pg01,'spi1_amn',spi1_1_amn,append=TRUE,row.names=FALSE)

# niveles de agregación (transacciones)
spi1_1_nrow_1 <- dcast(spi1_1,info_dt~cut_prc+acc_tp_abbr+amn_cut,fun=sum,value.var="num_row")
spi1_1_nrow_2 <- dcast(spi1_1,info_dt~cut_prc+acc_tp_abbr,fun=sum,value.var="num_row")
spi1_1_nrow_3 <- dcast(spi1_1,info_dt~cut_prc,fun=sum,value.var="num_row")
spi1_1_nrow_4 <- dcast(spi1_1,info_dt~acc_tp_abbr,fun=sum,value.var="num_row")
spi1_1_nrow_5 <- spi1_1[,.(TOT=sum(num_row)),info_dt]
spi1_1_nrow <- spi1_1_nrow_5[spi1_1_nrow_4,on=.(info_dt)][spi1_1_nrow_3,on=.(info_dt)][spi1_1_nrow_2,on=.(info_dt)][spi1_1_nrow_1,on=.(info_dt)]
#to_excel(spi1_1_nrow,"d:/","spi1_nrow_","vld")
#dbSendQuery(cnx_pg01,"truncate table spi1_nrow")
dbWriteTable(cnx_pg01,'spi1_nrow',spi1_1_nrow,append=TRUE,row.names=FALSE)

# SPI2
# recodificacion variables
rcv_acc_tp <- data.table(read_excel(paste0(dir_cfg,"/rpt_cfg.xlsx"),sheet="rcv"))[str_detect(cv_nm,"TIPO_CUENTA_2"),.(cv_cod,rcv_des_abbr)]
setnames(rcv_acc_tp,"rcv_des_abbr","acc_tp_abbr")
spi2_vld_1 <- spi2_vld_1[rcv_acc_tp,on=.(bnf_acc_tp=cv_cod)]
spi2_vld_1[,":="(cnt_tp=ifelse(itt_ptt_acc_bce=="1820030","SEC_PUB","OTR"))]

# niveles de agregación (montos)
spi2_1 <- spi2_vld_1[sta_end_cod==1,.(amn=sum(ifelse(is.na(amn),0,amn)),num_row=.N),.(info_dt,acc_tp_abbr,cut_prc,cnt_tp)]
spi2_1_amn_1 <- dcast(spi2_1,info_dt~cut_prc+cnt_tp+acc_tp_abbr,fun=sum,value.var="amn")
spi2_1_amn_2 <- dcast(spi2_1,info_dt~cut_prc+cnt_tp,fun=sum,value.var="amn")
spi2_1_amn_3 <- dcast(spi2_1,info_dt~cut_prc,fun=sum,value.var="amn")
spi2_1_amn_4 <- dcast(spi2_1,info_dt~cnt_tp,fun=sum,value.var="amn")
spi2_1_amn_5 <- spi2_1[,.(TOT=sum(amn)),info_dt]
spi2_1_amn <- spi2_1_amn_5[spi2_1_amn_4,on=.(info_dt)][spi2_1_amn_3,on=.(info_dt)][spi2_1_amn_2,on=.(info_dt)][spi2_1_amn_1,on=.(info_dt)]
#to_excel(spi2_1_amn,"d:/","spi2_amn_","vld")
#dbSendQuery(cnx_pg01,"truncate table spi2_amn")
dbWriteTable(cnx_pg01,'spi2_amn',spi2_1_amn,append=TRUE,row.names=FALSE)

# niveles de agregación (transacciones)
spi2_1_nrow_1 <- dcast(spi2_1,info_dt~cut_prc+cnt_tp+acc_tp_abbr,fun=sum,value.var="num_row")
spi2_1_nrow_2 <- dcast(spi2_1,info_dt~cut_prc+cnt_tp,fun=sum,value.var="num_row")
spi2_1_nrow_3 <- dcast(spi2_1,info_dt~cut_prc,fun=sum,value.var="num_row")
spi2_1_nrow_4 <- dcast(spi2_1,info_dt~cnt_tp,fun=sum,value.var="num_row")
spi2_1_nrow_5 <- spi2_1[,.(TOT=sum(num_row)),info_dt]
spi2_1_nrow <- spi2_1_nrow_5[spi2_1_nrow_4,on=.(info_dt)][spi2_1_nrow_3,on=.(info_dt)][spi2_1_nrow_2,on=.(info_dt)][spi2_1_nrow_1,on=.(info_dt)]
#to_excel(spi2_1_nrow,"d:/","spi2_nrow_","vld")
#dbSendQuery(cnx_pg01,"truncate table spi2_nrow")
dbWriteTable(cnx_pg01,'spi2_nrow',spi2_1_nrow,append=TRUE,row.names=FALSE)

# SCI
sci2_vld_1[,":="(cnt_tp=ifelse(itt_cll_acc_bce=="1820030","SEC_PUB","OTR"))]
sci2_1 <- sci2_vld_1[sta_end_cod==12,.(amn=sum(ifelse(is.na(amn),0,amn)),num_row=.N),.(info_dt,cnt_tp,cll_id)]
sec_pub <- data.table(id=c("1760013210001","1760003090001","1760004650001"),nm=c("SRI","CFN","IESS"))
sci2_1 <- sec_pub[sci2_1,on=.(id=cll_id)]
sci2_1_amn_1 <- dcast(sci2_1,info_dt~cnt_tp+nm,fun=sum,value.var="amn")
sci2_1_amn_2 <- dcast(sci2_1,info_dt~cnt_tp,fun=sum,value.var="amn")
sci2_1_amn_3 <- sci2_1[,.(TOT=sum(amn)),info_dt]
sci2_1_amn <- sci2_1_amn_3[sci2_1_amn_2,on=.(info_dt)][sci2_1_amn_1,on=.(info_dt)]
#to_excel(sci2_1_amn,"d:/","sci2_amn_","vld")
#dbSendQuery(cnx_pg01,"truncate table sci2_amn")
dbWriteTable(cnx_pg01,'sci2_amn',sci2_1_amn,append=TRUE,row.names=FALSE)

# ELIMINA INFORMACION DE TABLAS DEN BASE DE DATOS
#del_info_tbl(cnx_pg01,c('vld','spi1_amn','spi1_nrow','spi2_amn','spi2_nrow','sci2_amn'),"info_dt","('2017-12-07')")

# PREPARACION DE DATOS ---------------------------------------------------------------------------------------
# SPI1 -------------------------------------------------------------------------------------------------------
# corrección de nombres y campos informativos
cols_mdf <- c("ptt_nm","itt_ptt_loc_cod","bnf_nm","add_info")
spi1$dat <- txt_mdf(spi1$dat,cols_mdf)
# convierte variables datetime
cols_mdf <- c("itt_ptt_reg")
spi1$dat <- dttm_mdf(spi1$dat,cols_mdf)

# SPI2 -------------------------------------------------------------------------------------------------------
# corrección de nombres y campos informativos
cols_mdf <- c("ptt_nm","itt_ptt_loc_cod","bnf_nm","add_info")
spi2$dat <- txt_mdf(spi2$dat,cols_mdf)
# convierte variables datetime
cols_mdf <- c("itt_ptt_reg","bce_cps")
spi2$dat <- dttm_mdf(spi2$dat,cols_mdf)

# SCI2 -------------------------------------------------------------------------------------------------------
# corrección de nombres y campos informativos
#cols_mdf <- c()
#sci2$dat <- txt_mdf(sci2$dat,cols_mdf)
# convierte variables datetime
cols_mdf <- c("itt_cll_reg","bce_dtb")
sci2$dat <- dttm_mdf(sci2$dat,cols_mdf)

# tablas: id, cnt, acc ---------------------------------------------------------------------------------------
# beneficiarios de transacciones aprobadas
id_cnt01 <- unique(spi1_vld_1[bnf_id!="0000000000" & sta_end_cod==1,.(bnf_id,bnf_nm)]) # filtra validadas

#id_cnt01 <- unique(spi1_vld_1[!str_detect(bnf_nm,"@") & sta_end_cod==1,.(bnf_id,bnf_nm)]) # filtra emails
setnames(id_cnt01,c("id_cod","cnt_nm"))
id_cnt02 <- id_vld_pcs(id_cnt01) # valida identificaciones




# id
id01 <- copy(id_cnt02)
id01[,":="(id_cod_tmp=ifelse(id_tp==3 & as.numeric(str_sub(id_cod,3,3)) %in% c(0:6,9),str_sub(id_cod,end=10),id_cod))] # extrae 10 digitos del ruc
id01c <- unique(id01[as.numeric(str_sub(id_cod_tmp,3,3))<7,.(id_cod=id_cod_tmp)])[,":="(cnt_uid=1:.N)] # cédulas únicas
id01r <- id01[id_tp==3,-2] # todos los rucs
id02_1 <- melt(merge(id01r,id01c[,.(id_cod,cnt_uid)],by.x="id_cod_tmp",by.y="id_cod",all=TRUE),id.vars=c("cnt_uid"),measure.vars=c("id_cod","id_cod_tmp"))[order(cnt_uid)]
id02_2 <- unique(id02_1[!is.na(cnt_uid) & !is.na(value),.(cnt_uid,id_cod=value)])
id02_3 <- unique(id02_1[is.na(cnt_uid) & !is.na(value) & variable=="id_cod",.(cnt_uid=max(id02_2[,cnt_uid])+1:.N,id_cod=value)])
id02 <- rbindlist(list(id02_2,id02_3))
id02[,":="(id_cod_prm=1:.N),cnt_uid]
id02[,":="(id_tp=ifelse(str_length(id_cod)>10,3L,1L))] # cédula=1, ruc=3 (catálogo)

# cnt
cnt01 <- id02[,.(cnt_uid,id_cod)][id_cnt02,on=.(id_cod)]
cnt01[,":="(cnt_nm_lng=str_length(cnt_nm))]
# toma únicos en base a mayor longitud y luego por primera posición
cnt02 <- unique(cnt01[cnt01[,.(cnt_nm_lng=max(cnt_nm_lng)),cnt_uid],on=.(cnt_uid,cnt_nm_lng)],by="cnt_uid")
cnt02[,c("id_cod","id_tp","cnt_nm_lng"):=NULL]
# acc
spi1_vld_1_id <- id02[,.(id_cod,cnt_uid)][spi1_vld_1,on=.(id_cod==bnf_id)]
acc01 <- unique(spi1_vld_1_id[!is.na(cnt_uid) & sta_end_cod==1,.(cnt_uid,bnf_acc,bnf_acc_tp)])[order(cnt_uid)]
setnames(acc01,c("cnt_uid","acc_cod","acc_tp"))
acc01_01 <- unique(acc01[,.(acc_cod,acc_tp)])
acc01_01[,":="(acc_uid=1:.N)]
acc02 <- acc01[acc01_01,on=.(acc_cod,acc_tp)]


# OK ... VALIDAR LOS cnt_uid nulos

id_nm03 <- id_nm02[id_nm01,on=.(id_cod)][is.na(cnt_uid)] # identificaciones erroneas

no_cnt_id <- unique(spi1_vld_1_id[is.na(cnt_uid) & sta_end_cod==1,.(id_cod,bnf_nm)])

id_cnt02[id_cod=="0107081957"]



#fwrite(no_cnt_id,file="no_cnt_id.txt",sep="\t")

setcolorder(id_nm03,c("id_tp","id_cod","cnt_uid","id_cod_prm","log_prc_uid"))
setcolorder(id_nm03,c("cnt_nm","log_prc_uid"))


# GUARDA DATOS EN DB
dbWriteTable(cnx_pg01,'id',id02,append=TRUE,row.names=FALSE)
dbWriteTable(cnx_pg01,'cnt',cnt02,append=TRUE,row.names=FALSE)
dbWriteTable(cnx_pg01,'acc',acc02,append=TRUE,row.names=FALSE)






# HOMOLOGA add_info
spi2d[,":="(add_info=ifelse(str_count(add_info,"-")>1,str_replace(add_info,"-"," "),add_info))]

spi2d[,c("add_info01","add_info02"):=tstrsplit(add_info,"-",fixed=TRUE)]
spi2d_add_info02 <- spi2d[,.N,add_info02][!is.na(add_info02),add_info02]

spi2d_add_info02_hom <- hom_cv(spi2d_add_info02)

str(spi2d_add_info02)

fwrite(data.table(spi2d_add_info02),file="spi2d_add_info02.txt")






spi_2d3d[,":="(sta_end_cod=as.integer(sta_end_cod))]

spi_2d3d[,.N,sta_end_cod]

spi_2d3d[bnf_id == "0602932402"]


spi_2d3d_err_id <- spi_2d3d[sta_end_cod=="16"]

id_nm_err01 <- unique(spi_2d3d_err_id[!str_detect(bnf_nm,"@"),.(bnf_id,bnf_nm)]) # filtra emails
setnames(id_nm_err01,c("id_cod","cnt_nm"))
id_nm_err02 <- id_vld_pcs(id_nm_err01) # valida identificaciones
id_nm_err03 <- id_nm_err02[id_nm_err01,on=.(id_cod)][is.na(cnt_uid)] # identificaciones erroneas



spi_2d3d[is.na(itt_ptt_reg_dttm),.N]






#spi2 V17,V18: 21822
spi2d_vld <- unique(spi2d[rpt_id==1,.(V17,V18)])
setnames(spi2d_vld,c("bce_ref_cod","bce_seq_cod"))

#spi3 V1,V2
spi3d_vld <- unique(spi3d[rpt_id==1,.(V1,V2)])
setnames(spi3d_vld,c("bce_ref_cod","bce_seq_cod"))

# vld files
fsetequal(spi2d_vld,spi3d_vld)

############################
#spi1 V2
spi1d_vld <- unique(spi1d[rpt_id==1,.(V2)])
setnames(spi1d_vld,c("itt_ptt_ref_cod"))

#spi3 V2
spi4d_vld <- unique(spi4d[rpt_id==1,.(V2)])
setnames(spi4d_vld,c("itt_ptt_ref_cod"))

# vld files
fsetequal(spi1d_vld,spi4d_vld)

spi1_spi4 <- spi1d[rpt_id==1][spi4d[rpt_id==1,.(V2,V6)],on=.(V2)]

spi1_spi4[,sum(as.numeric(V5)),i.V6]


spi1d[,.N,id_rpt]
spi4d[,.N,id_rpt]


rpt_nm <- "spi2h"







##############################################################################################################

id_nm01[,c("T16","T14"):=lapply(.SD,str_length),.SDcols=1:2]


rpt01[,1:ncol(rpt01):=lapply(.SD,hom_txt,na=TRUE),.SDcols=1:ncol(rpt01)


id_nm01[,":="(id_tp=ifelse(str_length(V16)>10,"R","C"))]
#CONSULTAR EL ULTIMO ID_CNT DE BDD
max_idcnt <- 0
id_nm01[,":="(id_cnt=max_idcnt+1:.N)]

#DATOS PARA ACCOUNTING BENEFICIARIO
#LARGO DEL NUMERO DE CUENTA
id_acc01 <- unique(spi01d[,.(V12,V13)],by="V12")
#CONSULTAR EL ULTIMO ID_CNT DE BDD
max_idacc <- 0
id_acc01[,":="(id_acc=max_idcnt+1:.N)]

#UPDATE INFORMACION CON NUEVOS CODIGOS id_cnt Y id_acc
spi01d_1 <- id_nm01[,.(id_cnt,V16)][spi01d,on=.(V16)]
spi01d_1 <- id_acc01[,.(id_acc,V12)][spi01d_1,on=.(V12)]


# solo toma el primer valor que coincide con el max
dt[dt[,.I[which.max(v2)],by=v3][,V1]]


# ANALISIS BANCO DEL AUSTRO ----------------------------------------------------------------------------------
dat01 <- spi_2d3d[itt_ptt_acc_bce==3600186 & sta_end_cod=="01",.(amn=sum(amn),n=.N),.(sta_end_cod,itt_ptt_reg_dt,rpt_id)]


require(ggplot2)

scl_dig <- function(x) sprintf("%.0f",x)
scl_comm <- function(x) format(x, big.mark=",", scientific=FALSE)

require(RColorBrewer)
display.brewer.all("Blues")
display.brewer.pal(8,"Blues")
brewer.pal(8,"Blues")[6:8]

# geom_bar is designed to make it easy to create bar charts that show
# counts (or sums of weights)
g <- ggplot(dat01, aes(as.factor(itt_ptt_reg_dt),amn))
g <- g + geom_bar(stat = "identity",aes(fill = rpt_id))
g <- g + theme(axis.text.x=element_text(angle=90,vjust=0.5,hjust=1,size=7))
g <- g + labs(fill="corte",x="fecha",y="monto",title="Banco del Austro (SPI2)")
g <- g + scale_y_continuous(labels=scl_comm)
g <- g + scale_fill_continuous(guide = FALSE)
g

g <- ggplot(dat01, aes(as.factor(itt_ptt_reg_dt),n))
g <- g + geom_bar(stat = "identity",aes(fill = rpt_id))
g <- g + theme(axis.text.x=element_text(angle=90,vjust=0.5,hjust=1,size=7))
g <- g + labs(fill="corte",x="fecha",y="n",title="Banco del Austro (SPI2)")
#g <- g + scale_colour_gradientn(colours=brewer.pal(8,"Blues"))
g <- g + scale_y_continuous(labels=scl_comm)
g <- g + scale_fill_continuous(guide = FALSE)
g



scale_colour_gradientn(colours=brewer.pal(8,"Blues")


scl_comm(dat01s[month(itt_ptt_reg_dt)==9,.(mean=mean(V1),median=median(V1),min=min(V1),max=max(V1))])


<- g + scale_fill_brewer(palette="Dark2",direction=1)

g <- g + scale_fill_manual("cortes",values=brewer.pal(8,"Blues")[c(4,6,8)])
g

g <- g + scale_fill_manual("cortes",values=c("1"="#C6DBEF","2"="#4292C6","3"="#084594"))

values = c("A" = "black", "B" = "orange", "C" = "blue")

g <- ggplot(dat02, aes(as.factor(itt_ptt_reg_dt),V1)) + geom_bar(stat = "identity",aes(fill = rpt_id))
g <- g + theme(axis.text.x=element_text(angle=90,vjust=0.5,hjust=1))
g <- g + labs(fill="corte",x="fecha",y="monto",title="Banco del Austro (SPI2)")
g <- g + scale_y_continuous(labels=scl_comm)
g

g + theme(axis.text.x = element_text(angle = 90, hjust = 1))

p <-ggplot(mydata, aes(months, values))
p +geom_bar(stat = "identity")

# DESCRIPCION DE DATOS ---------------------------------------------------------------------------------------
# genera descripción de archivos
to_file(description(spi1[[2]]),"spi1d_des.txt")
to_file(description(spi2[[2]]),"spi2d_des.txt")
to_file(description(spi3[[2]]),"spi3d_des.txt")

# TOP 100 CLIENTES SPI1---------------------------------------------------------------------------------------
spi1_1_mnt <- spi1_1[,.(amn_mnt=sum(amn)),ptt_acc][order(-amn_mnt)][1:100]
#spi1_1_mnt[,":="(amn_cut=ifelse(amn_mnt>=1000000,">=1000000","<1000000"))]
cnt01 <- spi1_vld_1[ptt_acc %in% spi1_1_mnt[,ptt_acc]]
to_excel(cnt01,"d:/","spi1_cnt_","dat")


# TOP 100 CLIENTES SCI---------------------------------------------------------------------------------------

sci2_1_mnt <- sci2_1[,.(amn_mnt=sum(amn)),pay_acc][order(-amn_mnt)][1:100]
#spi1_1_mnt[,":="(amn_cut=ifelse(amn_mnt>=1000000,">=1000000","<1000000"))]
cnt01 <- spi1_vld_1[ptt_acc %in% spi1_1_mnt[,ptt_acc]]
to_excel(cnt01,"d:/","spi1_cnt_","dat")
