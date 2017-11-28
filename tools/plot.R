#!/usr/bin/env Rscript
# clean-up session parameters
rm(list=ls())

# load data
my.path <- paste0(getwd(), "/")
# example
# load.data <- read.csv(paste0(my.path, file.name), header=TRUE)

# read and parse input args
cmd_args <- commandArgs()
csv <- ""
out <- ""
for (arg in cmd_args) {
    arg <- as.character(arg)
    match <- grep("^--csv", arg)
    if (length(match) == 1) csv <- gsub("--csv=", "", arg)
    match <- grep("^--out", arg)
    if (length(match) == 1) out <- gsub("--out=", "", arg)
}
if (nchar(csv)==0 || nchar(out)<2) {
   stop("Usage: plot.R --csv=<csv file> --out=<out pdf file>")
}

# set seed
set.seed(12345)

# load libraries
libs <- c("ggplot2", "data.table", "plyr", "dplyr")
for(i in 1:length(libs)) {
    pkg <- sprintf("%s", libs[i])
    print(sprintf("load %s", pkg))
    suppressMessages(library(pkg, character.only = TRUE))
}

df=read.csv(csv, header=F)
names(df)=c("tier","old","new","b1","b2","b3","b4","b5","b6","b7","b8","b9","b10","b11","b12","b13","b14","b15")

totals=c(sum(df$old), sum(df$new), sum(df$b1), sum(df$b2), sum(df$b3), sum(df$b4), sum(df$b5), sum(df$b6), sum(df$b7), sum(df$b8), sum(df$b9), sum(df$b10), sum(df$b11), sum(df$b12), sum(df$b13), sum(df$b14), sum(df$b15))
x=c(-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
tdf=data.frame(x, totals)

#tbytes=1024*1024*1024*1024
#tdfnorm=data.frame(bins=x, size=tdf$totals/tbytes)
pbytes=1024*1024*1024*1024*1024
tdfnorm=data.frame(bins=x, size=tdf$totals/pbytes)
print(tdfnorm)

pdf(out)
#ggplot(tdfnorm, aes(x=bins, y=size)) + geom_bar(position="dodge", stat="identity") + ggtitle("Total size at T1") + xlab("Number of accesses") + ylab("Total size in TB")
ggplot(tdfnorm, aes(x=bins, y=size)) + geom_bar(position="dodge", stat="identity") + ggtitle("Total size at T1+T2") + xlab("Number of accesses") + ylab("Total size in PB")
dev.off()
