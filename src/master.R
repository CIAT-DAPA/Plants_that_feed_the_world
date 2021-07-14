##############################################
# 01 - Loading libraries and packages
##############################################

library(plyr)
library(dplyr)
library(ggplot2)

source("tools/download.R")
source("raw_data/fao.R")

##############################################
# 02 - Setting parameters
##############################################

# Setting global parameters
root = "/workdir/"
conf_folder = paste0(root, "conf")
inputs_folder = paste0(root, "inputs")

# Creating folders
ifelse(!dir.exists(file.path(inputs_folder)), dir.create(file.path(inputs_folder)), FALSE)

##############################################
# 03 - Getting data from sources
##############################################
