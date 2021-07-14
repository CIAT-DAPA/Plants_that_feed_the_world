
##############################################
####  01-DOWNLOAD DATA AND SAVE FILES



# This function save the countries names in a files from faostat file
# (string) f: File path
process.load.countries = function(f){
  tmp.source.group = gsub(".csv","",unlist(strsplit(f, "-")))
  #tmp.group = inputs.group[inputs.group$name == as.character(tmp.source.group[2]),]
  tmp.measure = read.csv(paste0(inputs.folder,"/",f ), header = T)
  print(paste0("........Measures were loaded"))
  
  tmp.countries = tmp.measure[,c("Area.Code","Area")]
  tmp.countries = unique(tmp.countries)
  write.csv(tmp.countries, paste0(inputs.folder,"/",tmp.source.group[1],"-",tmp.source.group[2],"-countries.csv"), row.names = F )
  print(paste0("........Countries saved"))
}
##############################################

##############################################
####  02-SET UP THE MAIN ENTITIES

# This function saves the new records of metrics from file
# (string) f: File name
process.load.metrics = function(f){
  tmp.source.group = gsub(".csv","",unlist(strsplit(f, "-")))
  tmp.group = inputs.group[inputs.group$name == as.character(tmp.source.group[2]),]
  tmp.measure = read.csv(paste0(inputs.folder,"/",f ), header = T)
  print(paste0("........Parameters were loaded"))
  
  # Get list of groups metrics by group
  process.metrics.query = dbSendQuery(db_cnn,paste0("select id,id_group,name,units from metrics where id_group = ",as.character(tmp.group$id[1])))
  process.metrics = fetch(process.metrics.query, n=-1)
  print(paste0("........Metrics were loaded"))
  
  # Compare data from database with the new values
  tmp.values = unique(tmp.measure[,c("Element","Unit")])
  tmp.new.values = subset(tmp.values,!(Element %in% process.metrics$name))
  
  if(dim(tmp.new.values)[1] > 0){
    tmp.new.values$id_group = tmp.group$id
    names(tmp.new.values)=c("name","units","id_group")
    print(paste0("........The data was cleaned"))
    
    dbWriteTable(db_cnn, value = tmp.new.values, name = "metrics", append = TRUE, row.names=F)
    print(paste0("........Records were saved ", dim(tmp.new.values)[1]))  
  }
  
  
}

##############################################

##############################################
####  03-IMPORT MEASURES


# This function saves the records of measure from file
# (string) f: File name
process.load.measure = function(f){
  db_cnn = connect_db("fao")
  tmp.source.group = gsub(".csv","",unlist(strsplit(f, "-")))
  tmp.group = inputs.group[inputs.group$name == as.character(tmp.source.group[2]),]
  tmp.measure = read.csv(paste0(inputs.folder,"/",f ), header = T)
  # Fixing some fields
  tmp.measure$Area = as.character(tmp.measure$Area)
  tmp.measure$Item = as.character(tmp.measure$Item)
  print(paste0("........Measures were loaded ",dim(tmp.measure)[1]))
  
  # Getting the dictionary of countries
  tmp.countries = read.csv(paste0(conf.folder,"/",tmp.source.group[1],"/countries.csv" ), header = T)
  tmp.countries$name = as.character(tmp.countries$name)
  print(paste0("........Countries were loaded ",dim(tmp.countries)[1]))
  # Getting the records which don't match with countries
  tmp.measure.fail = tmp.measure[!(tmp.measure$Area %in% tmp.countries$name),]
  write.csv(tmp.measure.fail, paste0(process.folder, "/",gsub(".csv","",f),"-countries-fail.csv"), row.names = F)
  print(paste0("........Countries won't be merged ",dim(tmp.measure.fail)[1]))
  # Merging with countries
  tmp.measure = merge(x=tmp.measure, y=tmp.countries, by.x="Area", by.y="name", all.x = F, all.y = F)
  write.csv(tmp.measure, paste0(process.folder, "/",gsub(".csv","",f),"-countries-good.csv"), row.names = F)
  print(paste0("........Countries were merged ",dim(tmp.measure)[1]))
  
  
  # Get the dictionary of crops
  tmp.crops.groups = read.csv(paste0(conf.folder,"/",tmp.source.group[1],"/crops_groups.csv" ), header = T)
  tmp.crops.groups = tmp.crops.groups[which(tmp.crops.groups$Metric == paste0(tmp.source.group[1],"-",tmp.source.group[2])),]
  tmp.measure = merge(x=tmp.measure, y=tmp.crops.groups, by.x="Item", by.y="Item", all.x = F, all.y = F)
  write.csv(tmp.measure, paste0(process.folder, "/",gsub(".csv","",f),"-crops-groups-fixed.csv"), row.names = F)
  print(paste0("........Crops were merged with crops group ", nrow(tmp.measure)))
  
  # Get the species list
  tmp.species = read.csv(paste0(conf.folder,"/",tmp.source.group[1],"/species_list.csv" ), header = T)
  tmp.column = paste0(tmp.source.group[1],".",tmp.source.group[2])
  tmp.species = tmp.species[,c("id_crop","crop",tmp.column)]
  tmp.measure = merge(x=tmp.measure, y=tmp.species, by.x="Item_cleaned", by.y=tmp.column, all.x = F, all.y = F)
  # Getting the records which don't match with crops
  tmp.measure.fail = tmp.measure[!(tmp.measure$id_crop %in% tmp.species$id_crop),]
  write.csv(tmp.measure.fail, paste0(process.folder, "/",gsub(".csv","",f),"-crops-fail.csv"), row.names = F)
  print(paste0("........Crops won't be merged ",dim(tmp.measure.fail)[1]))
  # Merging with crops
  write.csv(tmp.measure, paste0(process.folder, "/",gsub(".csv","",f),"-crops-good.csv"), row.names = F)
  print(paste0("........Crops were merged ",dim(tmp.measure)[1]))
  
  # Weight Segregation
  tmp.segregation.file = paste0(conf.folder,"/",tmp.source.group[1],"/segregation-",tmp.source.group[1],"-",tmp.source.group[2],".csv")
  if(file.exists(tmp.segregation.file)){
    print(paste0("............Segregation"))
    tmp.segregation = read.csv(tmp.segregation.file, header = T)
    tmp.measure = merge(x=tmp.measure, y=tmp.segregation, by.x=c("Item_cleaned","crop"), by.y=c("group","crop"), all.x = T, all.y = F)
    tmp.measure[["percentage"]][is.na(tmp.measure[["percentage"]])] = 1
  }
  
  # Get list of groups metrics by group
  tmp.metrics.query = dbSendQuery(db_cnn,paste0("select id as id_metric,name from metrics where id_group = ",as.character(tmp.group$id[1])))
  tmp.metrics = fetch(tmp.metrics.query, n=-1)
  print(paste0("........Metrics were loaded "))
  # Merge with metrics
  tmp.measure = merge(x=tmp.measure, y=tmp.metrics, by.x="Element", by.y="name", all.x = F, all.y = F)
  print(paste0("........Metrics were merged "))
  
  # Fixing the fields
  tmp.measure = select(tmp.measure, -ends_with("F"))
  tmp.measure = select(tmp.measure, -ends_with("Code"))
  tmp.measure = select(tmp.measure, -starts_with("name"))
  names(tmp.measure) = gsub("Y","",names(tmp.measure))
  
  if("percentage" %in% names(tmp.measure)){
    tmp.years.end = dim(tmp.measure)[2]-7
    
  }else{
    tmp.years.end = dim(tmp.measure)[2]-7
  }
  tmp.years.start = tmp.years.end - 6
  dbDisconnect(db_cnn)
  # Create records by every year
  lapply(tmp.years.start:tmp.years.end,function(y){
    tmp.values = tmp.measure[,y]
    
    # Only for segregation
    if("percentage" %in% names(tmp.measure)){
      tmp.values = tmp.values * tmp.measure$percentage
    }
    
    tmp.df = data.frame(id_metric=tmp.measure$id_metric,
                           id_country=tmp.measure$id_country,
                           id_crop=tmp.measure$id_crop,
                           year=as.integer(names(tmp.measure)[y]),
                           value=tmp.values)
    # Remove NA
    tmp.df =  tmp.df[complete.cases(tmp.df), ]
    # Remove 0's
    tmp.df =  tmp.df[which(tmp.df$value > 0), ]
    # Sum values where they have the same metric, country, crop and year 
    # It is because when we transform the original crops to master crops, they could be the same
    tmp.df = ddply(tmp.df,.(id_metric,id_country,id_crop,year),summarise,value=sum(value))
    
    write.csv(tmp.df, paste0(process.folder, "/final/",gsub(".csv","",f),as.character(names(tmp.measure)[y]),".csv"), row.names = F)
    
    rows = nrow(tmp.df)
    for (i in seq(from=1,to=rows, by=10000)){
      end = i+(10000-1)
      if(end > rows){
        end = rows
      }
      records = tmp.df[i:end,]
      db_cnn1 = connect_db("fao")
      print(paste0("........COnnected"))
      dbWriteTable(db_cnn1, value = records, name = "measures", append = TRUE, row.names=F)
      dbDisconnect(db_cnn1) 
      print(paste0("........Records were saved year: ",names(tmp.measure)[y],"  count: ", dim(records)[1]))
    }
  })
  
}

##############################################


# This function saves all data into database
# (string) f: Name of file
process.load = function(f){
  print(paste0("Star ", f))
  print(paste0("....Measures ", f))
  process.load.measure(f)
}

