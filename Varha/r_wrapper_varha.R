
# Set the working directory to the path where the script files are located:
setwd("/home/tommi/gitit/OMOP-Oncology-readiness/Varha/")

# Choose the sql input file. There are three input files: general.sql, genomic.sql and episode.sql. Only run episode.sql if you have Episodes in your data. 
sqlFile = "general.sql"
#sqlFile = "episodes.sql"
#sqlFile = "genomic.sql"



# Set your connection string

ini_kohde <- "omop"

konffi = ConfigParser::ConfigParser$new()$read("~/poiminta.ini")

yhteys_omop = dbConnect(
  drv = RPostgres::Postgres(),
  dbname = konffi$get("kanta", section = ini_kohde),
  host = konffi$get("palvelin", section = ini_kohde),
  user = konffi$get("kayttaja", section = ini_kohde),
  password = konffi$get("salasana", section = ini_kohde),
  port = 5432
)  

# tarkista että ollaan oikeassa kannassa
yhteys_omop



# Configuration end
# ----------------------------------------------------------------
# Configure your schema name, replace cdm with your current schema.
# If you don't need a schema, enter an empty string ""
schema <- "cdm"

#s <- readr::read_file(paste0("../OHDSI/", sqlFile))
s <- readr::read_file(sqlFile)

# Turn newlines into space because DBI requires one line at a time
s <- gsub("\r|\n", " ", s)

# Add the schema name to the script
if (schema != "") {
  if (endsWith(schema, ".") == F) schema = paste(schema, ".", sep="")
  
  s <- gsub("\\bdrug_exposure\\b", paste(schema, "drug_exposure", sep=""), s)
  s <- gsub("\\bdevice_exposure\\b", paste(schema, "device_exposure", sep=""), s)
  s <- gsub("\\bprocedure_occurrence\\b", paste(schema, "procedure_occurrence", sep=""), s)
  s <- gsub("\\bcondition_occurrence\\b", paste(schema, "condition_occurrence", sep=""), s)
  s <- gsub("\\bobservation\\b", paste(schema, "observation", sep=""), s)
  s <- gsub("\\bmeasurement\\b", paste(schema, "measurement", sep=""), s)
  s <- gsub("\\bepisode\\b", paste(schema, "episode", sep=""), s)
}

# Split the SQL query into individual coomands 
a <- strsplit(s, ";")

# Submit each command one at the time 
for (sql in a[[1]]) {
  sql = paste(trimws(sql, "l"), ";")
  #print(sql)
  if (startsWith(sql, "select")) {
    res = DBI::dbGetQuery(yhteys_omop, sql)
    f = paste(substring(sqlFile, 1, nchar(sqlFile)-4), ".csv", sep = "")
    write.csv(res, f, row.names = F)
    message(paste(f, "has been written."))
  } else {
    res = DBI::dbSendStatement(yhteys_omop, sql)
    DBI::dbClearResult(res)
  }
}

