library("DatabaseConnector")

# ----------------------------------------------------------------
# Configuration start
# This is your current working directory:
getwd()

# Set the working directory to the path where the script files are located:
#setwd("")

# Choose the sql input file. There are three input files: general.sql, genomic.sql and episode.sql. Only run episode.sql if you have Episodes in your data. 
sqlFile = "input.sql"

# Set your connection string

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "yourDBMS",
                                                                server="yourServer",
                                                                user="yourUsername",
                                                                port = "yourPort",
                                                                password="yourPassword",
                                                                pathToDriver = "JDBC/")


# Configuration end
# ----------------------------------------------------------------
con <- connect(connectionDetails)
# Configure your schema name, replace cdm with your current schema.
# If you don't need a schema, enter an empty string ""
schema <- "omop_cdm_53_pmtx_202203"

if (endsWith(sqlFile, ".sql") == FALSE)
  stop("Sql file must end with .sql")

s <- readr::read_file(sqlFile)

# Turn newlines into space because DBI requires one line at a time
s <- gsub("\r\n", " ", s)

# Add the schema name to the script
if (schema != "")
{
  if (endsWith(schema, ".") == FALSE)
    schema = paste(schema, ".", sep="")
  
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
for (sql in a[[1]])
{
  sql = paste(trimws(sql, "l"), ";")
  if (startsWith(sql, "select"))
  {
    res = DBI::dbGetQuery(con, sql)
    f = paste(substring(sqlFile, 1, nchar(sqlFile)-4), ".csv", sep = "")
    write.csv(res, f, row.names = FALSE)
    message(paste(f, "has been written."))
  }
  else
  {
    res = DBI::dbSendStatement(con, sql)
    DBI::dbClearResult(res)
  }
}

DBI::dbDisconnect(con)
