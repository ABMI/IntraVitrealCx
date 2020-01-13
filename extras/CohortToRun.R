# add the cdm database schema with the data
cdmDatabaseSchema <- ''
# add the work database schema this requires read/write privileges
cohortDatabaseSchema <- ''

# the name of the table that will be created in cohortDatabaseSchema to hold the cohorts
cohortTable <- 'beva_opthalmitis'

# the location to save the prediction models results to:
outputFolder <- ''

databaseId <- ''

# add connection details:
options(fftempdir = '/temp')
targetCohortId = 919
outcomeCohortId = 950


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = Sys.getenv("evidnet_server"),
                                                                user = Sys.getenv("evidnet_id"),
                                                                password = Sys.getenv("evidnet_pw"),
                                                                port = Sys.getenv("evidnet_port"))


execute(connectionDetails,
        cdmDatabaseSchema,
        cohortDatabaseSchema = cdmDatabaseSchema,
        cohortTable = cohortTable,
        oracleTempSchema = cohortDatabaseSchema,
        outputFolder,
        databaseId = databaseId,
        databaseName = databaseId,
        databaseDescription = databaseId,
        createCohorts = F,
        createPlpData = TRUE,
        packageResults = TRUE,
        maxCores = 4,
        minCellCount= 5,
        sampleSize = NULL)

