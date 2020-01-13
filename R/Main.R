# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of IntraVitrealCx
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Execute the Study
#'
#' @details
#' This function executes the IntraVitrealCx Study.
#'
#' The \code{createCohorts}, \code{synthesizePositiveControls}, \code{runAnalyses}, and \code{runDiagnostics} arguments
#' are intended to be used to run parts of the full study at a time, but none of the parts are considerd to be optional.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the work database schema.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @param databaseId           A short string for identifying the database (e.g.
#'                             'Synpuf').
#' @param databaseName         The full name of the database (e.g. 'Medicare Claims
#'                             Synthetic Public Use Files (SynPUFs)').
#' @param databaseDescription  A short description (several sentences) of the database.
#' @param onTreatmentWithBlankingPeriod logical value (TRUE or FALSE). If the analysis number 13(on-treatment with blanking period analysis) doesn't work, then please set this value FALSE
#' @param createCohorts        Create the cohortTable table with the exposure and outcome cohorts?
#' @param createPlpData
#' @param packageResults       Should results be packaged for later sharing?
#' @param maxCores             How many parallel cores should be used? If more cores are made available
#'                             this can speed up the analyses.
#' @param minCellCount         The minimum number of subjects contributing to a count before it can be included
#'                             in packaged results.
#' @param sampleSize
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              user = "joe",
#'                                              password = "secret",
#'                                              server = "myserver")
#'
#' execute(connectionDetails,
#'         cdmDatabaseSchema = "cdm_data",
#'         cohortDatabaseSchema = "study_results",
#'         cohortTable = "cohort",
#'         oracleTempSchema = NULL,
#'         outputFolder = "c:/temp/study_results",
#'         maxCores = 4)
#' }
#'
#' @export
execute <- function(connectionDetails,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema = cdmDatabaseSchema,
                    cohortTable = "cohort",
                    oracleTempSchema = cohortDatabaseSchema,
                    outputFolder,
                    databaseId = "Unknown",
                    databaseName = "Unknown",
                    databaseDescription = "Unknown",
                    createCohorts = TRUE,
                    createPlpData = TRUE,
                    packageResults = TRUE,
                    maxCores = 4,
                    minCellCount= 5,
                    sampleSize = NULL) {
    if (!file.exists(outputFolder))
        dir.create(outputFolder, recursive = TRUE)
    if (!is.null(getOption("fftempdir")) && !file.exists(getOption("fftempdir"))) {
        warning("fftempdir '", getOption("fftempdir"), "' not found. Attempting to create folder")
        dir.create(getOption("fftempdir"), recursive = TRUE)
    }

    ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))

    if (createCohorts) {
        ParallelLogger::logInfo("Creating exposure and outcome cohorts")
        createCohorts(connectionDetails = connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortDatabaseSchema = cohortDatabaseSchema,
                      cohortTable = cohortTable,
                      oracleTempSchema = oracleTempSchema,
                      outputFolder = outputFolder)
    }
    if (createPlpData){
        targetCohortId = 919
        outcomeCohortId = 950

        ##Create Covariate Table
        covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE,
                                                                        useDemographicsAge = TRUE)

        ##Incidence
        plpData <- PatientLevelPrediction::getPlpData(connectionDetails = connectionDetails,
                                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                                      cohortTable = cohortTable,
                                                      cohortId = targetCohortId,
                                                      covariateSettings = covariateSettings,
                                                      outcomeDatabaseSchema = cohortDatabaseSchema,
                                                      outcomeTable = cohortTable,
                                                      outcomeIds = outcomeCohortId,
                                                      sampleSize = sampleSize)

        population <- PatientLevelPrediction::createStudyPopulation(plpData = plpData,
                                                                    outcomeId = outcomeCohortId,
                                                                    washoutPeriod = 0,
                                                                    firstExposureOnly = FALSE,
                                                                    removeSubjectsWithPriorOutcome =F,
                                                                    priorOutcomeLookback = 0,
                                                                    riskWindowStart = 1,
                                                                    riskWindowEnd = 42,
                                                                    addExposureDaysToStart = FALSE,
                                                                    addExposureDaysToEnd = FALSE,
                                                                    minTimeAtRisk = 1,
                                                                    requireTimeAtRisk = FALSE,
                                                                    includeAllOutcomes = TRUE,
                                                                    verbosity = "DEBUG")
        exportFolder <- file.path(outputFolder, "export")
        if (!file.exists(exportFolder)) {
            dir.create(exportFolder, recursive = TRUE)
        }

        write.csv(table(population$outcomeCount),file.path(exportFolder, "outcomeCount.csv"))
    }

    if (packageResults) {

        exportFolder <- file.path(outputFolder, "export")
        if (!file.exists(exportFolder)) {
            dir.create(exportFolder, recursive = TRUE)
        }

        ParallelLogger::logInfo("Packaging results")
        # exportResults(outputFolder = outputFolder,
        #               databaseId = databaseId,
        #               databaseName = databaseName,
        #               databaseDescription = databaseDescription,
        #               minCellCount = minCellCount,
        #               maxCores = maxCores)

        # Add all to zip file -------------------------------------------------------------------------------
        ParallelLogger::logInfo("Adding results to zip file")
        zipName <- file.path(exportFolder, paste0("Results", databaseId, ".zip"))
        #files <- list.files(exportFolder, pattern = ".*\\.csv$")
        files <- list.files(exportFolder)
        oldWd <- setwd(exportFolder)
        on.exit(setwd(oldWd))
        DatabaseConnector::createZipFile(zipFile = zipName, files = files)
        ParallelLogger::logInfo("Results are ready for sharing at:", zipName)

    }

    invisible(NULL)
}
