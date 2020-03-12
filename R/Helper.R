# restricts to pop and saves/creates mapping
MapCovariates <- function(plpData,
                          population){
    covariates = plpData$covariates
    covariateRef = plpData$covariateRef

    # restrict to population for speed
    ParallelLogger::logTrace('restricting to population for speed...')
    idx <- ffbase::ffmatch(x = covariates$rowId, table = ff::as.ff(population$rowId))
    idx <- ffbase::ffwhich(idx, !is.na(idx))
    covariates <- covariates[idx, ]

    covariateData = list(covariates = covariates,
                         covariateRef = covariateRef,
                         timeRef = plpData$timeRef,
                         analysisRef = plpData$analysisRef,
                         metaData = plpData$metaData)
    class (covariateData) = "covariateData"
    return(covariateData)
}
