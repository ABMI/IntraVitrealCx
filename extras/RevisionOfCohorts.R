
fileList <- list.files(pattern = "^Ranibizumab", all.files = T, recursive = T, full.names = T)
for(txFile in fileList){
    tx  <- readLines(txFile)
    if(sum(grepl(pattern = "1397141", x=tx))){
        print(sprintf("Bevacizumab in %s is replaced", txFile))
    }

    tx2  <- gsub(pattern = "1397141", replace = "19080982", x = tx)
    tx2  <- gsub(pattern = "[Bb]evacizumab", replace = "ranibizumab", x = tx2)

    writeLines(tx2, con = txFile)
}


fileList <- list.files(pattern = "^Aflibercept", all.files = T, recursive = T, full.names = T)
for(txFile in fileList){
    tx  <- readLines(txFile)

    if(sum(grepl(pattern = "1397141", x=tx))){
        print(sprintf("Bevacizumab in %s is replaced", txFile))
    }

    tx2  <- gsub(pattern = "1397141", replace = "40244266", x = tx)
    tx2  <- gsub(pattern = "[Bb]evacizumab", replace = "aflibercept", x = tx2)

    writeLines(tx2, con = txFile)
}
