# Get all necessary packages across data prep and analysis scripts 
# add any packages your scripts require here. Keep in alphabetical order.

loadpacks <- c(
  'ggplot2',
  'tidyr',
  'dplyr',
  'DT',
  'egg',
  'plotly',
  'knitr',
  'rgdal',
  'rmarkdown',
  'RSQLite'
  )

completed_installs = 0

for(i in loadpacks){
  if(length(grep(i, (.packages(all.available=T))))==0) {
    
    install.packages(i, dependencies = TRUE, repos = "http://cran.us.r-project.org")
    
    completed_installs = completed_installs + 1
    
    }
}

if(completed_installs > 0) {
  cat('Successfully installed', completed_installs, 'dependencies for R installed at', Sys.getenv('R_HOME'))
}

rm(i, loadpacks)