import landing.export_to_formatted
import formatted.profiling_formatted
import trusted.profiling_and_export_to_trusted
import exploitation.integration_and_export_to_exploitation
import feature_generation.modelling

# execute file to export from landing to formatted DB
execfile('export_to_formatted.py')

# execute file to profile formatted DB
execfile('profiling_formatted.py')

# execute file to import from formatted DB, process, and export to trusted DB
execfile('profiling_and_export_to_trusted.py')

# execute file to import from trusted DB, process, and export to exploitation DB
execfile('integration_and_export_to_exploitation.py')

# execute file to import from exploitation, run modelling, and export to feature_extraction DB
execfile('modelling.py')


