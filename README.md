# GEOGloWS ECMWF Streamflow Model Data Service

This is source code for the REST data service for the GEOGloWS ECMWF Streamflow Project. This repository contains code 
for a docker container of a flask app. The data service is deployed at https://geoglows.ecmwf.int. Documentation and 
tutorials can be found at https://geoglows.ecmwf.int/training.

Required Environment Variables
- GOOGLE_ANALYTICS_ID: The ID of a Google Analytics 4 (GA4) property being used to track events (usage of the data service)
- GOOGLE_ANALYTICS_TOKEN: An auth token used for that
