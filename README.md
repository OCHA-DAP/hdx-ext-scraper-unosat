# Upload datasets to HDX from UNOSAT database

Takes command line arguments:

    -hk or --hdx_key: HDX api key
    -hs or --hdx_site: HDX site to use. Use prod for production. Defaults to feature.
    -dp or --db_params: Database connection parameters
    -sd or --start_date: Add any datasets created or updated after this date. 
                         Defaults to one week prior to current date.
 
 eg.
  
    python run.py -sd 2018-01-01 -hs feature -dp db=unosat_db,host=localhost,user=unosat_user,password=unosat_password,port=3306 -hk xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx